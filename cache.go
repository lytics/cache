package cache

import (
	"container/list"
	"hash/crc32"
	"reflect"
	"sync"
	"time"
)

// A Cache is a concurrent in-memory data structure backed by a database. It internally uses
// lock striping to try to achieve high throughput when used by many goroutines. The cache
// cannot use nil keys or nil values.
type Cache struct {
	stripes    []*stripe
	numStripes int
	// maxSize    int64
	// maxAge     int64
	loader    CacheLoader
	sizer     Sizer
	cacheLock sync.RWMutex
}

// A function that can look up a key that's not already in the cache. If this function panics
// or calls one of the cache functions, that's very bad and the result is undefined. Most
// implementations of this function will do a database lookup.
type CacheLoader func(key string) (interface{}, error)

// A function that can estimate the size of a cache entry. This is used to enforce the max
// size of the cache.
type Sizer func(interface{}) int64

// A convenient sizer function that always returns a size of 1. You can use this for
// limiting cache size by item count.
func SizerAlwaysOne(interface{}) int64 {
	return 1
}

// Make a new cache.
//  - numStripes: how many partitions/locks this cache should have. This number should be a
//                factor of 256 for even stripe distribution.
//  - loader: the caller provides this implementation to retrieve a cache entry from the
//            backing database if it's not already in the cache.
//  - sizer: the caller provides this implementation to calculate the size of a cache entry.
func NewCache(numStripes int, loader CacheLoader, sizer Sizer) *Cache {

	return &Cache{
		stripes:    newStripes(numStripes),
		numStripes: numStripes,
		loader:     loader,
		sizer:      sizer,
	}
}

// Look up the given key in the cache. If it's not in the cache, load it using the loader that
// was passed to NewCache.
func (this *Cache) GetOrLoad(key string) (interface{}, error) {
	// A Get is actually the same thing as a Combine with nothing. The combiner just returns
	// the loaded value, which will then be stored in the cache.
	combiner := func(_ string, loaded interface{}, _ interface{}) (interface{}, error) {
		return loaded, nil
	}
	return this.Combine(key, nil, combiner)
}

// Add an object to the cache manually without running the loader function. Silently
// overwrites any existing entry with the same key.
func (this *Cache) Insert(key string, val interface{}) {
	this.Combine(key, nil, func(string, interface{}, interface{}) (interface{}, error) {
		return val, nil
	})
}

// This can be used to modify an existing value held in the cache. You pass a function to be
// executed while holding locks that guarantee exclusive access to that cache entry. The
// arguments to the function are (1) the current cache value and (2) an arbitrary value to be
// combined with it that the caller passes to Combine(). A combiner may return an error, which
// will cause the Combine() function to have no effect.
// The return value of the combiner is used as the new cache value. If the combiner returns nil
// then the cache entry is deleted.
// If there is no value for the given key (the loader returned nil) then the combiner will
// get a nil as its first parameter.
type Combiner func(key string, oldI interface{}, newI interface{}) (interface{}, error)

// Modify a cached value using the given combiner function, holding locks to guarantee
// exclusive access. If no value is cached for the given key, the loader will first be invoked 
// to load a value.
// Returns the new value of the cache entry, or error if the combiner returned error.
func (this *Cache) Combine(key string, newVal interface{}, combiner Combiner) (interface{}, error) {
	this.cacheLock.RLock() // Mutual exclusion against threads computing cache expiration
	defer this.cacheLock.RUnlock()

	stripe := this.getStripe(key)

	stripe.lock.Lock() // Mutual exclusion against threads reading/writing the same stripe
	defer stripe.lock.Unlock()

	// Get the existing value from the to combine with (or nil if nonexistent)
	var existingVal interface{}
	elem, hasExisting := stripe.elemMap[key]
	if hasExisting {
		existingVal = elem.Value.(*keyValue).v
	} else {
		dbVal, err := this.loader(key)
		if err != nil {
			return nil, err
		}
		existingVal = dbVal
	}

	combinedVal, err := combiner(key, existingVal, newVal)
	if err != nil {
		return nil, err
	}

	// The combiner can return nil to cause the cache entry to be removed.
	if isNil(combinedVal) {
		if hasExisting {
			delete(stripe.elemMap, key)
			stripe.totalSize -= elem.Value.(*keyValue).size
			stripe.lst.Remove(elem)
		}
	} else {
		// The value returned from the combiner was not null.
		newSize := this.sizer(combinedVal)

		if hasExisting {
			// Update preexisting entry.
			keyValue := elem.Value.(*keyValue)

			stripe.totalSize -= keyValue.size
			stripe.totalSize += newSize

			keyValue.v = combinedVal
			keyValue.size = newSize
		} else {
			// The entry did not previously exist. Store it in the cache now.
			keyValue := newKeyValue(key, combinedVal, time.Now().UnixNano(), newSize)
			stripe.totalSize += newSize
			newElem := stripe.lst.PushBack(keyValue)
			stripe.elemMap[key] = newElem
		}
	}

	return combinedVal, nil
}

func isNil(combinedVal interface{}) bool {
	if combinedVal == nil {
		return true
	}

	rv := reflect.ValueOf(combinedVal)
	switch rv.Kind() {
	// These are the only nullable kinds. See golang's reflect/value.go
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.Interface, reflect.Slice:
		return rv.IsNil()
	}

	// The value is not a nullable type, therefore it's valid.
	return false
}

// A function that is called when a cache entry is removed from the cache. You might use this to
// flush to a database.
type EvictHandler func(key string, entry interface{}) error

// A shortcut for ExpireAndHandle(), but doesn't need an EvictHandler. Returns expired entries
// a map from cacheKey->cacheVal.
func (this *Cache) Expire(maxSize int64, maxAge time.Duration) map[string]interface{} {
	m := make(map[string]interface{})

	// This eviction hander will just add the evicted items to a map
	evictHandler := func(key string, val interface{}) error {
		m[key] = val
		return nil
	}

	if err := this.ExpireAndHandle(maxSize, maxAge, evictHandler); err != nil {
		panic(err) // there should never be an error because the EvictHandler will never have one
	}

	return m
}

// Removes all cache elements such that:
//  - the element's timestamp is older than the max age
//  - the element is one of the oldest and the cache is over its target size
// If the evict handler encounters an error, then the eviction will stop at that point, so the
// cache size constraints may not be met until an Expire call succeeds.
// The eviction handler holds a stripe lock while evicting (by design) so try to make it fast.
// TODO it would be nice to batch write during eviction
func (this *Cache) ExpireAndHandle(maxSize int64, maxAge time.Duration,
	evictHandler EvictHandler) error {

	// TODO running this function with the global lock absolutely destroys throughput. (~8x)

	// TODO can we defer taking the global lock?
	this.cacheLock.Lock()
	defer this.cacheLock.Unlock()

	// returnMap := make(map[string]interface{})
	expireNanos := time.Now().UnixNano() - int64(maxAge)

	// Now remove all elements older than their expiration time
	for _, stripe := range this.stripes {
		elem := stripe.lst.Front()
		for elem != nil {
			keyValue := elem.Value.(*keyValue)
			if keyValue.nanos >= expireNanos {
				break // no more values to expire in this stripe
			}
			elemToDelete := elem
			elem = elem.Next()
			if _, err := stripe.remove(elemToDelete, evictHandler); err != nil {
				return err
			}
		}
	}

	// Calculate the total size of all stripes
	sizeAllStripes := int64(0)
	for _, stripe := range this.stripes {
		sizeAllStripes += stripe.totalSize
	}

	// TODO can we improve performance by releasing global lock here and releasing 
	// currentSize-desiredSize worth of entries?

	// Now remove elements as necessary to enforce the size limit
	for sizeAllStripes > maxSize {

		// This could use a min-heap instead of a nested loop if it turns out to be too slow.
		minStripeIdx := -1
		oldestTimestamp := int64(-1)
		for stripeIdx, stripe := range this.stripes {

			stripeListHead := stripe.lst.Front()
			if stripeListHead == nil {
				continue
			}
			stripeOldestTs := stripeListHead.Value.(*keyValue).nanos
			if minStripeIdx == -1 || stripeOldestTs < oldestTimestamp {
				minStripeIdx = stripeIdx
				oldestTimestamp = stripeOldestTs
			}
		}

		if minStripeIdx < 0 {
			break // The cache is empty
		}

		stripeToTakeFrom := this.stripes[minStripeIdx]
		elemToRemove := stripeToTakeFrom.lst.Front()
		sizeRemoved, err := stripeToTakeFrom.remove(elemToRemove, evictHandler)
		if err != nil {
			return err
		}
		sizeAllStripes -= sizeRemoved
	}

	return nil
}

// Remove all entries from the cache, invoking the evictHandler for each one. See EvictHandler
// and Combine for instructions on how evict handlers work.
func (this *Cache) EvictAll(evictHandler EvictHandler) error {
	return this.ExpireAndHandle(-1, -1, evictHandler)
}

// Returns the sum of all the sizes of all the cache entries plus the lengths of the cache keys.
// The "size" of a cache entry is defined to be the value returned by the sizer for that entry.
// TODO should we offer a fast inconsistent approximate Size() that avoids taking the big lock?
func (this *Cache) Size() int64 {
	this.cacheLock.Lock()
	defer this.cacheLock.Unlock()

	totalSize := int64(0)
	for _, stripe := range this.stripes {
		stripe.lock.Lock()
		totalSize += stripe.totalSize
		stripe.lock.Unlock()
	}
	return totalSize
}

// Like GetOrLoad, but if there's an error, it will panic. Use this when you know that the cache 
// loader will never return an error to avoid having to write boilerplate error ignoring code.
func (this *Cache) GetOrLoadNoErr(key string) interface{} {
	val, err := this.GetOrLoad(key)
	if err != nil {
		panic(err)
	}
	return val
}

// Like Combine, but if there's an error, it will panic. Use this when you know that the cache 
// loader will never return an error to avoid having to write boilerplate error ignoring code.
func (this *Cache) CombineNoErr(key string, newVal interface{}, combiner Combiner) interface{} {
	val, err := this.Combine(key, newVal, combiner)
	if err != nil {
		panic(err)
	}
	return val
}

// This is the type of functions that may be passed to Fold(). This function is once for each 
// item stored in this lru. The value that it returns is passed as an argument to the next
// invocation of the function, and this value is usually used to gradually accumulate the
// result of the fold. For instance, a fold operation could calculate the sum of all the
// integers in an LRU by using a fold func that adds the current element to the running sum
// stored in the accumulator. The first time the function is called, the accumulator will be
// nil.
type FoldFunc func(accumulator interface{}, cacheEntry interface{}) (newAccumulator interface{})

// Apply the given fold function to each entry in the cache. See the FoldFunc docs for more info.
// Fold can be used to calculate info about all the cache entries such as the max or the sum,
// copy cache entries, and other things.
func (this *Cache) Fold(f FoldFunc) interface{} {
	this.cacheLock.Lock()
	defer this.cacheLock.Unlock()

	var accum interface{} = nil
	for _, stripe := range this.stripes {
		func() { // Wrap the loop body in a function so the deferred unlock gets executed
			stripe.lock.Lock()
			defer stripe.lock.Unlock()

			for elem := stripe.lst.Front(); elem != nil; elem = elem.Next() {
				accum = f(accum, elem.Value.(*keyValue).v)
			}
		}()
	}
	return accum
}

// The following are internal private functions.

// These are the elements that go in the FIFO expiration lists.
type keyValue struct {
	k     string      // The cache key for this value
	v     interface{} // The value stored in the cache under this key
	nanos int64       // The timestamp that this element was created
	size  int64       // The size of v, as estimated by the Sizer
}

func newKeyValue(k string, v interface{}, nanos int64, size int64) *keyValue {
	return &keyValue{
		k:     k,
		v:     v,
		nanos: nanos,
		size:  size,
	}
}

type stripe struct {
	lst       *list.List
	lock      *sync.Mutex
	totalSize int64
	elemMap   map[string]*list.Element
}

// Returns error if the eviction handler returns error. Otherwise returns the size of the
// removed entry.
func (this *stripe) remove(elem *list.Element, evictHandler EvictHandler) (int64, error) {
	kvToRemove := elem.Value.(*keyValue)

	if err := evictHandler(kvToRemove.k, kvToRemove.v); err != nil {
		return -1, err
	}

	this.lst.Remove(elem)
	this.totalSize -= kvToRemove.size
	delete(this.elemMap, kvToRemove.k)
	return kvToRemove.size, nil
}

func newStripe() *stripe {
	return &stripe{
		lst:       list.New(),
		lock:      new(sync.Mutex),
		totalSize: 0,
		elemMap:   make(map[string]*list.Element),
	}
}

func (this *Cache) getStripe(key string) *stripe {
	hash := crc32.ChecksumIEEE([]byte(key))

	// This will be skewed toward smaller stripe indexes if numstripes does not divide 256.
	stripeIndex := hash % uint32(this.numStripes)

	// FNV seems to be 2x slower than CRC32. Included for historical reasons but commented out.
	// keyBytes := []byte(key)
	// hasher := fnv.New32a()
	// n, err := hasher.Write(keyBytes)
	// if err != nil || n != len(keyBytes) {
	// 	panic("hashing fail")
	// }
	// hashBytes := hasher.Sum(make([]byte, 0))
	// if len(hashBytes) != 4 {
	// 	panic("hash output had wrong length")
	// }
	// stripeIndex := int(hashBytes[0]) % this.numStripes

	return this.stripes[stripeIndex]
}

// Return a new slice of stripes. Used to initialize and reset the cache.
func newStripes(numStripes int) []*stripe {
	stripes := make([]*stripe, numStripes)
	for i := 0; i < numStripes; i++ {
		stripes[i] = newStripe()
	}
	return stripes
}
