package cache

import (
	"container/list"
	// "fmt"
	"hash/crc32"
	"sync"
	"time"
)

// A Cache is a concurrent in-memory data structure backed by a database. It internally uses
// lock striping to try to achieve high throughput when used by many goroutines.
// TODO: we might want to support Remove(k), Add(k, v), Combine(k, v, func(v,v))
type Cache struct {
	stripes    []*stripe
	numStripes int
	maxSize    int64
	maxAge     int64
	loader     CacheLoader
	sizer      Sizer
	cacheLock  *sync.RWMutex
}

// Make a new cache.
//  - numStripes: how many partitions/locks this cache should have
//  - maxSize: the size after which the cache should start evicting writes. The cache is likely
//             to exceed this size; it's the caller's responsibility to periodically call
//             Expire() to bring the cache size back down to the max.
//  - loader: the caller provides this implementation to retrieve a cache entry from the
//            backing database if it's not already in the cache.
//  - sizer: the caller provides this implementation to calculate the size of a cache entry.
func NewCache(numStripes int, maxSize int64, maxAge time.Duration, loader CacheLoader,
	sizer Sizer) *Cache {

	return &Cache{
		stripes:    newStripes(numStripes),
		numStripes: numStripes,
		maxSize:    maxSize,
		maxAge:     int64(maxAge),
		loader:     loader,
		sizer:      sizer,
		cacheLock:  new(sync.RWMutex),
	}
}

// Look up the given key in the cache. If it's not in the cache, load it using the loader that
// was passed to NewCache.
func (this *Cache) GetOrLoad(key string) (interface{}, error) {
	this.cacheLock.RLock() // Mutual exclusion against threads computing cache expiration
	defer this.cacheLock.RUnlock()

	stripe := this.getStripe(key)

	// Future note: if renewOnRead is false, then reads do not modify the cache. In that case 
	// we could allow multiple concurrent readers on the same stripe by putting an RWMutex in 
	// struct stripe instead of Mutex.	
	stripe.lock.Lock() // Mutual exclusion against threads reading/writing the same stripe
	defer stripe.lock.Unlock()

	elem, isPresent := stripe.elemMap[key]
	if isPresent {
		keyValue := elem.Value.(*keyValue)
		// fmt.Printf("Hit!\t")
		return keyValue.v, nil
	}
	// fmt.Printf("Miss!\t")

	// For performance, we could do this without holding the stripe lock? But we also
	// want to prevent loading the same key multiple times concurrently to avoid slamming
	// the backing database with load spikes. Maybe we need a separate lock table for loads.
	v, err := this.loader(key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil // We do not cache negative responses.
	}
	size := this.sizer(v)
	keyValue := newKeyValue(key, v, time.Now().UnixNano(), size)
	newElem := stripe.lst.PushBack(keyValue)
	stripe.totalSize += size
	stripe.elemMap[key] = newElem
	return v, err
}

// Atomically removes and returns all cache elements such that:
//  - the element's timestamp is older than the max age
//  - the element is one of the oldest and the cache is over its target size
func (this *Cache) Expire() map[string]interface{} {
	// TODO running this function with the global lock absolutely destroys throughput. (~8x)

	this.cacheLock.Lock()
	defer this.cacheLock.Unlock()

	returnMap := make(map[string]interface{})
	expireNanos := time.Now().UnixNano() - int64(this.maxAge)

	// Now remove all elements older than their expiration time
	for _, stripe := range this.stripes {
		elem := stripe.lst.Front()
		for elem != nil {
			keyValue := elem.Value.(*keyValue)
			if keyValue.nanos >= expireNanos {
				break // no more values to expire in this stripe
			}
			delete(stripe.elemMap, keyValue.k)
			returnMap[keyValue.k] = keyValue.v
			stripe.totalSize -= keyValue.size
			elemToRemove := elem
			elem = elem.Next()
			stripe.lst.Remove(elemToRemove)
		}
	}

	// Calculate the total size of all stripes
	sizeAllStripes := int64(0)
	for _, stripe := range this.stripes {
		sizeAllStripes += stripe.totalSize
	}

	// Now remove elements as necessary to enforce the size limit
	for sizeAllStripes > this.maxSize {
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
		kvToRemove := elemToRemove.Value.(*keyValue)
		stripeToTakeFrom.lst.Remove(elemToRemove)

		returnMap[kvToRemove.k] = kvToRemove.v
		stripeToTakeFrom.totalSize -= kvToRemove.size
		sizeAllStripes -= kvToRemove.size
	}

	return returnMap
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

// A function that can look up a key that's not already in the cache. If this function panics
// or calls one of the cache functions, that's very bad and the result is undefined. Most
// implementations of this function will do a database lookup.
type CacheLoader func(key string) (interface{}, error)

// A function that can estimate the size of a cache entry. This is used to enforce the max
// size of the cache.
type Sizer func(interface{}) int64

// These are the elements that go in the FIFO expiration lists.
type keyValue struct {
	k     string      // The cache key for this value
	v     interface{} // The value stored in the cache under this key
	nanos int64       // The timestamp that this element was last touched.
	size  int64       // The key length + the size of v, as estimated by the Sizer
}

func newKeyValue(k string, v interface{}, nanos int64, size int64) *keyValue {
	return &keyValue{
		k:     k,
		v:     v,
		nanos: nanos,
		size:  size,
	}
}

// The following are internal private functions.

type stripe struct {
	lst       *list.List
	lock      *sync.Mutex
	totalSize int64
	elemMap   map[string]*list.Element
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
