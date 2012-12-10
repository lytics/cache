package cache

import (
	// "fmt"
	"github.com/bmizerany/assert"
	"strconv"
	"testing"
	"time"
)

func TestBasic(t *testing.T) {
	db := &FakeDatabase{100, 0}

	loadFunc := func(k string) (interface{}, error) {
		return db.Get(k)
	}

	sizeFunc := func(v interface{}) int64 {
		return int64(1)
	}

	ttl := 100 * time.Millisecond
	maxSize := int64(100)

	for _, nStripes := range []int{1, 2, 4, 5, 8, 16} {
		cache := NewCache(nStripes, loadFunc, sizeFunc)

		for _, keyAsInt := range []int{1, 2, 3, 4} {
			keyAsString := intToString(keyAsInt)
			result, err := cache.GetOrLoad(keyAsString)
			assert.Equal(t, nil, err)
			assert.Equal(t, keyAsInt*10, result)
		}

		// Look up a key that doesn't exist in the backing database, should return nil
		absentResult, err := cache.GetOrLoad("99999")
		assert.Equal(t, nil, err)
		assert.Equal(t, nil, absentResult)

		time.Sleep(ttl + 1)
		cache.Expire(maxSize, ttl) // all cache entries are past ttl, should clear the cache

		assert.Equal(t, int64(0), cache.Size())
	}
}

func TestTimeLimit(t *testing.T) {
	loadFunc := func(k string) (interface{}, error) {
		keyAsInt := stringToInt(k)
		return keyAsInt * 10, nil
	}

	sizeFunc := func(v interface{}) int64 {
		return int64(1)
	}

	ttl := 100 * time.Millisecond
	batchSize := 10
	maxSize := int64(batchSize * 5)
	cache := NewCache(16, loadFunc, sizeFunc)

	// We insert elements into the cache in two batches, with a sleep between them. We'll
	// check that the elements in the first batch time out before the ones in the second
	// batch.

	for i := 0; i < batchSize; i++ {
		_, err := cache.GetOrLoad(strconv.FormatInt(int64(i), 10))
		assert.Equal(t, nil, err)
	}

	time.Sleep(ttl/2 + 1)

	// Now insert the second batch
	cache.Expire(maxSize, ttl)
	assert.Equal(t, batchSize, int(cache.Size())) // The first batch should still be there
	for i := batchSize; i < 2*batchSize; i++ {
		_, err := cache.GetOrLoad(strconv.FormatInt(int64(i), 10))
		assert.Equal(t, nil, err)
	}

	time.Sleep(ttl/2 + 1)

	assert.Equal(t, 2*batchSize, int(cache.Size())) // The first batch should still be there
	expiredElems := cache.Expire(maxSize, ttl)
	for i := 0; i < batchSize; i++ {
		val, isPresent := expiredElems[intToString(i)]
		assert.T(t, isPresent)
		assert.Equal(t, i*10, val) // Times 10 because that's what the fake DB returned
	}
	assert.Equal(t, batchSize, int(cache.Size())) // The first batch should be out of the cache

	time.Sleep(ttl/2 + 1)

	// Now the second batch should time out also
	assert.Equal(t, batchSize, int(cache.Size()))
	expiredElems = cache.Expire(maxSize, ttl)
	for i := batchSize; i < 2*batchSize; i++ {
		val, isPresent := expiredElems[intToString(i)]
		assert.T(t, isPresent)
		assert.Equal(t, i*10, val)
	}
	assert.Equal(t, 0, int(cache.Size())) // All cache entries should have timed out

}

func TestSizeLimit(t *testing.T) {
	loadFunc := func(k string) (interface{}, error) {
		keyAsInt := stringToInt(k)
		return keyAsInt * 10, nil
	}

	sizeFunc := func(v interface{}) int64 {
		return int64(1)
	}

	ttl := time.Millisecond * 10
	sizeLimit := int64(5)
	cache := NewCache(16, loadFunc, sizeFunc)

	for i := 0; i < int(sizeLimit); i++ {
		cache.GetOrLoad(intToString(i))
		cache.Expire(sizeLimit, ttl)
		assert.Equal(t, i+1, int(cache.Size()))
	}

	for i := sizeLimit; i < sizeLimit*2; i++ {
		cache.GetOrLoad(intToString(int(i)))
		cache.Expire(sizeLimit, ttl)
		assert.Equal(t, sizeLimit, cache.Size())
	}

	time.Sleep(ttl + 1)

	expiredElems := cache.Expire(sizeLimit, ttl)
	assert.Equal(t, 0, int(cache.Size()))
	for i := sizeLimit; i < sizeLimit*2; i++ {
		v, isPresent := expiredElems[intToString(int(i))]
		assert.T(t, isPresent)
		assert.Equal(t, int(i*10), v)
	}
}

func TestCombine(t *testing.T) {
	loadFunc := func(k string) (interface{}, error) {
		return 50, nil
	}

	sizeFunc := func(v interface{}) int64 {
		return int64(1)
	}

	cache := NewCache(16, loadFunc, sizeFunc)

	// A combiner that treats cache entries as ints and combining as addition
	combiner := func(_ string, existing interface{}, newVal interface{}) (interface{}, error) {
		if existing == nil {
			return newVal, nil
		}
		return existing.(int) + newVal.(int), nil
	}

	cache.Combine("myKey", 2, combiner)
	assert.Equal(t, 52, cache.GetOrLoadNoErr("myKey"))
	assert.Equal(t, 62, cache.CombineNoErr("myKey", 10, combiner))
	assert.Equal(t, 62, cache.GetOrLoadNoErr("myKey"))
	assert.Equal(t, int64(1), cache.Size())

	// Check that cache entries are successfully removed when the combiner returns nil
	deleterCombiner := func(_ string, existing interface{}, newVal interface{}) (interface{}, error) {
		return nil, nil
	}
	assert.Equal(t, nil, cache.CombineNoErr("myKey", 123, deleterCombiner))
	assert.Equal(t, int64(0), cache.Size())
	assert.Equal(t, 50, cache.GetOrLoadNoErr("myKey")) // Cache loader should run
	assert.Equal(t, int64(1), cache.Size())
}

func TestEviction(t *testing.T) {
	loadFunc := func(k string) (interface{}, error) {
		return strconv.Atoi(k)
	}
	sizeFunc := func(v interface{}) int64 {
		return int64(1)
	}

	cache := NewCache(16, loadFunc, sizeFunc)

	numItems := 5
	for i := 0; i < numItems; i++ {
		cache.GetOrLoadNoErr(strconv.FormatInt(int64(i), 10))
	}
	evictedItems := make(chan int, numItems)
	evictionHandler := func(k string, v interface{}) error {
		evictedItems <- v.(int)
		return nil
	}
	err := cache.ExpireAndHandle(5, -1, evictionHandler) // negative ttl: expire all
	assert.Equal(t, nil, err)

	for i := 0; i < numItems; i++ {
		select {
		case <-evictedItems:
			continue
		default:
			panic("Chan should have had a value to read")
		}
	}

	select {
	case <-evictedItems:
		panic("Chan shouldn't have any more values to read")
	default:
		break
	}
}

func TestEvictAll(t *testing.T) {
	loadFunc := func(k string) (interface{}, error) {
		return strconv.Atoi(k)
	}
	sizeFunc := func(v interface{}) int64 {
		return int64(1)
	}
	cache := NewCache(16, loadFunc, sizeFunc)

	for i := 0; i < 5; i++ {
		cache.GetOrLoad(strconv.FormatInt(int64(i), 10))
	}

	assert.Equal(t, int64(5), cache.Size())
	evictCount := 0
	err := cache.EvictAll(func(_ string, _ interface{}) error {
		evictCount++
		return nil
	})
	assert.Equal(t, int64(0), cache.Size())
	assert.Equal(t, nil, err)
	assert.Equal(t, 5, evictCount)
}

func TestHitAndMiss(t *testing.T) {
	// Use a loader func that verifies that hits and misses are both happening correctly
}

func TestLoaderErr(t *testing.T) {
	// Check that the cache remains sane if the loader returns error
}

// TODO example of flushing expired writes

// A pretend database implementation that returns the input key*10. If maxKey is >0, then
// the return value is nil if the input key is greater than maxKey.
// It sleeps during lookup to simulate the effect of database latency.
type FakeDatabase struct {
	maxKey     int64
	simLatency time.Duration
}

func (this *FakeDatabase) Get(key string) (interface{}, error) {
	time.Sleep(this.simLatency)
	i := stringToInt(key)
	if this.maxKey == 0 || (i >= 0 && int64(i) <= this.maxKey) {
		return 10 * i, nil
	}
	return nil, nil
}

// Because the int to string boilerplate is silly and repetitious
func intToString(i int) string {
	return strconv.FormatInt(int64(i), 10)
}

// Because the string to int boilerplate is silly and repetitious
func stringToInt(s string) int {
	asInt, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return asInt
}
