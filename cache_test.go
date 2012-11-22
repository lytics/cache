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
		cache := NewCache(nStripes, maxSize, ttl, loadFunc, sizeFunc)

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
		cache.Expire() // all cache entries are past their ttl so this should empty the cache

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
	cache := NewCache(16, int64(batchSize*5), ttl, loadFunc, sizeFunc)

	// We insert elements into the cache in two batches, with a sleep between them. We'll
	// check that the elements in the first batch time out before the ones in the second
	// batch.

	for i := 0; i < batchSize; i++ {
		_, err := cache.GetOrLoad(strconv.FormatInt(int64(i), 10))
		assert.Equal(t, nil, err)
	}

	time.Sleep(ttl/2 + 1)

	// Now insert the second batch
	cache.Expire()
	assert.Equal(t, batchSize, int(cache.Size())) // The first batch should still be there
	for i := batchSize; i < 2*batchSize; i++ {
		_, err := cache.GetOrLoad(strconv.FormatInt(int64(i), 10))
		assert.Equal(t, nil, err)
	}

	time.Sleep(ttl/2 + 1)

	assert.Equal(t, 2*batchSize, int(cache.Size())) // The first batch should still be there
	expiredElems := cache.Expire()
	for i := 0; i < batchSize; i++ {
		val, isPresent := expiredElems[intToString(i)]
		assert.T(t, isPresent)
		assert.Equal(t, i*10, val) // Times 10 because that's what the fake DB returned
	}
	assert.Equal(t, batchSize, int(cache.Size())) // The first batch should be out of the cache

	time.Sleep(ttl/2 + 1)

	// Now the second batch should time out also
	assert.Equal(t, batchSize, int(cache.Size()))
	expiredElems = cache.Expire()
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
	sizeLimit := 5
	cache := NewCache(16, int64(sizeLimit), ttl, loadFunc, sizeFunc)

	for i := 0; i < sizeLimit; i++ {
		cache.GetOrLoad(intToString(i))
		cache.Expire()
		assert.Equal(t, i+1, int(cache.Size()))
	}

	for i := sizeLimit; i < sizeLimit*2; i++ {
		cache.GetOrLoad(intToString(i))
		cache.Expire()
		assert.Equal(t, sizeLimit, int(cache.Size()))
	}

	time.Sleep(ttl + 1)

	expiredElems := cache.Expire()
	assert.Equal(t, 0, int(cache.Size()))
	for i := sizeLimit; i < sizeLimit*2; i++ {
		v, isPresent := expiredElems[intToString(i)]
		assert.T(t, isPresent)
		assert.Equal(t, i*10, v)
	}
}

func TestHitAndMiss(t *testing.T) {
	// Use a loader func that verifies that hits and misses are both happening correctly
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
