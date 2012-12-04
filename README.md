cache
=====

In-memory concurrent cache data structure for go (golang). Use this when you have a bunch of
goroutines that need to share a single in-memory database cache with time-based expiration.

The interface is currently very (overly) simple. There is a single fixed TTL, and elements
will be removed from the cache when Expire() is called. There is currently no way to insert
an element into the cache manually.

The cache has a non-negative "size" that is used to check if the cache has grown past its max
size. The size of a cache is just the sum of the sizes of the cache entries. The size of each
cache entry is determined by a Sizer function provided by the application when a new cache is
created. The cache size does not include the size of the cache keys, which might matter if you
have many cache entries or large cache keys.

Cache entry expiration does not happen automatically (by age or by size). The application code 
must call cache.Expire(), which accepts an "eviction handler" function that will be called for 
each evicted item. For example, you could have an eviction handler that writes expired entries to
a database (a "writeback cache"). If the eviction handler encounters an error, the cache entry is
not evicted and the whole cache expiration operation returns the error.


Example
-------

```go
// Setup
loadFunc := func(k string) (interface{}, error) {
	return myDatabase.Lookup(k)
}

sizeFunc := func(v interface{}) int64 {
	return v.(*MyUserModelOrWhatever).Size()
}

ttl := 10 * time.Second
maxSize := int64(1e6)
cache := NewCache(nStripes, maxSize, ttl, loadFunc, sizeFunc)

// Process requests
select {
	case req := <-reqs:
		result, err := cache.GetOrLoad(req.UserIdOrWhatever)
		expiredEntries := cache.Expire() // Enforce size and age constraints on the cache
		// process this request
// ...
```
