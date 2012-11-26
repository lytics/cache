cache
=====

In-memory concurrent cache data structure for go (golang). Use this when you have a bunch of goroutines that
need to share a single in-memory database cache with time-based expiration.

The interface is currently very (overly) simple. There is a single fixed TTL, and elements
will be removed from the cache when Expire() is called. There is currently no way to insert
an element into the cache manually.

The cache has a non-negative "size" that is used to check if the cache has grown past its max
size. The size of a cache is just the sum of the sizes of the cache entries. The size of each
cache entry is determined by a function provided by the application when a new cache is
created. 

Cache entry expiration does not happen automatically (by age or by size). The application code 
must call cache.Expire(), which will atomically remove and return the cache entries that were
removed. The application can then choose to handle these expired entries or ignore them. This
is handy for a writeback cache, where updates are kept in memory and periodically flushed to
other storage.

Design note: we could have allowed the cache to handle eviction itself automatically. This
would open a very complicated can of worms, since we'd have to handle the possibility of IO
errors during eviction, and report those errors back to the application. This would greatly
complicate the interface and make cache usage less intuitive.


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
		cache.Expire() // Enforce size and age constraints on the cache
		// process this request
// ...
```
