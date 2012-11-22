cache
=====

In-memory concurrent cache data structure for go (golang). Use this when you have a bunch of goroutines that
need to share a single in-memory database cache.

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
		// process this request
// ...
```
