package cache

/* 
These benchmarks test raw in-memory throughput, without any simulated database latency.
This basically just shows that there's no crazy lock contention and that throughput
does actually improve by adding more processes and goroutines.
Typical throughput is ~1M ops/sec on my laptop for the good configurations (~1 usec/op)
*/

import (
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

func BenchmarkMem_8pr_64strp_16gort_200maxsize_HourExpire(b *testing.B) {
	doBenchmark(b.N, b, 8, 64, 16, int64(200), time.Hour, int64(900), 0)
}

func BenchmarkMem_8pr_32strp_16gort_200maxsize_HourExpire(b *testing.B) {
	doBenchmark(b.N, b, 8, 32, 16, int64(200), time.Hour, int64(900), 0)
}

func BenchmarkMem_4pr_32strp_16gort_200maxsize_HourExpire(b *testing.B) {
	doBenchmark(b.N, b, 4, 32, 16, int64(200), time.Hour, int64(900), 0)
}

func BenchmarkMem_4pr_16strp_16gort_200maxsize_HourExpire(b *testing.B) {
	doBenchmark(b.N, b, 4, 16, 16, int64(200), time.Hour, int64(900), 0)
}

func BenchmarkMem_4pr_64strp_16gort_200maxsize_HourExpire(b *testing.B) {
	doBenchmark(b.N, b, 4, 64, 16, int64(200), time.Hour, int64(900), 0)
}

func BenchmarkMem_4pr_64strp_8gort_200maxsize_HourExpire(b *testing.B) {
	doBenchmark(b.N, b, 4, 64, 8, int64(200), time.Hour, int64(900), 0)
}

func BenchmarkMem_4pr_64strp_32gort_200maxsize_HourExpire(b *testing.B) {
	doBenchmark(b.N, b, 4, 64, 32, int64(200), time.Hour, int64(900), 0)
}

// A degenerate case where there are many procs and goroutines competing for 1 stripe lock.
func BenchmarkMem_8pr_1strp_16gort_200maxsize_HourExpire(b *testing.B) {
	doBenchmark(b.N, b, 8, 1, 16, int64(200), time.Hour, int64(900), 0)
}

// The single-threaded case, with no concurrency.
func BenchmarkMem_1pr_1strp_16gort_200maxsize_HourExpire(b *testing.B) {
	doBenchmark(b.N, b, 1, 1, 16, int64(200), time.Hour, int64(900), 0)
}

func BenchmarkMem_8pr_1strp_1gort_200maxsize_HourExpire(b *testing.B) {
	doBenchmark(b.N, b, 8, 1, 1, int64(200), time.Hour, int64(900), 0)
}

// This isn't a proper golang "benchmark," it hard-codes the number of iterations and has
// no meaningful concept of latency per operation.
func TestCacheWarmup(t *testing.T) {
	runBenchmark := func(count int) float64 {
		beforeNanos := time.Now().UnixNano()
		doBenchmark(count, nil, 8, 128, 128, int64(100000), time.Hour, int64(900000), 0)
		duration := time.Now().UnixNano() - beforeNanos
		throughput := float64(count) / float64(duration)
		return throughput
	}

	firstCount := 10000
	firstThroughput := runBenchmark(firstCount)
	fmt.Printf("%d ops throughput=%f\n", firstCount, firstThroughput)

	secondCount := firstCount * 5
	secondThroughput := runBenchmark(secondCount)
	fmt.Printf("%d ops throughput=%f\n", secondCount, secondThroughput)

	speedUp := secondThroughput / firstThroughput
	fmt.Printf("Speedup: %f\n", speedUp)
}

func doBenchmark(numOps int, b *testing.B, gomaxprocs int, numStripes int, numGoRoutines int,
	maxSize int64, maxAge time.Duration, fakeDbMax int64, simLatency time.Duration) {

	if b != nil {
		b.StopTimer()
	}

	origGomaxprocs := runtime.GOMAXPROCS(-1)
	runtime.GOMAXPROCS(gomaxprocs)

	db := &FakeDatabase{fakeDbMax, simLatency}
	loader := func(key string) (interface{}, error) {
		return db.Get(key)
	}

	sizer := func(x interface{}) int64 {
		return 1
	}

	randMax := int64(1000000)

	opsForGoroutine := make([]int, numGoRoutines)
	for i := 0; i < numGoRoutines; i++ {
		opsForGoroutine[i] = numOps / numGoRoutines
	}
	remainder := numOps - (numGoRoutines * (numOps / numGoRoutines))
	for i := 0; i < remainder; i++ {
		opsForGoroutine[i] += 1
	}

	initWg := new(sync.WaitGroup)
	finishedWg := new(sync.WaitGroup)
	initWg.Add(1)
	finishedWg.Add(numGoRoutines)

	cache := NewCache(numStripes, maxSize, maxAge, loader, sizer)

	for i := 0; i < numGoRoutines; i++ {
		go func(numOpsForThread int, seed int) {
			// fmt.Printf("numops: %d\n", numOpsForThread)
			rng := rand.New(rand.NewSource(int64(seed)))

			// We use a zipfian distribution of key lookup frequency to simulate the fact
			// that different cache keys are more popular than others.
			// The params to zipf are totally hacked, I just eyeballed the output values and 
			// the distribution looks kind of OK-ish.
			// We probably shouldn't hardcode these, and instead take them as arguments.
			zipf := rand.NewZipf(rng, 1.1, 10, uint64(randMax))
			initWg.Wait() // Block until all goroutines are ready

			for j := 0; j < numOpsForThread; j++ {
				r := zipf.Uint64()
				// fmt.Printf("zipf: %d\t", r)
				result, err := cache.GetOrLoad(strconv.FormatInt(int64(r), 10))
				cache.Expire() // Enforce cache size and max age constraints
				if err != nil {
					panic("Cache lookup error")
				}
				if r <= uint64(fakeDbMax) && result != 10*int(r) {
					panic(fmt.Sprintf("Unexpected result %T:%d looking up %T:%d",
						result, result, r, r))
				} else if r > uint64(fakeDbMax) && result != nil {
					panic("Result should have been nil")
				}

			}
			finishedWg.Done()
		}(opsForGoroutine[i], i)
	}

	if b != nil {
		b.StartTimer()
	}
	initWg.Done()     // Unblock worker goroutines
	finishedWg.Wait() // Wait for workers to finish

	runtime.GOMAXPROCS(origGomaxprocs)
}
