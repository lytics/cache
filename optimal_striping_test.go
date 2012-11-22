package cache

/*
These benchmarks let us find the right number of stripes to use for a certain level of DB latency.

Weaknesses:
 Database latency isn't really constant, we could give it a probability distribution
*/

import (
	"fmt"
	"testing"
	"time"
)

const (
	latency = 20 * time.Millisecond
	numOps  = 2000
)

// func BenchmarkDbLatency_4pr_128strp_32gort_maxsize_HourExpire(b *testing.B) {
// 	doBenchmark(b.N, 4, 128, 32, int64(1000000), int64(100000), time.Hour, int64(900000),
// 		latency)
// }

// func BenchmarkDbLatency_4pr_128strp_16gort_maxsize_HourExpire(b *testing.B) {
// 	doBenchmark(b.N, 4, 128, 16, int64(1000000), int64(100000), time.Hour, int64(900000),
// 		latency)
// }

// func BenchmarkDbLatency_4pr_64strp_16gort_maxsize_HourExpire(b *testing.B) {
// 	doBenchmark(b.N, 4, 64, 16, int64(1000000), int64(100000), time.Hour, int64(900000),
// 		latency)
// }

func BenchmarkDbLatency_128pr_128strp_64gort_maxsize_HourExpire(b *testing.B) {
	b.N = numOps
	fmt.Printf("(Starting benchmark)")
	doBenchmark(numOps, b, 128, 128, 64, int64(100000), time.Hour, int64(900000),
		latency)
}

func BenchmarkDbLatency_8pr_128strp_128gort_maxsize_HourExpire(b *testing.B) {
	b.N = numOps
	fmt.Printf("(Starting benchmark)")
	doBenchmark(numOps, b, 8, 128, 128, int64(100000), time.Hour, int64(900000),
		latency)
}
