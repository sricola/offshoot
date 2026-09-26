package main

import (
	"sort"
	"sync"
	"time"
)

type sample struct {
	depth int
	d     time.Duration
}

// metrics collects per-operation latency samples tagged with the depth of
// the node the operation was performed on, plus the branch-management time
// that goes into the overhead ratio.
type metrics struct {
	mu         sync.Mutex
	ops        map[string][]sample
	branchTime time.Duration
	casRetries int
}

// branchOps are the four operations that count as branch management (the
// numerator of the branching overhead ratio); eval is productive work.
var branchOps = map[string]bool{"fork": true, "checkout": true, "checkpoint": true, "destroy": true}

func newMetrics() *metrics { return &metrics{ops: map[string][]sample{}} }

func (m *metrics) add(op string, depth int, d time.Duration) {
	m.mu.Lock()
	m.ops[op] = append(m.ops[op], sample{depth: depth, d: d})
	if branchOps[op] {
		m.branchTime += d
	}
	m.mu.Unlock()
}

// totals returns the branch-management time and CAS-retry count. Called
// after every worker has finished, but through the mutex all the same.
func (m *metrics) totals() (time.Duration, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.branchTime, m.casRetries
}

func (m *metrics) casRetry() {
	m.mu.Lock()
	m.casRetries++
	m.mu.Unlock()
}

// quantiles returns p50 and p99 of op's samples at exactly depth, and the
// sample count. Nearest-rank, no interpolation.
func (m *metrics) quantiles(op string, depth int) (p50, p99 time.Duration, n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ds []time.Duration
	for _, s := range m.ops[op] {
		if s.depth == depth {
			ds = append(ds, s.d)
		}
	}
	if len(ds) == 0 {
		return 0, 0, 0
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	return ds[rank(len(ds), 0.50)], ds[rank(len(ds), 0.99)], len(ds)
}

func rank(n int, q float64) int {
	i := int(q * float64(n))
	if i >= n {
		i = n - 1
	}
	return i
}
