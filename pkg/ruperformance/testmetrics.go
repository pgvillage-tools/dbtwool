package ruperformance

import (
	"sync"
	"sync/atomic"
	"time"
)

type testMetrics struct {
	measuring     atomic.Int32
	olapCompleted atomic.Int64
	oltpOps       atomic.Int64

	startOnce sync.Once
	startTime atomic.Int64 // unix nano
}

func (m *testMetrics) markStart() {
	m.startOnce.Do(func() { m.startTime.Store(time.Now().UnixNano()) })
}

func (m *testMetrics) startTimeOrFallback(executionTimeSec int) time.Time {
	if ns := m.startTime.Load(); ns != 0 {
		return time.Unix(0, ns)
	}
	return time.Now().Add(-time.Duration(executionTimeSec) * time.Second)
}
