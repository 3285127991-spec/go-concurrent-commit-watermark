// Package commitwatermark implements a concurrency-safe commit watermark
// tracker. IDs are issued in strictly increasing order and may complete out
// of order; the committed watermark only advances over contiguously
// completed IDs.
package commitwatermark

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
)

var (
	// ErrClosed is returned by operations attempted after Close.
	ErrClosed = errors.New("commitwatermark: tracker closed")
	// ErrNotIssued is returned when completing an ID that was never issued.
	ErrNotIssued = errors.New("commitwatermark: id not issued")
	// ErrAlreadyCompleted is returned when completing an ID twice.
	ErrAlreadyCompleted = errors.New("commitwatermark: id already completed")
)

// Snapshot is an independent copy of the tracker state.
type Snapshot struct {
	// Committed is the current watermark: the largest ID such that every
	// issued ID up to it has completed.
	Committed int64
	// LastIssued is the most recently issued ID.
	LastIssued int64
	// Pending lists completed IDs not yet covered by the watermark, in
	// increasing order.
	Pending []int64
	// Waiters is the number of currently blocked Wait calls.
	Waiters int
}

// Tracker tracks issued and completed IDs and maintains a commit watermark.
// The zero value is not usable; construct with NewTracker.
type Tracker struct {
	mu         sync.Mutex
	start      int64
	lastIssued int64
	committed  int64
	pending    map[int64]struct{}
	waiters    int
	closed     bool
	// changed is closed and replaced whenever the watermark advances or
	// the tracker closes, broadcasting to all blocked waiters.
	changed chan struct{}
}

// NewTracker returns a Tracker whose first issued ID is start.
func NewTracker(start int64) *Tracker {
	return &Tracker{
		start:      start,
		lastIssued: start - 1,
		committed:  start - 1,
		pending:    make(map[int64]struct{}),
		changed:    make(chan struct{}),
	}
}

// Issue returns the next ID. IDs are strictly increasing and never repeat.
func (t *Tracker) Issue() (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return 0, ErrClosed
	}
	t.lastIssued++
	return t.lastIssued, nil
}

// Complete marks id as completed. It may be called out of order; the
// watermark advances only past contiguously completed IDs.
func (t *Tracker) Complete(id int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ErrClosed
	}
	if id < t.start || id > t.lastIssued {
		return fmt.Errorf("commitwatermark: complete %d: %w", id, ErrNotIssued)
	}
	if id <= t.committed {
		return fmt.Errorf("commitwatermark: complete %d: %w", id, ErrAlreadyCompleted)
	}
	if _, ok := t.pending[id]; ok {
		return fmt.Errorf("commitwatermark: complete %d: %w", id, ErrAlreadyCompleted)
	}
	t.pending[id] = struct{}{}

	advanced := false
	for {
		next := t.committed + 1
		if _, ok := t.pending[next]; !ok {
			break
		}
		delete(t.pending, next)
		t.committed = next
		advanced = true
	}
	if advanced {
		t.broadcastLocked()
	}
	return nil
}

// Wait blocks until the watermark reaches id, ctx is done, or the tracker
// is closed. It returns nil once Committed() >= id, ctx.Err() on
// cancellation, or ErrClosed if the tracker is closed first.
func (t *Tracker) Wait(ctx context.Context, id int64) error {
	t.mu.Lock()
	if t.committed >= id {
		t.mu.Unlock()
		return nil
	}
	if t.closed {
		t.mu.Unlock()
		return ErrClosed
	}
	t.waiters++
	for {
		changed := t.changed
		t.mu.Unlock()
		select {
		case <-ctx.Done():
			t.mu.Lock()
			t.waiters--
			t.mu.Unlock()
			return ctx.Err()
		case <-changed:
			t.mu.Lock()
		}
		if t.committed >= id {
			t.waiters--
			t.mu.Unlock()
			return nil
		}
		if t.closed {
			t.waiters--
			t.mu.Unlock()
			return ErrClosed
		}
	}
}

// Committed returns the current watermark.
func (t *Tracker) Committed() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.committed
}

// Snapshot returns an independent copy of the tracker state.
func (t *Tracker) Snapshot() Snapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	pending := make([]int64, 0, len(t.pending))
	for id := range t.pending {
		pending = append(pending, id)
	}
	slices.Sort(pending)
	return Snapshot{
		Committed:  t.committed,
		LastIssued: t.lastIssued,
		Pending:    pending,
		Waiters:    t.waiters,
	}
}

// Close shuts the tracker down: Issue and Complete start returning
// ErrClosed and blocked Wait calls return ErrClosed. Close is idempotent.
func (t *Tracker) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	t.broadcastLocked()
	return nil
}

// broadcastLocked wakes all blocked waiters. Callers must hold t.mu.
func (t *Tracker) broadcastLocked() {
	close(t.changed)
	t.changed = make(chan struct{})
}
