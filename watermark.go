// Package commitwatermark implements a concurrency-safe commit watermark
// tracker.
//
// A Tracker issues strictly increasing IDs. Completions may arrive out of
// order, but the committed watermark only advances past a contiguous run of
// completed IDs: if IDs 1 and 3 are complete, the watermark stays at 1 until
// 2 completes, then jumps to 3. Wait blocks until the watermark reaches a
// target ID, the tracker is closed, or the caller's context is done.
package commitwatermark

import (
	"context"
	"errors"
	"math"
	"slices"
	"sync"
)

var (
	// ErrClosed is returned by Issue, Complete and Wait after Close.
	ErrClosed = errors.New("commitwatermark: tracker closed")
	// ErrNotIssued is returned by Complete for an ID outside the issued range.
	ErrNotIssued = errors.New("commitwatermark: id not issued")
	// ErrAlreadyCompleted is returned by Complete for a repeated completion.
	ErrAlreadyCompleted = errors.New("commitwatermark: id already completed")
	// ErrExhausted is returned by Issue when the ID space is exhausted.
	ErrExhausted = errors.New("commitwatermark: id space exhausted")
)

// Snapshot is an independent copy of the tracker state at a point in time.
type Snapshot struct {
	// Committed is the current watermark: the largest ID such that every
	// issued ID up to and including it has completed.
	Committed uint64
	// LastIssued is the most recently issued ID.
	LastIssued uint64
	// Pending lists, in ascending order, completed IDs not yet covered by
	// the watermark because of earlier gaps.
	Pending []uint64
	// Waiters is the number of goroutines currently blocked in Wait.
	Waiters int
}

// Tracker tracks issued and completed IDs and the commit watermark.
// The zero value is not usable; construct one with New.
type Tracker struct {
	mu         sync.Mutex
	start      uint64
	lastIssued uint64
	committed  uint64
	pending    map[uint64]struct{}
	broadcast  chan struct{} // closed and replaced on every watermark advance; closed for good on Close
	waiters    int
	closed     bool
}

// New returns a Tracker whose first issued ID is start, which must be >= 1.
// The initial watermark is start-1, i.e. nothing is committed yet.
func New(start uint64) (*Tracker, error) {
	if start == 0 {
		return nil, errors.New("commitwatermark: start must be >= 1")
	}
	return &Tracker{
		start:      start,
		lastIssued: start - 1,
		committed:  start - 1,
		pending:    make(map[uint64]struct{}),
		broadcast:  make(chan struct{}),
	}, nil
}

// Issue returns the next ID. IDs are strictly increasing and never repeated.
// It fails with ErrClosed after Close.
func (t *Tracker) Issue() (uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return 0, ErrClosed
	}
	if t.lastIssued == math.MaxUint64 {
		return 0, ErrExhausted
	}
	t.lastIssued++
	return t.lastIssued, nil
}

// Complete marks id as completed and advances the watermark past any
// contiguous run of completed IDs. It fails with ErrNotIssued for an ID
// outside the issued range, ErrAlreadyCompleted for a repeated completion,
// and ErrClosed after Close.
func (t *Tracker) Complete(id uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ErrClosed
	}
	if id < t.start || id > t.lastIssued {
		return ErrNotIssued
	}
	if id <= t.committed {
		return ErrAlreadyCompleted
	}
	if _, ok := t.pending[id]; ok {
		return ErrAlreadyCompleted
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
		close(t.broadcast)
		t.broadcast = make(chan struct{})
	}
	return nil
}

// Committed returns the current watermark.
func (t *Tracker) Committed() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.committed
}

// Wait blocks until the watermark reaches id, the tracker is closed, or ctx
// is done. It returns nil once the watermark has reached id, ErrClosed if
// the tracker is closed first, and ctx.Err() if ctx is done first.
// Cancelling ctx only affects this call.
func (t *Tracker) Wait(ctx context.Context, id uint64) error {
	t.mu.Lock()
	for {
		if t.committed >= id {
			t.mu.Unlock()
			return nil
		}
		if t.closed {
			t.mu.Unlock()
			return ErrClosed
		}
		ch := t.broadcast
		t.waiters++
		t.mu.Unlock()

		var ctxErr error
		select {
		case <-ch:
		case <-ctx.Done():
			ctxErr = ctx.Err()
		}

		t.mu.Lock()
		t.waiters--
		if ctxErr != nil {
			t.mu.Unlock()
			return ctxErr
		}
	}
}

// Snapshot returns an independent copy of the tracker state.
func (t *Tracker) Snapshot() Snapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	pending := make([]uint64, 0, len(t.pending))
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

// Close shuts the tracker down. It is idempotent. Afterwards Issue and
// Complete fail with ErrClosed, and every Wait still blocked returns
// ErrClosed.
func (t *Tracker) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	t.closed = true
	close(t.broadcast)
}
