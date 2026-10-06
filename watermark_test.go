package commitwatermark_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"

	"commitwatermark"
)

func newTracker(t *testing.T, start uint64) *commitwatermark.Tracker {
	t.Helper()
	tr, err := commitwatermark.New(start)
	if err != nil {
		t.Fatalf("New(%d): %v", start, err)
	}
	return tr
}

func issueN(t *testing.T, tr *commitwatermark.Tracker, n int) []uint64 {
	t.Helper()
	ids := make([]uint64, 0, n)
	for i := 0; i < n; i++ {
		id, err := tr.Issue()
		if err != nil {
			t.Fatalf("Issue: %v", err)
		}
		ids = append(ids, id)
	}
	return ids
}

func mustComplete(t *testing.T, tr *commitwatermark.Tracker, id uint64) {
	t.Helper()
	if err := tr.Complete(id); err != nil {
		t.Fatalf("Complete(%d): %v", id, err)
	}
}

// waitForWaiters spins until at least want goroutines are blocked in Wait.
func waitForWaiters(t *testing.T, tr *commitwatermark.Tracker, want int) {
	t.Helper()
	for i := 0; i < 1_000_000; i++ {
		if tr.Snapshot().Waiters >= want {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("waiter count never reached %d", want)
}

func waitForNoWaiters(t *testing.T, tr *commitwatermark.Tracker) {
	t.Helper()
	for i := 0; i < 1_000_000; i++ {
		if tr.Snapshot().Waiters == 0 {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("waiters never drained")
}

func TestSequentialCompletion(t *testing.T) {
	if _, err := commitwatermark.New(0); err == nil {
		t.Fatal("New(0) should fail")
	}
	tr := newTracker(t, 1)
	if got := tr.Committed(); got != 0 {
		t.Fatalf("initial committed = %d, want 0", got)
	}
	for _, id := range issueN(t, tr, 3) {
		mustComplete(t, tr, id)
		if got := tr.Committed(); got != id {
			t.Fatalf("after Complete(%d): committed = %d", id, got)
		}
	}
}

func TestOutOfOrderCompletion(t *testing.T) {
	tr := newTracker(t, 1)
	issueN(t, tr, 4)

	mustComplete(t, tr, 1)
	mustComplete(t, tr, 3)
	mustComplete(t, tr, 4)
	if got := tr.Committed(); got != 1 {
		t.Fatalf("committed = %d, want 1 (gap at 2)", got)
	}
	mustComplete(t, tr, 2)
	if got := tr.Committed(); got != 4 {
		t.Fatalf("committed = %d, want 4 after filling the gap", got)
	}
	if pending := tr.Snapshot().Pending; len(pending) != 0 {
		t.Fatalf("pending = %v, want empty", pending)
	}
}

func TestCompleteInvalidIDs(t *testing.T) {
	tr := newTracker(t, 5)
	issueN(t, tr, 3) // issued: 5, 6, 7

	for _, id := range []uint64{0, 4, 8, 100} {
		if err := tr.Complete(id); !errors.Is(err, commitwatermark.ErrNotIssued) {
			t.Fatalf("Complete(%d) = %v, want ErrNotIssued", id, err)
		}
	}
	mustComplete(t, tr, 7) // pending, watermark stays at 4
	if err := tr.Complete(7); !errors.Is(err, commitwatermark.ErrAlreadyCompleted) {
		t.Fatalf("duplicate Complete(7) = %v, want ErrAlreadyCompleted", err)
	}
	mustComplete(t, tr, 5)
	mustComplete(t, tr, 6) // watermark jumps to 7
	if err := tr.Complete(6); !errors.Is(err, commitwatermark.ErrAlreadyCompleted) {
		t.Fatalf("re-Complete(6) = %v, want ErrAlreadyCompleted", err)
	}
}

func TestIssueStrictlyIncreasing(t *testing.T) {
	tr := newTracker(t, 42)
	seen := make(map[uint64]bool)
	prev := uint64(41)
	for i := 0; i < 100; i++ {
		id, err := tr.Issue()
		if err != nil {
			t.Fatalf("Issue: %v", err)
		}
		if id <= prev {
			t.Fatalf("Issue returned %d after %d, not strictly increasing", id, prev)
		}
		if seen[id] {
			t.Fatalf("Issue returned duplicate id %d", id)
		}
		seen[id] = true
		prev = id
	}
	if got := tr.Snapshot().LastIssued; got != 141 {
		t.Fatalf("LastIssued = %d, want 141", got)
	}
}

func TestWaitWakesOnCommit(t *testing.T) {
	tr := newTracker(t, 1)
	issueN(t, tr, 3)

	done := make(chan error, 1)
	go func() { done <- tr.Wait(context.Background(), 3) }()
	waitForWaiters(t, tr, 1)

	mustComplete(t, tr, 1)
	mustComplete(t, tr, 2)
	select {
	case err := <-done:
		t.Fatalf("Wait(3) returned %v before the watermark reached 3", err)
	default:
	}

	mustComplete(t, tr, 3)
	if err := <-done; err != nil {
		t.Fatalf("Wait(3) = %v, want nil", err)
	}
	waitForNoWaiters(t, tr)
}

func TestWaitContextCancel(t *testing.T) {
	tr := newTracker(t, 1)
	issueN(t, tr, 2)

	ctx, cancel := context.WithCancel(context.Background())
	cancelled := make(chan error, 1)
	survivor := make(chan error, 1)
	go func() { cancelled <- tr.Wait(ctx, 2) }()
	go func() { survivor <- tr.Wait(context.Background(), 2) }()
	waitForWaiters(t, tr, 2)

	cancel()
	if err := <-cancelled; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Wait = %v, want context.Canceled", err)
	}
	// Cancelling one waiter must not affect the other.
	select {
	case err := <-survivor:
		t.Fatalf("uncancelled Wait returned %v", err)
	default:
	}

	mustComplete(t, tr, 1)
	mustComplete(t, tr, 2)
	if err := <-survivor; err != nil {
		t.Fatalf("surviving Wait = %v, want nil", err)
	}
	waitForNoWaiters(t, tr)
}

func TestMultipleWaiters(t *testing.T) {
	tr := newTracker(t, 1)
	issueN(t, tr, 5)

	targets := []uint64{2, 3, 5, 5, 4}
	results := make(chan error, len(targets))
	for _, id := range targets {
		go func(id uint64) { results <- tr.Wait(context.Background(), id) }(id)
	}
	waitForWaiters(t, tr, len(targets))

	for _, id := range []uint64{2, 4, 1, 5, 3} { // shuffled completion order
		mustComplete(t, tr, id)
	}
	for range targets {
		if err := <-results; err != nil {
			t.Fatalf("Wait = %v, want nil", err)
		}
	}
	if got := tr.Committed(); got != 5 {
		t.Fatalf("committed = %d, want 5", got)
	}
	waitForNoWaiters(t, tr)
}

func TestSnapshot(t *testing.T) {
	tr := newTracker(t, 1)
	issueN(t, tr, 5)
	mustComplete(t, tr, 1)
	mustComplete(t, tr, 3)
	mustComplete(t, tr, 4)

	snap := tr.Snapshot()
	if snap.Committed != 1 || snap.LastIssued != 5 || snap.Waiters != 0 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
	if len(snap.Pending) != 2 || snap.Pending[0] != 3 || snap.Pending[1] != 4 {
		t.Fatalf("pending = %v, want [3 4]", snap.Pending)
	}

	// The snapshot must be an independent copy.
	snap.Pending[0] = 99
	if got := tr.Snapshot().Pending[0]; got != 3 {
		t.Fatalf("mutating the snapshot changed tracker state, pending[0] = %d", got)
	}

	done := make(chan error, 1)
	go func() { done <- tr.Wait(context.Background(), 5) }()
	waitForWaiters(t, tr, 1)
	if got := tr.Snapshot().Waiters; got != 1 {
		t.Fatalf("waiters = %d, want 1", got)
	}
	mustComplete(t, tr, 2)
	mustComplete(t, tr, 5)
	if err := <-done; err != nil {
		t.Fatalf("Wait(5) = %v, want nil", err)
	}
	waitForNoWaiters(t, tr)
	if got := tr.Committed(); got != 5 {
		t.Fatalf("committed = %d, want 5", got)
	}
}

func TestClose(t *testing.T) {
	tr := newTracker(t, 1)
	issueN(t, tr, 3)
	mustComplete(t, tr, 1)

	waitErrs := make(chan error, 2)
	go func() { waitErrs <- tr.Wait(context.Background(), 2) }()
	go func() { waitErrs <- tr.Wait(context.Background(), 3) }()
	waitForWaiters(t, tr, 2)

	tr.Close()
	for i := 0; i < 2; i++ {
		if err := <-waitErrs; !errors.Is(err, commitwatermark.ErrClosed) {
			t.Fatalf("blocked Wait after Close = %v, want ErrClosed", err)
		}
	}
	waitForNoWaiters(t, tr)

	if _, err := tr.Issue(); !errors.Is(err, commitwatermark.ErrClosed) {
		t.Fatalf("Issue after Close = %v, want ErrClosed", err)
	}
	if err := tr.Complete(2); !errors.Is(err, commitwatermark.ErrClosed) {
		t.Fatalf("Complete after Close = %v, want ErrClosed", err)
	}
	if err := tr.Wait(context.Background(), 2); !errors.Is(err, commitwatermark.ErrClosed) {
		t.Fatalf("Wait(2) after Close = %v, want ErrClosed", err)
	}
	if err := tr.Wait(context.Background(), 1); err != nil {
		t.Fatalf("Wait(1) already committed after Close = %v, want nil", err)
	}
	tr.Close() // idempotent, must not panic
}

func TestConcurrentIssueComplete(t *testing.T) {
	tr := newTracker(t, 1)
	const n = 200

	ids := make(chan uint64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := tr.Issue()
			if err != nil {
				t.Errorf("Issue: %v", err)
				return
			}
			ids <- id
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[uint64]bool, n)
	all := make([]uint64, 0, n)
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate issued id %d", id)
		}
		seen[id] = true
		all = append(all, id)
	}
	if len(all) != n {
		t.Fatalf("issued %d ids, want %d", len(all), n)
	}

	waitDone := make(chan error, 1)
	go func() { waitDone <- tr.Wait(context.Background(), n) }()

	for _, id := range all {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			if err := tr.Complete(id); err != nil {
				t.Errorf("Complete(%d): %v", id, err)
			}
		}(id)
	}
	wg.Wait()

	if err := <-waitDone; err != nil {
		t.Fatalf("Wait(%d) = %v", n, err)
	}
	if got := tr.Committed(); got != n {
		t.Fatalf("committed = %d, want %d", got, n)
	}
	snap := tr.Snapshot()
	if snap.LastIssued != n || len(snap.Pending) != 0 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
}
