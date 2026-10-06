package commitwatermark_test

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"sync"
	"testing"

	"commitwatermark"
)

// waitForWaiters spins (without sleeping) until n Wait calls are blocked.
func waitForWaiters(t *testing.T, tr *commitwatermark.Tracker, n int) {
	t.Helper()
	for i := 0; i < 100000; i++ {
		if tr.Snapshot().Waiters == n {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("waiter count never reached %d", n)
}

func mustIssue(t *testing.T, tr *commitwatermark.Tracker, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := tr.Issue(); err != nil {
			t.Fatalf("Issue: %v", err)
		}
	}
}

func mustComplete(t *testing.T, tr *commitwatermark.Tracker, id int64) {
	t.Helper()
	if err := tr.Complete(id); err != nil {
		t.Fatalf("Complete(%d): %v", id, err)
	}
}

func TestIssueStrictlyIncreasing(t *testing.T) {
	tr := commitwatermark.NewTracker(7)
	defer tr.Close()

	seen := make(map[int64]bool)
	for want := int64(7); want < 12; want++ {
		id, err := tr.Issue()
		if err != nil {
			t.Fatalf("Issue: %v", err)
		}
		if id != want {
			t.Fatalf("Issue = %d, want %d", id, want)
		}
		if seen[id] {
			t.Fatalf("Issue returned duplicate id %d", id)
		}
		seen[id] = true
	}
	if got := tr.Committed(); got != 6 {
		t.Fatalf("Committed = %d, want 6 (start-1)", got)
	}
}

func TestCompleteInOrder(t *testing.T) {
	tr := commitwatermark.NewTracker(1)
	defer tr.Close()
	mustIssue(t, tr, 3)

	for id := int64(1); id <= 3; id++ {
		mustComplete(t, tr, id)
		if got := tr.Committed(); got != id {
			t.Fatalf("after Complete(%d): Committed = %d, want %d", id, got, id)
		}
	}
	if got := tr.Snapshot().Pending; len(got) != 0 {
		t.Fatalf("Pending = %v, want empty", got)
	}
}

func TestCompleteOutOfOrderJump(t *testing.T) {
	tr := commitwatermark.NewTracker(1)
	defer tr.Close()
	mustIssue(t, tr, 3)

	mustComplete(t, tr, 1)
	mustComplete(t, tr, 3)
	if got := tr.Committed(); got != 1 {
		t.Fatalf("Committed = %d, want 1 (2 is missing)", got)
	}
	mustComplete(t, tr, 2)
	if got := tr.Committed(); got != 3 {
		t.Fatalf("Committed = %d, want 3 (jump over pending 3)", got)
	}
}

func TestCompleteErrors(t *testing.T) {
	tr := commitwatermark.NewTracker(1)
	defer tr.Close()
	mustIssue(t, tr, 3)

	mustComplete(t, tr, 2)
	if err := tr.Complete(2); !errors.Is(err, commitwatermark.ErrAlreadyCompleted) {
		t.Fatalf("duplicate Complete(2) = %v, want ErrAlreadyCompleted", err)
	}
	mustComplete(t, tr, 1) // commits 1 and 2
	if err := tr.Complete(1); !errors.Is(err, commitwatermark.ErrAlreadyCompleted) {
		t.Fatalf("Complete(1) after commit = %v, want ErrAlreadyCompleted", err)
	}
	if err := tr.Complete(4); !errors.Is(err, commitwatermark.ErrNotIssued) {
		t.Fatalf("Complete(4) unissued = %v, want ErrNotIssued", err)
	}
	if err := tr.Complete(0); !errors.Is(err, commitwatermark.ErrNotIssued) {
		t.Fatalf("Complete(0) below start = %v, want ErrNotIssued", err)
	}
}

func TestWaitWakesOnAdvance(t *testing.T) {
	tr := commitwatermark.NewTracker(1)
	defer tr.Close()
	mustIssue(t, tr, 3)

	done := make(chan error, 1)
	go func() { done <- tr.Wait(context.Background(), 3) }()
	waitForWaiters(t, tr, 1)

	mustComplete(t, tr, 1)
	mustComplete(t, tr, 3)
	select {
	case err := <-done:
		t.Fatalf("Wait returned early with %v (watermark still 1)", err)
	default:
	}

	mustComplete(t, tr, 2) // watermark jumps to 3
	if err := <-done; err != nil {
		t.Fatalf("Wait = %v, want nil", err)
	}
	if got := tr.Snapshot().Waiters; got != 0 {
		t.Fatalf("Waiters = %d, want 0 after wake", got)
	}
}

func TestWaitContextCancelAffectsOnlySelf(t *testing.T) {
	tr := commitwatermark.NewTracker(1)
	defer tr.Close()
	mustIssue(t, tr, 2)

	ctx, cancel := context.WithCancel(context.Background())
	cancelled := make(chan error, 1)
	other := make(chan error, 1)
	go func() { cancelled <- tr.Wait(ctx, 1) }()
	go func() { other <- tr.Wait(context.Background(), 2) }()
	waitForWaiters(t, tr, 2)

	cancel()
	if err := <-cancelled; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Wait = %v, want context.Canceled", err)
	}
	select {
	case err := <-other:
		t.Fatalf("cancellation leaked to other waiter: %v", err)
	default:
	}

	mustComplete(t, tr, 1)
	mustComplete(t, tr, 2)
	if err := <-other; err != nil {
		t.Fatalf("other Wait = %v, want nil", err)
	}
}

func TestWaitMultipleWaiters(t *testing.T) {
	tr := commitwatermark.NewTracker(1)
	defer tr.Close()
	mustIssue(t, tr, 3)

	results := make([]chan error, 3)
	for i, target := range []int64{1, 2, 3} {
		ch := make(chan error, 1)
		results[i] = ch
		go func() { ch <- tr.Wait(context.Background(), target) }()
	}
	waitForWaiters(t, tr, 3)

	mustComplete(t, tr, 3) // out of order: nobody may wake
	for i, ch := range results {
		select {
		case err := <-ch:
			t.Fatalf("waiter %d woke early: %v", i, err)
		default:
		}
	}

	mustComplete(t, tr, 1)
	if err := <-results[0]; err != nil {
		t.Fatalf("waiter for 1 = %v, want nil", err)
	}
	mustComplete(t, tr, 2) // watermark jumps to 3, waking the rest
	for _, ch := range results[1:] {
		if err := <-ch; err != nil {
			t.Fatalf("waiter = %v, want nil", err)
		}
	}
	if got := tr.Snapshot().Waiters; got != 0 {
		t.Fatalf("Waiters = %d, want 0", got)
	}
}

func TestSnapshot(t *testing.T) {
	tr := commitwatermark.NewTracker(1)
	defer tr.Close()
	mustIssue(t, tr, 4)
	mustComplete(t, tr, 2)
	mustComplete(t, tr, 4)

	waiter := make(chan error, 1)
	go func() { waiter <- tr.Wait(context.Background(), 3) }()
	waitForWaiters(t, tr, 1)

	snap := tr.Snapshot()
	if snap.Committed != 0 || snap.LastIssued != 4 || snap.Waiters != 1 {
		t.Fatalf("Snapshot = %+v, want Committed=0 LastIssued=4 Waiters=1", snap)
	}
	if !slices.Equal(snap.Pending, []int64{2, 4}) {
		t.Fatalf("Pending = %v, want [2 4]", snap.Pending)
	}

	// Mutating the snapshot must not affect the tracker.
	snap.Pending[0] = 99
	if got := tr.Snapshot().Pending; !slices.Equal(got, []int64{2, 4}) {
		t.Fatalf("after mutation Pending = %v, want [2 4]", got)
	}

	mustComplete(t, tr, 1)
	mustComplete(t, tr, 3) // commits everything through 4
	if err := <-waiter; err != nil {
		t.Fatalf("Wait = %v, want nil", err)
	}
	snap = tr.Snapshot()
	if snap.Committed != 4 || len(snap.Pending) != 0 || snap.Waiters != 0 {
		t.Fatalf("final Snapshot = %+v, want Committed=4 no pending/waiters", snap)
	}
}

func TestClose(t *testing.T) {
	tr := commitwatermark.NewTracker(1)
	mustIssue(t, tr, 2)
	mustComplete(t, tr, 1)

	blocked := make(chan error, 1)
	go func() { blocked <- tr.Wait(context.Background(), 2) }()
	waitForWaiters(t, tr, 1)

	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := <-blocked; !errors.Is(err, commitwatermark.ErrClosed) {
		t.Fatalf("blocked Wait after Close = %v, want ErrClosed", err)
	}
	if _, err := tr.Issue(); !errors.Is(err, commitwatermark.ErrClosed) {
		t.Fatalf("Issue after Close = %v, want ErrClosed", err)
	}
	if err := tr.Complete(2); !errors.Is(err, commitwatermark.ErrClosed) {
		t.Fatalf("Complete after Close = %v, want ErrClosed", err)
	}
	if err := tr.Wait(context.Background(), 2); !errors.Is(err, commitwatermark.ErrClosed) {
		t.Fatalf("Wait for pending id after Close = %v, want ErrClosed", err)
	}
	if err := tr.Wait(context.Background(), 1); err != nil {
		t.Fatalf("Wait for committed id after Close = %v, want nil", err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
}

func TestConcurrentIssueCompleteWait(t *testing.T) {
	tr := commitwatermark.NewTracker(1)
	defer tr.Close()

	const n = 200
	ids := make(chan int64, n)

	var issuers sync.WaitGroup
	for i := 0; i < 4; i++ {
		issuers.Add(1)
		go func() {
			defer issuers.Done()
			for j := 0; j < n/4; j++ {
				id, err := tr.Issue()
				if err != nil {
					t.Errorf("Issue: %v", err)
					return
				}
				ids <- id
			}
		}()
	}
	go func() { issuers.Wait(); close(ids) }()

	waiter := make(chan error, 1)
	go func() { waiter <- tr.Wait(context.Background(), n) }()

	var completers sync.WaitGroup
	for i := 0; i < 4; i++ {
		completers.Add(1)
		go func() {
			defer completers.Done()
			for id := range ids {
				if err := tr.Complete(id); err != nil {
					t.Errorf("Complete(%d): %v", id, err)
				}
			}
		}()
	}
	completers.Wait()

	if err := <-waiter; err != nil {
		t.Fatalf("Wait(%d) = %v, want nil", n, err)
	}
	if got := tr.Committed(); got != n {
		t.Fatalf("Committed = %d, want %d", got, n)
	}
	snap := tr.Snapshot()
	if snap.LastIssued != n || len(snap.Pending) != 0 {
		t.Fatalf("Snapshot = %+v, want LastIssued=%d and no pending", snap, n)
	}
}
