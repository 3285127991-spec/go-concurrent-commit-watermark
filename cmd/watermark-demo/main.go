// Command watermark-demo demonstrates out-of-order completion, watermark
// jumps, Wait, and Close using the commitwatermark package.
package main

import (
	"context"
	"fmt"
	"log"
	"runtime"

	"commitwatermark"
)

func main() {
	tr, err := commitwatermark.New(1)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("== issue 5 ids ==")
	for i := 0; i < 5; i++ {
		id, err := tr.Issue()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  issue -> %d\n", id)
	}

	fmt.Println("\n== out-of-order completion ==")
	for _, id := range []uint64{3, 1, 2} {
		mustComplete(tr, id)
		fmt.Printf("  complete(%d) -> committed = %d\n", id, tr.Committed())
	}

	fmt.Println("\n== wait for 5 in a goroutine ==")
	waitDone := make(chan error, 1)
	go func() { waitDone <- tr.Wait(context.Background(), 5) }()
	waitForWaiters(tr, 1)
	for _, id := range []uint64{4, 5} {
		mustComplete(tr, id)
		fmt.Printf("  complete(%d) -> committed = %d\n", id, tr.Committed())
	}
	if err := <-waitDone; err != nil {
		log.Fatal(err)
	}
	fmt.Println("  Wait(5) returned: watermark reached 5")

	fmt.Println("\n== close with a blocked waiter ==")
	blocked := make(chan error, 1)
	go func() { blocked <- tr.Wait(context.Background(), 42) }()
	waitForWaiters(tr, 1)
	tr.Close()
	fmt.Printf("  Wait(42) after Close   -> %v\n", <-blocked)
	if _, err := tr.Issue(); err != nil {
		fmt.Printf("  Issue after Close      -> %v\n", err)
	}
	if err := tr.Complete(4); err != nil {
		fmt.Printf("  Complete(4) after Close -> %v\n", err)
	}

	fmt.Println("\ndemo done")
}

func mustComplete(tr *commitwatermark.Tracker, id uint64) {
	if err := tr.Complete(id); err != nil {
		log.Fatal(err)
	}
}

func waitForWaiters(tr *commitwatermark.Tracker, want int) {
	for tr.Snapshot().Waiters < want {
		runtime.Gosched()
	}
}
