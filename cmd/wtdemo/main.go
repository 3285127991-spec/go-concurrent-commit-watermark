// Command wtdemo demonstrates out-of-order completion, watermark jumps,
// Wait, and Close using the commitwatermark package.
package main

import (
	"context"
	"fmt"
	"log"

	"commitwatermark"
)

func main() {
	tr := commitwatermark.NewTracker(1)

	var ids []int64
	for i := 0; i < 5; i++ {
		id, err := tr.Issue()
		if err != nil {
			log.Fatal(err)
		}
		ids = append(ids, id)
	}
	fmt.Println("issued:", ids)

	complete := func(id int64) {
		if err := tr.Complete(id); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("complete(%d) -> committed=%d\n", id, tr.Committed())
	}

	// Out-of-order completion: the watermark stalls at 1, then jumps to 3.
	complete(3)
	complete(1)
	complete(2)

	// Wait blocks until the watermark reaches its target.
	done := make(chan error, 1)
	go func() { done <- tr.Wait(context.Background(), 4) }()
	complete(4)
	fmt.Println("wait(4) returned:", <-done)

	// Close wakes waiters that are still blocked.
	blocked := make(chan error, 1)
	go func() { blocked <- tr.Wait(context.Background(), 5) }()
	if err := tr.Close(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("wait(5) after close:", <-blocked)

	fmt.Printf("final snapshot: %+v\n", tr.Snapshot())
}
