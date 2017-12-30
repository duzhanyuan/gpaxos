package util

import (
	"testing"
	"sync"
)

func testFunc(waitGroup *sync.WaitGroup) {
	waitGroup.Done()
}

func TestRoutine(t *testing.T) {
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)

	StartRoutine(func() {
		testFunc(&waitGroup)
	})

	waitGroup.Wait()
}
