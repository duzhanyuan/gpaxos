package util

import (
	"testing"
	"time"
	"sync"
	"fmt"
	"runtime"
)

func TestWaitlock(t *testing.T) {
	runtime.GOMAXPROCS(4)

	wl := NewWaitlock()

	fmt.Printf("begin test TestWaitlock....\n")
	waitChan := make(chan bool, 1)
	var waitGroup sync.WaitGroup
	waitGroup.Add(3)

	go func() {
		_, err :=wl.Lock(1000)
		TestAssert(t,
			err == nil,
			"wait lock should success")
		waitChan <- true
		// sleep 1 sec to force routine 2 timeout
		time.Sleep(1 * time.Second)
		wl.Unlock()
		waitGroup.Done()
	}()

	go func() {
		<- waitChan
		_, err := wl.Lock(100)
		TestAssert(t,
			err == Waitlock_Timeout,
			"wait lock should timeout")
		waitGroup.Done()
	}()

	go func() {
		time.Sleep(2 * time.Second)
		_, err := wl.Lock(3000)
		TestAssert(t,
			err == nil,
			"wait lock should not timeout")
		waitGroup.Done()
	}()

	waitGroup.Wait()
	fmt.Printf("passed ....\n")
}

func TestManyWaitlock(t *testing.T) {
	runtime.GOMAXPROCS(4)

	wl := NewWaitlock()

	n := 100
	fmt.Printf("begin test TestManyWaitlock....\n")
	var waitGroup sync.WaitGroup

	waitGroup.Add(n)

	go func() {
		_, err :=wl.Lock(1000)
		TestAssert(t,
			err == nil,
			"wait lock should success")
		wl.Unlock()
	}()

	for n > 0 {
		n--
		go func() {
			// sleep for routine get the lock
			time.Sleep(1 * time.Second)
			_, err :=wl.Lock(2000)
			TestAssert(t,
				err == nil,
				"wait lock should success")
			wl.Unlock()
			waitGroup.Done()
		}()
	}

	waitGroup.Wait()
}