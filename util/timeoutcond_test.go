package util

import (
  "fmt"
  "testing"
  "time"
  "sync"
)

func lockAndSignal(t *TimeoutCond) {
  t.Lock.Lock()
  t.Signal()
  t.Lock.Unlock()
}

func waitAndPrint(t *TimeoutCond, i int) {
  t.Lock.Lock()
  fmt.Println("Goroutine", i, "waiting...")
  ok := t.WaitOrTimeout(10 * time.Second)
  t.Lock.Unlock()
  fmt.Println("This is goroutine", i, "ok:", ok)
}

func TestTimeoutCond(test *testing.T) {
  var m sync.Mutex
  t := NewTimeoutCond(&m)

  // Simple wait
  //
  t.Lock.Lock()
  go lockAndSignal(t)
  t.Wait()
  t.Lock.Unlock()
  fmt.Println("Simple wait finished.")

  // Wait that times out
  //
  t.Lock.Lock()
  ok := t.WaitOrTimeout(100 * time.Millisecond)
  t.Lock.Unlock()
  fmt.Println("Timeout wait finished. Timeout:", !ok)

  for i := 0; i < 10; i++ {
    go waitAndPrint(t, i)
  }
  time.Sleep(1 * time.Second)
  t.Lock.Lock()
  fmt.Println("About to signal")
  t.Broadcast()
  t.Lock.Unlock()
  time.Sleep(10 * time.Second)
}