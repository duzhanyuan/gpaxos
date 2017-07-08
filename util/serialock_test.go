package util_test

import (
  //"github.com/lichuang/gpaxos/util"
  "testing"
  //"sync"
  //"time"
  "fmt"
)

func TestSerialock(t *testing.T) {
  fmt.Printf("hello")
  /*
  var mutex sync.Mutex

  lock := util.NewSeriaLock(&mutex)

  now := util.NowTimeMs()
  end := false

  go func() {
    now = util.NowTimeMs()
    lock.Lock()
    time.Sleep(time.Second * 1)
    lock.Interupt()
    end = true
  }()

  for !end {
    lock.WaitTime(100)
  }

  fmt.Printf("diff: %d", util.NowTimeMs() - now)
  lock.UnLock()
  */
}
