package util

// from: https://stackoverflow.com/questions/29923666/waiting-on-a-sync-cond-with-a-timeout

import (
  "sync"
  "time"
)

type TimeoutCond struct {
  locker sync.Locker
  mutex sync.Mutex
  channel chan bool
}

func NewTimeoutCondWithMutex(l sync.Locker) *TimeoutCond {
  return &TimeoutCond{
    channel: make(chan bool),
    locker:  l,
  }
}

func NewTimeoutCond() *TimeoutCond {
  cond := &TimeoutCond{
    channel: make(chan bool),
  }
  cond.locker = cond.mutex
  return cond
}

func (t *TimeoutCond) Lock() {
  t.locker.Lock()
}

func (t *TimeoutCond) Unlock() {
  t.locker.Unlock()
}

func (t *TimeoutCond) Wait() {
  t.locker.Unlock()
  <-t.channel
  t.locker.Lock()
}

func (t *TimeoutCond) WaitFor(ms int) bool {
  t.locker.Unlock()
  var r bool
  select {
  case <- time.After(time.Millisecond * time.Duration(ms)):
    r = false
  case <-t.channel:
    r = true
  }
  t.locker.Lock()
  return r
}

func (t *TimeoutCond) Signal() {
  t.signal()
}

func (t *TimeoutCond) Broadcast() {
  for {
    // Stop when we run out of waiters
    if !t.signal() {
      return
    }
  }
}

func (t *TimeoutCond) signal() bool {
  select {
  case t.channel <- true:
    return true
  default:
    return false
  }
}
