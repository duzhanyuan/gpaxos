package util

// from: https://stackoverflow.com/questions/29923666/waiting-on-a-sync-cond-with-a-timeout

import (
  "sync"
  "time"
)

type TimeoutCond struct {
  Locker sync.Locker
  ch     chan bool
}

func NewTimeoutCond(l sync.Locker) *TimeoutCond {
  return &TimeoutCond{
    ch:     make(chan bool),
    Locker: l,
  }
}

func (t *TimeoutCond) Lock() {
  t.Locker.Lock()
}

func (t *TimeoutCond) Unlock() {
  t.Locker.Unlock()
}

func (t *TimeoutCond) Wait() {
  t.Locker.Unlock()
  <-t.ch
  t.Locker.Lock()
}

func (t *TimeoutCond) WaitOrTimeout(d time.Duration) bool {
  tmo := time.NewTimer(d)
  t.Locker.Unlock()
  var r bool
  select {
  case <-tmo.C:
    r = false
  case <-t.ch:
    r = true
  }
  if !tmo.Stop() {
    select {
    case <-tmo.C:
    default:
    }
  }
  t.Locker.Lock()
  return r
}

func (t *TimeoutCond) Signal() {
  t.signal()
}

func (t *TimeoutCond) Broadcast() {
  for {
    // Stop when we run out of waiters
    //
    if !t.signal() {
      return
    }
  }
}

func (t *TimeoutCond) signal() bool {
  select {
  case t.ch <- true:
    return true
  default:
    return false
  }
}
