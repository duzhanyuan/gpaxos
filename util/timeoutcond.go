package util

// from: https://stackoverflow.com/questions/29923666/waiting-on-a-sync-cond-with-a-timeout

import (
  "sync"
  "time"
)

type TimeoutCond struct {
  locker sync.Locker
  channel chan bool
}

func NewTimeoutCond(l sync.Locker) *TimeoutCond {
  return &TimeoutCond{
    channel: make(chan bool),
    locker:  l,
  }
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

// false if timeout, else return true
func (t *TimeoutCond) WaitFor(ms int) bool {
  timer := time.NewTimer(time.Duration(ms) * time.Millisecond)
  t.locker.Unlock()
  var r bool
  select {
  case <- timer.C:
    r = false
  case <-t.channel:
    r = true
  }
  if !timer.Stop() {
    select {
    case <- timer.C:
    default:
    }
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