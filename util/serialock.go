package util

import (
  "sync"
  "time"
)

type SeriaLock struct {
  mutex *sync.Mutex
  channel chan bool
}

func NewSeriaLock(mutex *sync.Mutex) *SeriaLock {
  return &SeriaLock{
    mutex:mutex,
    channel:make(chan bool),
  }
}

func (self *SeriaLock) Lock() {
  self.mutex.Lock()
}

func (self *SeriaLock) UnLock() {
  self.mutex.Unlock()
}

func (self *SeriaLock) Wait() {
  <- self.channel
  self.mutex.Lock()
}

func (self *SeriaLock) Interupt() {
  self.channel <- true
}

func (self *SeriaLock) WaitTime(timeout uint64) bool {
  select {
    case <- self.channel:
      self.mutex.Lock()
      return true
    case time.After(time.Microsecond * time.Duration(timeout)):
      return false
  }

  return false
}