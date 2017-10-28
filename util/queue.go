package util

import (
  "sync"
)

type Queue struct {
  timeoutCond *TimeoutCond
  mutex       sync.Mutex
  storage     *Deque
}

func NewQueue() *Queue {
  queue := &Queue{}

  queue.timeoutCond = NewTimeoutCond(&queue.mutex)
  queue.storage = NewDeque()

  return queue
}

func (self *Queue) Peek() interface{} {
  for self.Empty() {
    self.timeoutCond.Wait()
  }

  return self.storage.Pop()
}

func (self *Queue) PeekWithTimeout(ms int) interface{} {
  for self.Empty() {
    self.timeoutCond.Lock()
    ret := self.timeoutCond.WaitFor(ms)
    self.timeoutCond.Unlock()
    if !ret {
      return nil
    }
  }

  return self.storage.Pop()
}

func (self *Queue) Pop() {
  self.storage.Pop()
}

func (self *Queue) Add(value interface{}) int {
  self.storage.Append(value)

  self.timeoutCond.Signal()

  return self.storage.Size()
}

func (self *Queue) Size() int {
  return self.storage.Size()
}

func (self *Queue) Signal() {
  self.timeoutCond.Signal()
}

func (self *Queue) Broadcast() {
  self.timeoutCond.Broadcast()
}

func (self *Queue) Lock() {
  self.mutex.Lock()
}

func (self *Queue) Unlock() {
  self.mutex.Unlock()
}

func (self *Queue) Empty() bool {
  return self.storage.Empty()
}
