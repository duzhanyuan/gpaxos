package util

import (
  "container/list"
  "sync"
)

// Deque is a head-tail linked list data structure implementation.
// It is based on a doubly linked list container, so that every
// operations time complexity is O(1).
//
// every operations over an instiated Deque are synchronized and
// safe for concurrent usage.
type Deque struct {
  sync.RWMutex
  container *list.List
  capacity  int
}

// NewDeque creates a Deque.
func NewDeque() *Deque {
  return NewCappedDeque(-1)
}

// NewCappedDeque creates a Deque with the specified capacity limit.
func NewCappedDeque(capacity int) *Deque {
  return &Deque{
    container: list.New(),
    capacity:  capacity,
  }
}

// Append inserts element at the back of the Deque in a O(1) time complexity,
// returning true if successful or false if the deque is at capacity.
func (s *Deque) Append(item interface{}) bool {
  s.Lock()

  if s.capacity < 0 || s.container.Len() < s.capacity {
    s.container.PushBack(item)
    s.Unlock()
    return true
  }

  s.Unlock()
  return false
}

// Prepend inserts element at the Deques front in a O(1) time complexity,
// returning true if successful or false if the deque is at capacity.
func (s *Deque) Prepend(item interface{}) bool {
  s.Lock()

  if s.capacity < 0 || s.container.Len() < s.capacity {
    s.container.PushFront(item)
    s.Unlock()
    return true
  }

  s.Unlock()
  return false
}

// Pop removes the last element of the deque in a O(1) time complexity
func (s *Deque) Pop() interface{} {
  s.Lock()

  var item interface{} = nil
  var lastContainerItem *list.Element = nil

  lastContainerItem = s.container.Back()
  if lastContainerItem != nil {
    item = s.container.Remove(lastContainerItem)
  }

  s.Unlock()
  return item
}

// Shift removes the first element of the deque in a O(1) time complexity
func (s *Deque) Shift() interface{} {
  s.Lock()

  var item interface{} = nil
  var firstContainerItem *list.Element = nil

  firstContainerItem = s.container.Front()
  if firstContainerItem != nil {
    item = s.container.Remove(firstContainerItem)
  }

  s.Unlock()
  return item
}

// First returns the first value stored in the deque in a O(1) time complexity
func (s *Deque) First() interface{} {
  s.RLock()

  item := s.container.Front()
  s.RUnlock()
  if item != nil {
    return item.Value
  } else {
    return nil
  }
}

// Last returns the last value stored in the deque in a O(1) time complexity
func (s *Deque) Last() interface{} {
  s.RLock()

  item := s.container.Back()
  s.RUnlock()
  if item != nil {
    return item.Value
  } else {
    return nil
  }
}

// Size returns the actual deque size
func (s *Deque) Size() int {
  s.RLock()

  len := s.container.Len()
  s.RUnlock()
  return len
}

// Capacity returns the capacity of the deque, or -1 if unlimited
func (s *Deque) Capacity() int {
  s.RLock()
  cap := s.capacity
  s.RUnlock()
  return cap
}

// Empty checks if the deque is empty
func (s *Deque) Empty() bool {
  s.RLock()

  empty := s.container.Len() == 0
  s.RUnlock()
  return empty
}

// Full checks if the deque is full
func (s *Deque) Full() bool {
  s.RLock()

  full := s.capacity >= 0 && s.container.Len() >= s.capacity
  s.RUnlock()
  return full
}
