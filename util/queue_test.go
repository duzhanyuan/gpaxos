package util

import (
  "testing"
)

func TestQequeAppend(t *testing.T) {
  queue := NewQueue()
  var value int32 = 100

  go func() {
    queue.Add(value, true)
  }()

  ret := queue.PeekWithTimeout(5000)

  assert(
    t,
    ret != nil,
    "queue peek nil",
  )

  v := ret.(int32)
  assert(
    t,
    v == value,
    "queue peel error, get %d, expect %d", t, value,
  )
}

func TestPeekEmptyQueue(t *testing.T) {
  queue := NewQueue()

  ret := queue.PeekWithTimeout(1000)

  assert(
    t,
    ret == nil,
    "queue peek not empty",
  )
}
