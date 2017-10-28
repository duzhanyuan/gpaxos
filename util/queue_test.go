package util

import (
  "testing"
)

func TestQueueAppend(t *testing.T) {
  queue := NewQueue()
  var value int32 = 100

  go func() {
    queue.Add(value)
  }()

  ret := queue.PeekWithTimeout(5000)

  TestAssert(
    t,
    ret != nil,
    "queue peek nil",
  )

  v := ret.(int32)
  TestAssert(
    t,
    v == value,
    "queue peel error, get %d, expect %d", t, value,
  )
}

func TestPeekEmptyQueue(t *testing.T) {
  queue := NewQueue()

  ret := queue.PeekWithTimeout(1000)

  TestAssert(
    t,
    ret == nil,
    "queue peek not empty",
  )
}
