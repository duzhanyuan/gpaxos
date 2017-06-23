package util

import (
  "github.com/emirpasic/gods/trees/binaryheap"
  "github.com/lichuang/gpaxos/util"
)

type TimeManager struct {
  NowTimerId uint32
  TimerHeap  *binaryheap.Heap
}

func NewTimerManager() *TimeManager {
  return &TimeManager{
    NowTimerId:1,
    TimerHeap: binaryheap.NewWith(byTimer),
  }
}

func (self *TimeManager) AddTimer(absTime uint64) *Timer {
  return self.AddTimerWithType(absTime, 0)
}

func (self *TimeManager) AddTimerWithType(absTime uint64, timerType int) *Timer {
  timer := NewTimer(self.NowTimerId, absTime, timerType)
  self.NowTimerId++
  self.TimerHeap.Push(timer)

  return timer
}

func (self *TimeManager)GetNextTimeout() int32 {
  if self.TimerHeap.Empty() {
    return -1
  }

  var nextTimeout int32 = 0
  obj, ok := self.TimerHeap.Peek()
  if !ok {
    return -1
  }

  timer := obj.(*Timer)
  nowTime := util.NowTimeMs()
  if timer.AbsTime > nowTime {
    nextTimeout = int32(timer.AbsTime - nowTime)
  }

  return nextTimeout
}

func (self *TimeManager) PopTimeout(timerId *uint32, timerType *int) bool {
  if self.TimerHeap.Empty() {
    return false
  }

  obj, ok := self.TimerHeap.Peek()
  if !ok {
    return false
  }

  timer := obj.(*Timer)
  nowTime := util.
  if timer.AbsTime > nowTime {
    return false
  }

  self.TimerHeap.Pop()

  *timerId = timer.TimerId
  *timerType = timer.TimerType

  return true
}