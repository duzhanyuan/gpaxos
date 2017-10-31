package util

import (
  "github.com/emirpasic/gods/trees/binaryheap"
  "sync"
  "container/list"
  "time"
)

type TimerThread struct {
  // save timer id
  nowTimerId uint32

  // save timers in heap, compare by abstime && timer id
  timerHeap *binaryheap.Heap

  // for check timer id existing
  existTimerIdMap map[uint32]bool

  // mutex protect timer exist map
  mapMutex sync.Mutex

  // mutex protect timer lists
  mutex sync.Mutex

  // new added timers saved in newTimerList
  newTimerList *list.List

  // being added into head saved in currentTimerList
  currentTimerList *list.List

  // timer channel notifier
  newTimerChan chan bool

  // thread end flag
  end bool

  // now time (in ms)
  now uint64
}

type TimerObj interface {
  OnTimeout(timer *Timer)
}

type Timer struct {
  Id        uint32
  Obj       TimerObj
  AbsTime   uint64
  TimerType int
}

func NewTimerThread() *TimerThread {
  timerThread := &TimerThread{
    nowTimerId: 1,
    timerHeap:binaryheap.NewWith(byTimer),
    existTimerIdMap:make(map[uint32]bool,0),
    newTimerList: list.New(),
    currentTimerList: list.New(),
    newTimerChan:make(chan bool,1),
    end: false,
  }

  start := make(chan bool, 1)
  go timerThread.main(start)
  <- start

  return timerThread
}

func (self *TimerThread) Stop() {
  self.end = true
}

func (self *TimerThread) main(start chan bool) {
  start <- true

  for !self.end {
    self.now = NowTimeMs()

    nextTimeout := self.getNextTimeout()
    var notifyTimeout int32 = 1

    if nextTimeout > 0 {
     if nextTimeout <= self.now {
       self.dealWithTimeout(nextTimeout)
     } else {
       notifyTimeout = int32(self.now - nextTimeout)
     }
    }

    if (self.waitAddTimerNotify(notifyTimeout)) {
      self.doAddNewTimer()
    }
  }
}

func (self *TimerThread) waitAddTimerNotify(ms int32) bool {
  // wait channel notify
  timer := time.NewTimer(time.Duration(ms) * time.Millisecond)
  ret := false
  select {
  case <- timer.C:
    ret = false
  case <- self.newTimerChan:
    ret = true
  }

  if !timer.Stop() {
    select {
    // otherwise should wait timer
    case <- timer.C:
    default:
    }
  }

  return ret
}

func (self *TimerThread) fireTimeout(timer *Timer) {
  id := timer.Id
  self.mapMutex.Lock()
  _, ok := self.existTimerIdMap[id]
  self.mapMutex.Unlock()

  if ok {
    timer.Obj.OnTimeout(timer)
  }
}

func (self *TimerThread) dealWithTimeout(absTime uint64) {
  for {
    if self.timerHeap.Empty() {
      return
    }

    obj, ok := self.timerHeap.Peek()
    if !ok {
      return
    }

    timer := obj.(*Timer)
    if timer.AbsTime > absTime {
      return
    }

    self.timerHeap.Pop()

    self.fireTimeout(timer)
  }
}

func (self *TimerThread) doAddNewTimer() {
  // simple exchange the timer list pointer
  self.mutex.Lock()
  tmp := self.currentTimerList
  self.currentTimerList = self.newTimerList
  self.newTimerList = tmp
  self.mutex.Unlock()

  len := self.currentTimerList.Len()
  for i:=0; i < len;i++ {
    obj := self.currentTimerList.Front()
    self.currentTimerList.Remove(obj)

    timer := obj.Value.(*Timer)

    // is it already timeout?
    if timer.AbsTime <= self.now {
      // fire timeout event directly
      self.fireTimeout(timer)
    } else {
      // push to timer heap
      self.timerHeap.Push(timer)
    }
  }
}

func (self *TimerThread) getNextTimeout() uint64 {
  if self.timerHeap.Empty() {
    return 0
  }

  obj, ok := self.timerHeap.Peek()
  if !ok {
    return 0
  }

  timer := obj.(*Timer)

  return timer.AbsTime
}

func (self *TimerThread) AddTimer(absTime uint64, timeType int, obj TimerObj) uint32 {
  self.mutex.Lock()
  timer := newTimer(self.nowTimerId, absTime, timeType, obj)
  timerId := self.nowTimerId
  self.nowTimerId += 1

  self.newTimerList.PushBack(timer)
  // notify channel only once
  if len(self.newTimerChan) == 0 {
    self.newTimerChan <- true
  }

  // add into exist timer map
  self.mapMutex.Lock()
  self.existTimerIdMap[timerId] = true
  self.mapMutex.Unlock()

  self.mutex.Unlock()

  return timerId
}

func (self *TimerThread) DelTimer(timerId uint32) {
  self.mapMutex.Lock()
  delete(self.existTimerIdMap, timerId)
  self.mapMutex.Unlock()
}

func newTimer(timerId uint32, absTime uint64, timeType int, obj TimerObj) *Timer {
  return &Timer{
    Id:        timerId,
    AbsTime:   absTime,
    Obj:       obj,
    TimerType: timeType,
  }
}

func (self *Timer) LT(other *Timer) bool {
  if self.AbsTime == other.AbsTime {
    return self.Id < other.Id
  }

  return self.AbsTime < other.AbsTime
}

// comparator used by timer thread heap
func byTimer(a, b interface{}) int {
  timer1 := a.(*Timer)
  timer2 := b.(*Timer)

  ret := timer1.LT(timer2)
  if ret {
    return -1
  }

  return 1
}