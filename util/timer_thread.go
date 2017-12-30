package util

import (
  "sync"
  "container/list"
  "time"

  log "github.com/lichuang/log4go"
)

type TimerThread struct {
  // save timer id
  nowTimerId uint32

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
    existTimerIdMap:make(map[uint32]bool,0),
    newTimerList: list.New(),
    currentTimerList: list.New(),
    end: false,
    now: NowTimeMs(),
  }

  StartRoutine(func() {
  	timerThread.main()
	})
  return timerThread
}

func (self *TimerThread) Stop() {
  self.end = true
}

func (self *TimerThread) main() {
  for !self.end {
    // fire every 1 ms
    timerChan := time.NewTimer(1 * time.Millisecond).C
    <- timerChan
    self.now = NowTimeMs()

again:
    // iterator current timer list
    len := self.currentTimerList.Len()
    for i:=0; i < len;i++ {
      obj := self.currentTimerList.Front()
      timer := obj.Value.(*Timer)
      if timer.AbsTime > self.now {
        break
      }

      self.currentTimerList.Remove(obj)
      self.fireTimeout(timer)
    }

    // if current timer list is empty, then exchange two list
    if self.currentTimerList.Len() == 0 {
      self.mutex.Lock()
      tmp := self.currentTimerList
      self.currentTimerList = self.newTimerList
      self.newTimerList = tmp
      self.mutex.Unlock()
      // check timeout agant
      goto again
    }
  }
}

func (self *TimerThread) fireTimeout(timer *Timer) {
  id := timer.Id
  self.mapMutex.Lock()
  _, ok := self.existTimerIdMap[id]
  self.mapMutex.Unlock()

  if ok {
    log.Debug("fire timeout:%v, %d", timer.Obj, timer.TimerType)
    timer.Obj.OnTimeout(timer)
  }
}

func (self *TimerThread) AddTimer(timeoutMs uint32, timeType int, obj TimerObj) uint32 {
  self.mutex.Lock()
  absTime := self.now + uint64(timeoutMs)
  timer := newTimer(self.nowTimerId, absTime, timeType, obj)
  timerId := self.nowTimerId
  self.nowTimerId += 1

  self.newTimerList.PushBack(timer)

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