package algorithm

import (
  "errors"
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/util"
  "github.com/lichuang/gpaxos/common"
)

type IOThread struct {
  IsEnd        bool
  IsStart      bool
  TimerMng     *util.TimeManager
  TimerIdMap   map[uint32]bool
  Instance     *Instance
  MessageQueue *util.Queue
  RetryQueue   *util.Deque
  QueueMemSize int
}

func NewIOThread(config *config.Config, instance *Instance) *IOThread {
  return &IOThread{
    TimerMng:     util.NewTimerManager(),
    TimerIdMap:   make(map[uint32]bool),
    Instance:     instance,
    MessageQueue: util.NewQueue(),
    RetryQueue:   util.NewDeque(),
    QueueMemSize: 0,
  }
}

func (self *IOThread) Run() {
  self.IsEnd = false
  self.IsStart = true
  go self.Main()
}

func (self *IOThread) Main() {
  for {
    var nextTimeout int32 = 1000
    self.DealwithTimeout(&nextTimeout)

    self.OneLoop(nextTimeout)

    if self.IsEnd {
      log.Info("io thread end")
      break
    }
  }
}

func (self *IOThread) OneLoop(timeoutMs int32) {
  self.MessageQueue.Lock()
  ret := self.MessageQueue.PeekWithTimeout(timeoutMs)
  if ret == nil {
    self.MessageQueue.Unlock()
  } else {
    self.MessageQueue.Pop()
    self.MessageQueue.Unlock()

    msg := ret.(string)
    if msg != "" && len(msg) > 0 {
      self.QueueMemSize -= len(msg)
      self.Instance.OnReceiveMsg(msg)
    }
  }

  self.DealWithRetry()

  self.Instance.CheckNewValue()
}

func (self *IOThread) AddNotify() {
  self.MessageQueue.Lock()
  self.MessageQueue.Add("")
  self.MessageQueue.Unlock()
}

func (self *IOThread) AddMessage(msg string) error {
  self.MessageQueue.Lock()

  if self.MessageQueue.Size() > common.GetMaxQueueLen() {
    self.MessageQueue.Unlock()
    log.Error("Queue full, skip msg")
    return errors.New("Queue full, skip msg")
  }

  if self.QueueMemSize > common.GetMaxQueueMemSize() {
    self.MessageQueue.Unlock()
    log.Error("queue memsize %d too large, can't enqueue", self.QueueMemSize)
    return errors.New("Queue memsize full")
  }

  self.MessageQueue.Add(msg)
  self.QueueMemSize += len(msg)

  self.MessageQueue.Unlock()

  return nil
}

func (self *IOThread) AddRetryPaxosMsg(msg *common.PaxosMsg) {
  if self.RetryQueue.Size() > common.GetMaxRetryQueueLen() {
    self.RetryQueue.Pop()
  }

  self.RetryQueue.Append(msg)
}

func (self *IOThread) DealWithRetry() {
  haveRetryOne := false

  for !self.RetryQueue.Empty() {
    item := self.RetryQueue.Pop()
    msg := item.(*common.PaxosMsg)

    msgInstanceId := msg.GetInstanceID()
    nowInstanceId := self.Instance.GetNowInstanceID()

    if msgInstanceId > nowInstanceId + 1 {
      break
    }

    if msgInstanceId == nowInstanceId + 1 {
      if haveRetryOne {
        log.Debug("retry msg (i+1). instanceid %d", msgInstanceId)
        self.Instance.OnReceivePaxosMsg(msg)
      } else {
        break
      }
    }

    if msgInstanceId == nowInstanceId {
      log.Debug("retry msg. instanceid %d", msgInstanceId)
      self.Instance.OnReceivePaxosMsg(msg)
      haveRetryOne = true
    }
  }
}

func (self *IOThread) AddTimer(timeout uint64, timerType int) uint32 {
  if timeout == -1 {
    return -1
  }

  absTime := util.GetSteadyClockMS() + timeout
  timer := self.TimerMng.AddTimerWithType(absTime, timerType)
  self.TimerIdMap[timer.TimerId] = true

  return timer.TimerId
}

func (self *IOThread) RemoveTimer(timerId *uint32) {
  _, exist := self.TimerIdMap[*timerId]
  if !exist {
    return
  }

  delete(self.TimerIdMap, *timerId)
  *timerId = 0
}

func (self *IOThread) DealwithTimeoutOne(timerId uint32, timerType int) {
  _, exist := self.TimerIdMap[timerId]
  if !exist {
    return
  }

  delete(self.TimerIdMap, timerId)
  self.Instance.OnTimeout(timerId, timerType)
}

func (self *IOThread) DealwithTimeout(nextTimeout *int32) {
  hasTimeout := true

  for {
    var timerId uint32 = 0
    var timerType int = 0

    hasTimeout = self.TimerMng.PopTimeout(&timerId, &timerType)
    if !hasTimeout {
      break
    }

    self.DealwithTimeoutOne(timerId, timerType)
    *nextTimeout = self.TimerMng.GetNextTimeout()
    if *nextTimeout != 0 {
      break
    }
  }
}
