package algorithm

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/storage"
  "sync"
  "github.com/lichuang/gpaxos/util"
  "time"

  log "github.com/lichuang/log4go"
)

type LearnerSender struct {
  config          *config.Config
  learner         *Learner
  paxosLog        *storage.PaxosLog
  isSending       bool
  beginInstanceID uint64
  sendToNodeID    uint64
  isConfirmed     bool
  ackInstanceID   uint64
  absLastAckTime  uint64
  absLastSendTime uint64
  isEnd           bool
  isStart         bool
  mutex           sync.Mutex
}

func NewLearnerSender(instance *Instance, learner *Learner) *LearnerSender {
  sender := &LearnerSender{
    config:   instance.config,
    learner:  learner,
    paxosLog: instance.paxosLog,
    isEnd:    false,
    isStart:  false,
  }

  sender.SendDone()

  return sender
}

func (self *LearnerSender) Start() {
  start := make(chan bool, 1)
  go self.main(start)
  <-start
}

func (self *LearnerSender) Stop() {
  self.isEnd = true
}

func (self *LearnerSender) main(start chan bool) {
  start <- true
  self.isStart = true

  for {
    self.WaitToSend()

    if self.isEnd {
      return
    }

    self.SendLearnedValue(self.beginInstanceID, self.sendToNodeID)

    self.SendDone()
  }
}

func (self *LearnerSender) ReleshSending() {
  self.absLastSendTime = util.NowTimeMs()
}

func (self *LearnerSender) IsImSending() bool {
  if !self.isSending {
    return false
  }

  nowTime := util.NowTimeMs()
  var passTime uint64 = 0
  if nowTime > self.absLastSendTime {
    passTime = nowTime - self.absLastSendTime
  }

  if passTime >= common.GetLeanerSenderPrepareTimeoutMs() {
    return false
  }

  return true
}

func (self *LearnerSender) CheckAck(sendInstanceId uint64) bool {
  if sendInstanceId < self.ackInstanceID {
    log.Info("Already catch up, ack instanceid %d now send instanceid %d",
      self.ackInstanceID, sendInstanceId)
    return false
  }

  for sendInstanceId > self.ackInstanceID+common.GetLeanerSender_Ack_Lead() {
    nowTime := util.NowTimeMs()
    var passTime uint64 = 0
    if nowTime > self.absLastAckTime {
      passTime = nowTime - self.absLastAckTime
    }

    if passTime >= common.GetLeanerSender_Ack_TimeoutMs() {
      log.Error("Ack timeout, last acktime %d now send instanceid %d",
        self.absLastAckTime, sendInstanceId)
      return false
    }

    time.Sleep(10 * time.Microsecond)
  }

  return true
}

func (self *LearnerSender) Prepare(beginInstanceId uint64, sendToNodeId uint64) bool {
  self.mutex.Lock()

  prepareRet := false
  if !self.IsImSending() && !self.isConfirmed {
    prepareRet = true

    self.isSending = true
    self.absLastSendTime = util.NowTimeMs()
    self.absLastAckTime = self.absLastSendTime
    self.beginInstanceID = beginInstanceId
    self.ackInstanceID = beginInstanceId
    self.sendToNodeID = sendToNodeId
  }

  self.mutex.Unlock()
  return prepareRet
}

func (self *LearnerSender) Confirm(beginInstanceId uint64, sendToNodeId uint64) bool {
  self.mutex.Lock()

  confirmRet := false
  if self.IsImSending() && !self.isConfirmed {
    if self.beginInstanceID == beginInstanceId && self.sendToNodeID == sendToNodeId {
      confirmRet = true
      self.isConfirmed = true
    }
  }

  self.mutex.Unlock()
  return confirmRet
}

func (self *LearnerSender) Ack(ackInstanceId uint64, fromNodeId uint64) {
  self.mutex.Lock()
  if self.IsImSending() && self.isConfirmed {
    if self.sendToNodeID == fromNodeId {
      if self.ackInstanceID > self.ackInstanceID {
        self.ackInstanceID = ackInstanceId
        self.absLastAckTime = util.NowTimeMs()
      }
    }
  }
  self.mutex.Unlock()
}

func (self *LearnerSender) WaitToSend() {
  self.mutex.Lock()

  for !self.isConfirmed {
    time.Sleep(100 * time.Microsecond)
    if self.isEnd {
      break
    }
  }

  self.mutex.Unlock()
}

func (self *LearnerSender) SendLearnedValue(beginInstanceId uint64, sendToNodeId uint64) {
  log.Info("BeginInstanceID %d SendToNodeID %d", beginInstanceId, sendToNodeId)

  sendInstanceId := beginInstanceId

  sendQps := common.GetLearnerSenderSendQps()
  var sleepMs uint64 = 1
  if sendQps > 1000 {
    sleepMs = sendQps/1000 + 1
  }
  var sendInterval uint64 = sleepMs

  var sendCnt uint64 = 0
  var lastCksum uint32
  for sendInstanceId < self.learner.GetInstanceId() {
    err := self.SendOne(sendInstanceId, sendToNodeId, &lastCksum)
    if err != nil {
      log.Error("SendOne fail, SendInstanceID %d SendToNodeID %d error %v",
        sendInstanceId, sendToNodeId, err)
      return
    }

    if !self.CheckAck(sendInstanceId) {
      break
    }

    sendCnt++
    sendInstanceId++
    self.ReleshSending()

    if sendCnt >= sendInterval {
      sendCnt = 0
      time.Sleep(time.Duration(sleepMs) * time.Microsecond)
    }
  }
}

func (self *LearnerSender) SendOne(sendInstanceId uint64, sendToNodeId uint64, lastCksum *uint32) error {
  state, err := self.paxosLog.ReadState(sendInstanceId)
  if err != nil {
    return err
  }

  ballot := NewBallotNumber(state.GetAcceptedID(), state.GetAcceptedNodeID())

  err = self.learner.SendLearnValue(sendToNodeId, sendInstanceId, *ballot,
    state.GetAcceptedValue(), *lastCksum, true)

  *lastCksum = state.GetChecksum()

  return err
}

func (self *LearnerSender) SendDone() {
  self.mutex.Lock()

  self.isSending = false
  self.isConfirmed = false
  self.beginInstanceID = common.INVALID_INSTANCEID
  self.sendToNodeID = common.NULL_NODEID
  self.absLastAckTime = 0
  self.ackInstanceID = 0
  self.absLastSendTime = 0

  self.mutex.Unlock()
}
