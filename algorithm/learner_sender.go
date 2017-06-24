package algorithm

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/logstorage"
  "sync"
  "math"
  "github.com/lichuang/gpaxos/util"
  "time"
)

type LearnerSender struct {
  Config          *config.Config
  Learner         *Learner
  PaxosLog        *logstorage.PaxosLog
  IsSending       bool
  BeginInstanceID uint64
  SendToNodeID    uint64
  IsConfirmed     bool
  AckInstanceID   uint64
  AbsLastAckTime  uint64
  AbsLastSendTime uint64
  IsEnd           bool
  IsStart         bool
  Mutex           sync.Mutex
}

func NewLearnerSender(config *config.Config, learner *Learner, storage logstorage.LogStorage) *LearnerSender {
  sender := &LearnerSender{
    Config:   config,
    Learner:  learner,
    PaxosLog: logstorage.NewPaxosLog(storage),
    IsEnd:    false,
    IsStart:  false,
  }

  sender.SendDone()

  return sender
}

func (self *LearnerSender) Stop() {
  self.IsEnd = true
}

func (self *LearnerSender) Main() {
  self.IsStart = true

  for {

  }
}

func (self *LearnerSender) ReleshSending() {
  self.AbsLastSendTime = util.NowTimeMs()
}

func (self *LearnerSender) IsImSending() bool {
  if !self.IsSending {
    return false
  }

  nowTime := util.NowTimeMs()
  var passTime uint64 = 0
  if nowTime > self.AbsLastSendTime {
    passTime = nowTime - self.AbsLastSendTime
  }

  if passTime >= common.GetLeanerSenderPrepareTimeoutMs() {
    return false
  }

  return true
}

func (self *LearnerSender) CheckAck(sendInstanceId uint64) bool {
  if sendInstanceId < self.AckInstanceID {
    log.Info("Already catch up, ack instanceid %d now send instanceid %d",
      self.AckInstanceID, sendInstanceId)
    return false
  }

  for sendInstanceId > self.AckInstanceID+common.GetLeanerSender_Ack_Lead() {
    nowTime := util.NowTimeMs()
    var passTime uint64 = 0
    if nowTime > self.AbsLastAckTime {
      passTime = nowTime - self.AbsLastAckTime
    }

    if passTime >= common.GetLeanerSender_Ack_TimeoutMs() {
      log.Error("Ack timeout, last acktime %d now send instanceid %d",
        self.AbsLastAckTime, sendInstanceId)
      return false
    }

    time.Sleep(10 * time.Microsecond)
  }

  return true
}

func (self *LearnerSender) Prepare(beginInstanceId uint64, sendToNodeId uint64) bool {
  self.Mutex.Lock()

  prepareRet := false
  if !self.IsImSending() && !self.IsConfirmed {
    prepareRet = true

    self.IsSending = true
    self.AbsLastSendTime = util.NowTimeMs()
    self.AbsLastAckTime = self.AbsLastSendTime
    self.BeginInstanceID = beginInstanceId
    self.AckInstanceID = beginInstanceId
    self.SendToNodeID = sendToNodeId
  }

  self.Mutex.Unlock()
  return prepareRet
}

func (self *LearnerSender) Confirm(beginInstanceId uint64, sendToNodeId uint64) bool {
  self.Mutex.Lock()

  confirmRet := false
  if self.IsImSending() && !self.IsConfirmed {
    if self.BeginInstanceID == beginInstanceId && self.SendToNodeID == sendToNodeId {
      confirmRet = true
      self.IsConfirmed = true
    }
  }

  self.Mutex.Unlock()
  return confirmRet
}

func (self *LearnerSender) Ack(ackInstanceId uint64, fromNodeId uint64) {
  self.Mutex.Lock()
  if self.IsImSending() && self.IsConfirmed {
    if self.SendToNodeID == fromNodeId {
      if self.AckInstanceID > self.AckInstanceID {
        self.AckInstanceID = ackInstanceId
        self.AbsLastAckTime = util.NowTimeMs()
      }
    }
  }
  self.Mutex.Unlock()
}

func (self *LearnerSender) WaitToSend() {
  self.Mutex.Lock()

  for !self.IsConfirmed {
    time.Sleep(100 * time.Microsecond)
    if self.IsEnd {
      break
    }
  }

  self.Mutex.Unlock()
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
  for sendInstanceId < self.Learner.GetInstanceId() {
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
  var state common.AcceptorStateData
  err := self.PaxosLog.ReadState(self.Config.GetMyGroupIdx(), sendInstanceId, &state)
  if err != nil {
    return err
  }

  ballot := newBallotNumber(state.GetAcceptedID(), state.GetAcceptedNodeID())

  err = self.Learner.SendLearnValue(sendToNodeId, sendInstanceId, ballot,
                                    state.GetAcceptedValue(), *lastCksum, true)

  *lastCksum = state.GetChecksum()

  return err
}

func (self *LearnerSender) SendDone() {
  self.Mutex.Lock()

  self.IsSending = false
  self.IsConfirmed = false
  self.BeginInstanceID = math.MaxUint64 - 1
  self.SendToNodeID = 0
  self.AbsLastAckTime = 0
  self.AckInstanceID = 0
  self.AbsLastSendTime = 0

  self.Mutex.Unlock()
}
