package algorithm

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/logstorage"
  "sync"
  "math"
)

type LearnerSender struct {
  Config          *config.Config
  Learner         *Learner
  PaxosLog        *logstorage.PaxosLog
  IsImSending     bool
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

func NewLearnerSender(config *config.Config, learner *Learner, log *logstorage.PaxosLog) *LearnerSender {
  sender := &LearnerSender{
    Config:   config,
    Learner:  learner,
    PaxosLog: log,
    IsEnd:    false,
    IsStart:  false,
  }

  sender.SendDone()

  return sender
}

func (self *LearnerSender) SendDone() {
  self.Mutex.Lock()

  self.IsImSending = false
  self.IsConfirmed = false
  self.BeginInstanceID = math.MaxUint64 - 1
  self.SendToNodeID = 0
  self.AbsLastAckTime = 0
  self.AckInstanceID = 0
  self.AbsLastSendTime = 0

  self.Mutex.Unlock()
}
