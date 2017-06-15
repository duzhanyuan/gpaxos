package algorithm

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/logstorage"
)

type Learner struct {
  Base                             *Base
  PaxosLog                         *logstorage.PaxosLog
  Acceptor                         *Acceptor
  LearnerState                     *LearnerState
  IsImLearning                     bool
  HighestSeenInstanceID            uint64
  HighestSeenInstanceID_FromNodeID uint64
}

func NewLearner(config *config.Config, transport common.MsgTransport,
  instance *Instance, acceptor *Acceptor,
  storage logstorage.LogStorage) *Learner {
  learner := new(Learner)

  learner.Base = newBase(config, transport, instance)
  learner.PaxosLog = logstorage.NewPaxosLog(storage)
  learner.Acceptor = acceptor
  learner.IsImLearning = false
  learner.HighestSeenInstanceID = 0
  learner.HighestSeenInstanceID_FromNodeID = 0

  return learner
}
