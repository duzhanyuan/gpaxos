package algorithm

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/logstorage"
  "github.com/lichuang/gpaxos/checkpoint"
  "github.com/lichuang/gpaxos/sm_base"
)

type Learner struct {
  Base
  PaxosLog                         *logstorage.PaxosLog
  Acceptor                         *Acceptor
  LearnerState                     *LearnerState
  IsImLearning                     bool
  HighestSeenInstanceID            uint64
  HighestSeenInstanceID_FromNodeID uint64
  LastAckInstanceId                uint64
  SMFactory                        *sm_base.StateMachineFactory
  CpMng                            *checkpoint.CheckpointManager
  LearnerSender           *LearnerSender
}

func NewLearner(config *config.Config, transport *common.MsgTransport,
  instance *Instance, acceptor *Acceptor,
  storage logstorage.LogStorage, thread *IOThread, manager *checkpoint.CheckpointManager,
  factory *sm_base.StateMachineFactory) *Learner {
  learner := &Learner{
    Base:                             newBase(config, transport, instance),
    PaxosLog:                         logstorage.NewPaxosLog(storage),
    Acceptor:                         acceptor,
    IsImLearning:                     false,
    HighestSeenInstanceID:            0,
    HighestSeenInstanceID_FromNodeID: 0,
    LastAckInstanceId:0,
    CpMng:                            manager,
    SMFactory:                        factory,
    LearnerState:NewLearnerState(config, storage),
  }
  learner.LearnerSender = NewLearnerSender(config, learner, storage)

  return learner
}

func (self *Learner) InitForNewPaxosInstance() {
  self.
}

func (self *Learner) ProposerSendSuccess(instanceId uint64, proposerId uint64) {

}

func (self *Learner) SendLearnValue(sendNodeId uint64, learnInstanceId uint64,
                                    ballot BallotNumber, value[]byte, cksum uint32, needAck bool) error {

}
