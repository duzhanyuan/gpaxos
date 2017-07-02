package algorithm

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/checkpoint"
  "github.com/lichuang/gpaxos/sm_base"
  "github.com/lichuang/gpaxos/util"
)

type CheckpointSender struct {
  SendNodeId           uint64
  Config               *config.Config
  Learner              *Learner
  Factory              *sm_base.StateMachineFactory
  CpMng                *checkpoint.CheckpointManager
  isEnd                bool
  IsEnded              bool
  UUID                 uint64
  Sequence             uint64
  AckSequence          uint64
  AbsLastAckTime       uint64
  AlreadySendedFileMap map[string]bool
}

func NewCheckpointSender(sendNodeId uint64, config *config.Config, learner *Learner,
                         factory *sm_base.StateMachineFactory, cpmng *checkpoint.CheckpointManager) *CheckpointSender {
  return &CheckpointSender{
    SendNodeId:sendNodeId,
    Config:config,
    Learner:learner,
    Factory:factory,
    CpMng:cpmng,
    UUID:(config.GetMyNodeId() ^ learner.GetInstanceId()) + uint64(util.Rand()),
  }
}

func (self *CheckpointSender) Stop() {
  if !self.IsEnded {
    self.isEnd = true
  }
}

func (self CheckpointSender) Start() {
  go self.main()
}

func (self *CheckpointSender) main() {
  self.AbsLastAckTime = util.NowTimeMs()
  needContinue := false
  
}

func (self *CheckpointSender) Ack(sendNodeId uint64, uuid uint64, sequence uint64) {

}

func (self *CheckpointSender) End() {
  self.isEnd = true
}

func (self *CheckpointSender) IsEnd() bool {
  return self.isEnd
}