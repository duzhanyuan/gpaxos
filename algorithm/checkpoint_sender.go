package algorithm

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/checkpoint"
  "github.com/lichuang/gpaxos/sm_base"
)

type CheckpointSender struct {
  SendNodeId           uint64
  Config               *config.Config
  Learner              *Learner
  Factory              *sm_base.StateMachineFactory
  CpMng                *checkpoint.CheckpointManager
  IsEnd                bool
  IsEnded              bool
  UUID                 uint64
  Sequence             uint64
  AckSequence          uint64
  AbsLastAckTime       uint64
  AlreadySendedFileMap map[string]bool
}

func (self *CheckpointSender) Stop() {

}
