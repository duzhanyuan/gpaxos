package algorithm

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/util"

  log "github.com/lichuang/log4go"
)

type ProposerState struct {
  config                      *config.Config
  highestOtherPreAcceptBallot BallotNumber
  Value                       []byte
  proposalId                  uint64
  highestOtherProposalId      uint64
}

func NewProposalState(config *config.Config) *ProposerState {
  proposalState := new(ProposerState)
  proposalState.config = config
  proposalState.proposalId = 1

  proposalState.init()

  return proposalState
}

func (self *ProposerState) init() {
  self.highestOtherProposalId = 0
}

func (self *ProposerState) SetStartProposalId(proposalId uint64) {
  self.proposalId = proposalId
}

func (self *ProposerState) NewPrepare() {
  log.Info("start proposalid %d highestother %d mynodeid %d",
    self.proposalId, self.highestOtherProposalId, self.config.GetMyNodeId())

  maxProposalId := self.highestOtherProposalId
  if self.proposalId > self.highestOtherProposalId {
    maxProposalId = self.proposalId
  }

  self.proposalId = maxProposalId + 1

  log.Info("end proposalid %d", self.proposalId)
}

func (self *ProposerState) AddPreAcceptValue(otherPreAcceptBallot BallotNumber, otherPreAcceptValue []byte) {
  if otherPreAcceptBallot.IsNull() {
    return
  }

  if otherPreAcceptBallot.BT(&self.highestOtherPreAcceptBallot) {
    self.highestOtherPreAcceptBallot = otherPreAcceptBallot
    self.Value = util.CopyBytes(otherPreAcceptValue)
  }
}

func (self *ProposerState) GetProposalId() uint64 {
  return self.proposalId
}

func (self *ProposerState) GetValue() []byte {
  return self.Value
}

func (self *ProposerState) SetValue(value []byte) {
  self.Value = util.CopyBytes(value)
}

func (self *ProposerState) SetOtherProposalId(otherProposalId uint64) {
  if otherProposalId > self.highestOtherProposalId {
    self.highestOtherProposalId = otherProposalId
  }
}

func (self *ProposerState) ResetHighestOtherPreAcceptBallot() {
  self.highestOtherPreAcceptBallot.Reset()
}