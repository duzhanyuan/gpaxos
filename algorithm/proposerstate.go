package algorithm

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/util"
)

type ProposerState struct {
  Config                      *config.Config
  HighestOtherPreAcceptBallot BallotNumber
  Value                       []byte
  ProposalId                  uint64
  HighestOtherProposalId      uint64
}

func NewProposalState(config *config.Config) *ProposerState {
  proposalState := new(ProposerState)
  proposalState.Config = config
  proposalState.ProposalId = 1

  proposalState.init()

  return proposalState
}

func (self *ProposerState) init() {
  self.HighestOtherProposalId = 0
}

func (self *ProposerState) SetStartProposalId(proposalId uint64) {
  self.ProposalId = proposalId
}

func (self *ProposerState) NewPrepare() {
  log.Info("start proposalid %d highestother %d mynodeid %d",
    self.ProposalId, self.HighestOtherProposalId, self.Config.GetMyNodeId())

  maxProposalId := self.HighestOtherProposalId
  if self.ProposalId > self.HighestOtherProposalId {
    maxProposalId = self.ProposalId
  }

  self.ProposalId = maxProposalId + 1

  log.Info("end proposalid %d", self.ProposalId)
}

func (self *ProposerState) AddPreAcceptValue(otherPreAcceptBallot BallotNumber, otherPreAcceptValue []byte) {
  if otherPreAcceptBallot.IsNull() {
    return
  }

  if otherPreAcceptBallot.BT(self.HighestOtherPreAcceptBallot) {
    self.HighestOtherPreAcceptBallot = otherPreAcceptBallot
    self.Value = util.CopyBytes(otherPreAcceptValue)
  }
}

func (self *ProposerState) GetProposalId() uint64 {
  return self.ProposalId
}

func (self *ProposerState) GetValue() []byte {
  return self.Value
}

func (self *ProposerState) SetValue(value []byte) {
  self.Value = util.CopyBytes(value)
}

func (self *ProposerState) SetOtherProposalId(otherProposalId uint64) {
  if otherProposalId > self.HighestOtherProposalId {
    self.HighestOtherProposalId = otherProposalId
  }
}

func (self *ProposerState) ResetHighestOtherPreAcceptBallot() {
  self.HighestOtherPreAcceptBallot.Reset()
}
