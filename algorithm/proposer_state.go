package algorithm

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/util"

  log "github.com/lichuang/log4go"
)

// state type
const (
  PAUSE = iota
  PREPARE
)

// save the proposer state data
type ProposerState struct {
  config                      *config.Config

  // save pre-accept ballot number
  highestOtherPreAcceptBallot BallotNumber

  // propose value
  value                       []byte

  // propose id
  proposalId                  uint64

  // save the highest other propose id,
  // next propose id = max(proposalId, highestOtherProposalId) + 1
  highestOtherProposalId      uint64

  state                       int
}

func newProposalState(config *config.Config) *ProposerState {
  proposalState := new(ProposerState)
  proposalState.config = config
  proposalState.proposalId = 1

  return proposalState.init()
}

func (self *ProposerState) init() *ProposerState {
  self.highestOtherProposalId = 0
  self.value = nil
  self.state = PAUSE

  return self
}

func (self *ProposerState) getState() int {
  return self.state
}

func (self *ProposerState) setState(state int) {
  self.state = state
}

func (self *ProposerState) setStartProposalId(proposalId uint64) {
  self.proposalId = proposalId
}

func (self *ProposerState) newPrepare() {
  log.Info("start proposalid %d highestother %d mynodeid %d",
    self.proposalId, self.highestOtherProposalId, self.config.GetMyNodeId())

  // next propose id = max(proposalId, highestOtherProposalId) + 1
  maxProposalId := self.highestOtherProposalId
  if self.proposalId > self.highestOtherProposalId {
    maxProposalId = self.proposalId
  }

  self.proposalId = maxProposalId + 1

  log.Info("end proposalid %d", self.proposalId)
}

func (self *ProposerState) addPreAcceptValue(otherPreAcceptBallot BallotNumber, otherPreAcceptValue []byte) {
  if otherPreAcceptBallot.IsNull() {
    return
  }

  // update value only when the ballot >= highestOtherPreAcceptBallot
  if otherPreAcceptBallot.BT(&self.highestOtherPreAcceptBallot) {
    self.highestOtherPreAcceptBallot = otherPreAcceptBallot
    self.value = util.CopyBytes(otherPreAcceptValue)
  }
}

func (self *ProposerState) getProposalId() uint64 {
  return self.proposalId
}

func (self *ProposerState) getValue() []byte {
  return self.value
}

func (self *ProposerState) setValue(value []byte) {
  self.value = util.CopyBytes(value)
}

func (self *ProposerState) setOtherProposalId(otherProposalId uint64) {
  if otherProposalId > self.highestOtherProposalId {
    self.highestOtherProposalId = otherProposalId
  }
}

func (self *ProposerState) resetHighestOtherPreAcceptBallot() {
  self.highestOtherPreAcceptBallot.Reset()
}