package algorithm


import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/util"
  "github.com/lichuang/gpaxos/common"
)

type Proposer struct {
  Base
  State *ProposerState
}

func NewProposer(config *config.Config, transport *common.MsgTransport, instance *Instance) *Proposer {
  proposer := &Proposer{
    State: NewProposalState(config),
  }

  proposer.InitForNewPaxosInstance()

  return proposer
}

func (self *Proposer) SetStartProposalID(proposalId uint64) {
  self.State.SetStartProposalId(proposalId)
}

func (self *Proposer) InitForNewPaxosInstance() {

}