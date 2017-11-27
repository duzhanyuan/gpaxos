package algorithm

import "fmt"

type BallotNumber struct {
  proposalId uint64
  nodeId     uint64
}

func NewBallotNumber(proposalId uint64, nodeId uint64) *BallotNumber {
  return &BallotNumber{
    proposalId: proposalId,
    nodeId:     nodeId,
  }
}

func (self *BallotNumber) String() string {
  return fmt.Sprintf("%d:%d", self.proposalId, self.nodeId)
}

func (self *BallotNumber) BE(other *BallotNumber) bool {
  if self.proposalId == other.proposalId {
    return self.nodeId >= other.nodeId
  }

  return self.proposalId >= other.proposalId
}

func (self *BallotNumber) NE(other *BallotNumber) bool {
  return self.proposalId != other.proposalId ||
    self.nodeId != other.nodeId
}

func (self *BallotNumber) EQ(other *BallotNumber) bool {
  return !self.NE(other)
}

func (self *BallotNumber) BT(other *BallotNumber) bool {
  if self.proposalId == other.proposalId {
    return self.nodeId > other.nodeId
  }

  return self.proposalId > other.proposalId
}

func (self *BallotNumber) IsNull() bool {
  return self.proposalId == 0
}

func (self *BallotNumber) Clone(bn *BallotNumber) {
  self.nodeId = bn.nodeId
  self.proposalId = bn.proposalId
}

func (self *BallotNumber) Reset() {
  self.nodeId = 0
  self.proposalId = 0
}