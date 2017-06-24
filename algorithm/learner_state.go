package algorithm

import (
  "github.com/golang/protobuf/proto"

  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/util"
  "github.com/lichuang/gpaxos/logstorage"
)

type LearnerState struct {
  Config       *config.Config
  LearnedValue []byte
  IsLearned    bool
  NewChecksum  uint32
  PaxosLog     *logstorage.PaxosLog
}

func NewLearnerState(config *config.Config, storage logstorage.LogStorage) *LearnerState {
  paxosLog := logstorage.NewPaxosLog(storage)

  return &LearnerState{
    Config:      config,
    PaxosLog:    paxosLog,
    IsLearned:   false,
    NewChecksum: 0,
  }
}

func (self *LearnerState) GetNewChecksum() uint32 {
  return self.NewChecksum
}

func (self *LearnerState) LearnValueWithoutWrite(instanceId uint64, value []byte, checksum uint32) {
  self.LearnedValue = util.CopyBytes(value)
  self.IsLearned = false
  self.NewChecksum = checksum
}

func (self *LearnerState) LearnValue(instanceId uint64, learnedBallot BallotNumber, value []byte, lastChecksum uint32) error {
  if instanceId == 0 && lastChecksum == 0 {
    self.NewChecksum = 0
  } else if len(value) > 0 {
    self.NewChecksum = util.Crc32(lastChecksum, value, common.CRC32_SKIP)
  }

  state := common.AcceptorStateData{
    InstanceID:     proto.Uint64(instanceId),
    AcceptedValue:  proto.NewBuffer(value).Bytes(),
    PromiseID:      proto.Uint64(learnedBallot.proposalId),
    PromiseNodeID:  proto.Uint64(learnedBallot.nodeId),
    AcceptedID:     proto.Uint64(learnedBallot.nodeId),
    AcceptedNodeID: proto.Uint64(learnedBallot.nodeId),
    Checksum:       proto.Uint32(self.NewChecksum),
  }

  options := logstorage.WriteOptions{
    Sync: false,
  }

  err := self.PaxosLog.WriteState(options, self.Config.GetMyGroupIdx(), instanceId, state)
  if err != nil {
    log.Error("logstorage writestate fail, instanceid %d valuelen %d err %v",
      instanceId, len(value), err)
    return err
  }

  self.LearnValueWithoutWrite(instanceId, value, self.NewChecksum)

  return nil
}

func (self *LearnerState) GetLearnValue() []byte {
  return self.LearnedValue
}

func (self *LearnerState) GetIsLearned() bool {
  return self.IsLearned
}
