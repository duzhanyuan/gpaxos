package common

import (
  "github.com/syndtr/goleveldb/leveldb/errors"
)

const (
  CRC32_SKIP    = 8
  NET_CRC32SKIP = 7
)

const Version = 1

// error code
var (
  ErrKeyNotFound       = errors.New("key not found")
  ErrGetFail           = errors.New("get fail")
  ErrInvalidGroupIndex = errors.New("invalid group index")
  ErrInvalidInstanceId = errors.New("invalid instanceid")
  ErrInvalidMetaFileId = errors.New("invalid meta file id")
  ErrInvalidMsg        = errors.New("invalid msg")
  ErrFileNotExist      = errors.New("file not exist")
  ErrDbNotInit         = errors.New("db not init yet")
)

const (
  MsgCmd_PaxosMsg      = 1
  MsgCmd_CheckpointMsg = 2
)

const (
  MsgType_PaxosPrepare                     = 1
  MsgType_PaxosPrepareReply                = 2
  MsgType_PaxosAccept                      = 3
  MsgType_PaxosAcceptReply                 = 4
  MsgType_PaxosLearner_AskforLearn         = 5
  MsgType_PaxosLearner_SendLearnValue      = 6
  MsgType_PaxosLearner_ProposerSendSuccess = 7
  MsgType_PaxosProposal_SendNewValue       = 8
  MsgType_PaxosLearner_SendNowInstanceID   = 9
  MsgType_PaxosLearner_ComfirmAskforLearn  = 10
  MsgType_PaxosLearner_SendLearnValue_Ack  = 11
  MsgType_PaxosLearner_AskforCheckpoint    = 12
  MsgType_PaxosLearner_OnAskforCheckpoint  = 13
)

const (
  Timer_Proposer_Prepare_Timeout = 1
  Timer_Proposer_Accept_Timeout  = 2
  Timer_Learner_Askforlearn_noop = 3
  Timer_Instance_Commit_Timeout  = 4
)

var INVALID_INSTANCEID uint64 = uint64(-1)