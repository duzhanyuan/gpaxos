package common

import (
  "github.com/syndtr/goleveldb/leveldb/errors"
  "math"
)

const (
  CRC32_SKIP    = 0
  NET_CRC32SKIP = 0
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
  ErrNodeNotFound      = errors.New("node not found")
	ErrWriteFileFail     = errors.New("write file fail")
)

// MsgCmd
const (
  MsgCmd_PaxosMsg      = 1
  MsgCmd_CheckpointMsg = 2
)

// PaxosMsgFlagType
const (
  PaxosMsgFlagType_SendLearnValue_NeedAck = 1
)

// PaxosMsgType
const (
  MsgType_PaxosPrepare                     = 1  // for acceptor
  MsgType_PaxosPrepareReply                = 2  // for proposer
  MsgType_PaxosAccept                      = 3  // for acceptor
  MsgType_PaxosAcceptReply                 = 4  //
  MsgType_PaxosLearner_AskforLearn         = 5
  MsgType_PaxosLearner_SendLearnValue      = 6
  MsgType_PaxosLearner_ProposerSendSuccess = 7
  MsgType_PaxosProposal_SendNewValue       = 8
  MsgType_PaxosLearner_SendNowInstanceID   = 9
  MsgType_PaxosLearner_ConfirmAskforLearn  = 10
  MsgType_PaxosLearner_SendLearnValue_Ack  = 11
  MsgType_PaxosLearner_AskforCheckpoint    = 12
  MsgType_PaxosLearner_OnAskforCheckpoint  = 13
)

// TimerType
const (
  Timer_Proposer_Prepare_Timeout = 1
  Timer_Proposer_Accept_Timeout  = 2
  Timer_Learner_Askforlearn_noop = 3
  Timer_Instance_Commit_Timeout  = 4
)

// CheckpointMsgType
const (
  CheckpointMsgType_SendFile     = 1
  CheckpointMsgType_SendFile_Ack = 2
)

// CheckpointSendFileFlag
const (
  CheckpointSendFileFlag_BEGIN = 1
  CheckpointSendFileFlag_ING   = 2
  CheckpointSendFileFlag_END   = 3
)

// CheckpointSendFileAckFlag
const (
  CheckpointSendFileAckFlag_OK   = 1
  CheckpointSendFileAckFlag_Fail = 2
)

var INVALID_INSTANCEID uint64 = math.MaxUint64
var NULL_NODEID uint64 = math.MaxUint64 - 1
