package common

import "github.com/syndtr/goleveldb/leveldb/errors"

const (
	CRC32_SKIP = 8
    NET_CRC32SKIP = 7
)

// error code
var (
	ErrKeyNotFound       = errors.New("key not found")
	ErrGetFail           = errors.New("get fail")
	ErrInvalidGroupIndex = errors.New("invalid group index")
	ErrInvalidInstanceId = errors.New("invalid instanceid")
	ErrInvalidMetaFileId = errors.New("invalid meta file id")
	ErrFileNotExist      = errors.New("file not exist")
	ErrDbNotInit         = errors.New("db not init yet")
)

const(
    MsgCmd_PaxosMsg = 1
    MsgCmd_CheckpointMsg = 2
)