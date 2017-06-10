package util

import (
	"encoding/binary"
)

var INT32SIZE int = 0
var INT64SIZE int = 0
var UINT32SIZE int = 0
var UINT64SIZE int = 0
var UINT16SIZE int = 0

func init() {
	INT32SIZE = binary.Size(int32(0))
	INT64SIZE = binary.Size(int64(0))

	UINT64SIZE = binary.Size(uint64(0))

	UINT32SIZE = binary.Size(uint32(0))

    UINT16SIZE = binary.Size(uint16(0))
}
