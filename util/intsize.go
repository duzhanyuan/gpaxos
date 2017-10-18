package util

import (
  "encoding/binary"
)

var INT32SIZE int = binary.Size(int32(0))
var INT64SIZE int = binary.Size(int64(0))
var UINT32SIZE int =  binary.Size(uint32(0))
var UINT64SIZE int = binary.Size(uint64(0))
var UINT16SIZE int = binary.Size(uint16(0))
