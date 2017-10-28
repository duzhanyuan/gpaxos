package util

import (
  "testing"
)

func Test_binary(t *testing.T) {
  len := INT32SIZE + UINT32SIZE + UINT64SIZE
  buffer := make([]byte, len)

  EncodeInt32(buffer, 0, 1)
  EncodeUint32(buffer, INT32SIZE, 11)
  EncodeUint64(buffer, INT32SIZE+UINT32SIZE, 111)

  var i int32
  var u32 uint32
  var u64 uint64

  DecodeInt32(buffer, 0, &i)
  TestAssert(t,
    i == 1,
    "int32 decode error:%d, expected:%d\n", i, 1)

  DecodeUint32(buffer, INT32SIZE, &u32)
  TestAssert(t,
    u32 == 11,
    "uint32 decode error:%d, expected:%d\n", u32, 11)

  DecodeUint64(buffer, INT32SIZE+UINT32SIZE, &u64)
  TestAssert(t,
    u64 == 111,
    "uint64 decode error:%d, expected:%d\n", u64, 111)
}
