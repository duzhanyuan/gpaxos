package util

import (
  "testing"
  "fmt"
  "bytes"
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
  if i != 1 {
    t.Errorf("int32 decode error:%d\n", i)
  }

  DecodeUint32(buffer, INT32SIZE, &u32)
  if u32 != 11 {
    t.Errorf("uint32 decode error:%d\n", u32)
  }

  DecodeUint64(buffer, INT32SIZE+UINT32SIZE, &u64)
  if u64 != 111 {
    t.Errorf("uint64 decode error:%d\n", u64)
  }
}

func Test_binary_string(t *testing.T) {
  len := INT32SIZE + UINT32SIZE + UINT64SIZE
  buffer := make([]byte, len)

  EncodeInt32(buffer, 0, 1)
  EncodeUint32(buffer, INT32SIZE, 11)
  EncodeUint64(buffer, INT32SIZE+UINT32SIZE, 111)

  var i int32
  var u32 uint32
  var u64 uint64

  buf := bytes.NewBufferString(string(buffer)).Bytes()

  DecodeInt32(buf, 0, &i)
  if i != 1 {
    t.Errorf("int32 decode error:%d\n", i)
  }

  DecodeUint32(buf, INT32SIZE, &u32)
  if u32 != 11 {
    t.Errorf("uint32 decode error:%d\n", u32)
  }

  DecodeUint64(buf, INT32SIZE+UINT32SIZE, &u64)
  if u64 != 111 {
    t.Errorf("uint64 decode error:%d\n", u64)
  }
}

func Test_Bytes(t *testing.T) {
  var str string = "abc";
  buf := []byte(str);
  fmt.Printf("buf:%s\n", string(buf))
  buf = AppendBytes(buf, buf, buf)
  fmt.Printf("buf:%s\n", string(buf))
}
