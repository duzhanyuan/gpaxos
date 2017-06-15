package util

import "bytes"

func AppendBytes(inputs ...[]byte) [] byte {
  return bytes.Join(inputs, []byte(""))
}

func CopyBytes(src []byte) [] byte {
  dst := make([]byte, len(src))
  copy(dst, src)
  return dst
}
