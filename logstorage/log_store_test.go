package logstorage

import (
  "testing"
)

func Test_encode_decode(t *testing.T) {
  var fileid int32 = 170
  var offset uint64 = 11678
  var cksum uint32 = 111233456

  var encode string
  log := new(LogStore)

  log.EncodeFileId(fileid, offset, cksum, &encode)

  var i int32
  var o uint64
  var c uint32

  log.DecodeFileId(encode, &i, &o, &c)

  if i != fileid || o != offset || c != cksum {
    t.Errorf("encode/decode error")
  }
}
