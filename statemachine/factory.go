package statemachine

import (
  "github.com/lichuang/gpaxos/util"
)

type StatemachineFactory struct {

}

func NewStatemachineFactory() *StatemachineFactory {
  return &StatemachineFactory{}
}

func (self *StatemachineFactory)PackPaxosValue(value[]byte, smid int32) []byte {
  buf := make([] byte, util.INT32SIZE)
  util.EncodeInt32(buf, 0, smid)

  return util.AppendBytes(buf, value)
}

func (self *StatemachineFactory) UnpackPaxosValue(value []byte) ([]byte, int32) {
  var smid int32
  util.DecodeInt32(value, 0, &smid)
  return value[util.INT32SIZE:], smid
}