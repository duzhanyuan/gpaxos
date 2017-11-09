package algorithm

import (
  "testing"
  "github.com/lichuang/gpaxos/util"
)

func TestPackMsg(t *testing.T) {
  base := &Base{
  }

  buf, header, _ := base.packBaseMsg([]byte("test"), 100)
  body, err := unpackBaseMsg(buf, header)

  util.TestAssert(t,
    err == nil,
    "error %v", err)

  util.TestAssert(t,
    string(body) == "test",
      "error body:%s, expected:%s", string(body), "test")

  util.TestAssert(t,
    header.GetCmdid() == 100,
    "error cmd id:%d, expected:%d", header.GetCmdid(), 100)
}
