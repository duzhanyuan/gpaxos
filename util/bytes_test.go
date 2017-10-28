package util

import (
  "testing"
)

func Test_Bytes(t *testing.T) {
  var str string = "abc";
  buf := []byte(str);
  buf = AppendBytes(buf, buf, buf)
  TestAssert(t,
    string(buf) == "abcabcabc",
    "AppendBytes error:%s, expected:%s\n", string(buf), "abcabcabc")
}
