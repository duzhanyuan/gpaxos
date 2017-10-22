package logstorage

import (
  "io/ioutil"
  "fmt"
  "testing"
)

func Test_basic(t *testing.T) {
  db := LogStorage{}
  testValue := "test"
  var testInstanceId uint64 = 111

  tmp, _ := ioutil.TempDir("/tmp", "gpaxos")
  db.Init(tmp)

  err := db.Put(WriteOptions{}, testInstanceId, testValue)
  if err != nil {
    fmt.Printf("put error: %v", err)
    return
  }

  var value []byte
  err = db.Get(testInstanceId, &value)
  if err != nil {
    fmt.Printf("get error: %v", err)
  }

  if string(value) != testValue {
    t.Errorf("get error:%v\n", len(string(value)))
  }
}
