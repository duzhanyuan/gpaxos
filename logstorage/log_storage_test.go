package logstorage

import (
  "fmt"
  "testing"
)

func Test_basic(t *testing.T) {
  db := LogStorage{}
  testValue := "test"
  var testInstanceId uint64 = 111

  db.Init("./tmp")

  err := db.Put(WriteOptions{}, testInstanceId, testValue)
  if err != nil {
    fmt.Printf("put error: %v", err)
    return
  }

  var value string
  err = db.Get(testInstanceId, &value)
  if err != nil {
    fmt.Printf("get error: %v", err)
  }

  if value != testValue {
    t.Errorf("get error:%s\n", value)
  }
}
