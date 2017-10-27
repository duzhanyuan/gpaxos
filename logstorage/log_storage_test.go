package logstorage

import (
  "io/ioutil"
  "fmt"
  "testing"

  "github.com/lichuang/gpaxos/log"
  "os"
)

func Test_basic(t *testing.T) {
  log.NewLogger("/tmp/logs/error%Y%M%D%H.log", true, log.DEBUG)

  db := LogStorage{}
  testValue := "test"
  var testInstanceId uint64 = 111

  tmp, _ := ioutil.TempDir("/tmp", "gpaxos")
  db.Init(tmp)

  err := db.Put(WriteOptions{}, testInstanceId, []byte(testValue))
  if err != nil {
    fmt.Printf("put error: %v", err)
    return
  }

  value, err := db.Get(testInstanceId)
  if err != nil {
    fmt.Printf("get error: %v", err)
  }

  if string(value) != testValue {
    t.Errorf("get error:%v\n", len(string(value)))
  }

  os.Remove(tmp)
}
