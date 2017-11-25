package storage

import (
  "io/ioutil"
  "testing"

  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/common"

  "os"
  "fmt"
  "math/rand"
)

func Test_basic(t *testing.T) {
  tmp, _ := ioutil.TempDir("/tmp", "gpaxos")
  filepattern := tmp + "/gpaxoslog/error%Y%M%D%H.log"
  log.NewLogger(filepattern, true, log.INFO)
  defer os.RemoveAll(tmp)

  db := LogStorage{}
  testValue := "test"
  var testInstanceId uint64 = 111

  db.Init(tmp)

  err := db.Put(WriteOptions{}, testInstanceId, []byte(testValue))
  if err != nil {
    t.Errorf("put error: %v", err)
    return
  }

  value, err := db.Get(testInstanceId)
  if err != nil {
    t.Errorf("get error: %v", err)
    return
  }

  if string(value) != testValue {
    t.Errorf("get error:%v\n", len(string(value)))
  }

  db.Del(WriteOptions{}, testInstanceId)
  value, err = db.Get(testInstanceId)
  if err == nil {
    t.Errorf("get deleted value")
  }
}

func Test_many(t *testing.T) {
  tmp, _ := ioutil.TempDir("/tmp", "gpaxos")
  filepattern := tmp + "/gpaxoslog/error%Y%M%D%H.log"
  log.NewLogger(filepattern, true, log.INFO)
  defer os.RemoveAll(tmp)

  // set the log file to be a small size,so we can check rotating file case
  common.SetLogFileMaxSize(1024)

  db := LogStorage{}

  db.Init(tmp)

  current := db.valueStore.file.Name()
  var maxInstanceId uint64
  minInstanceId := rand.Intn(100)
  step := rand.Intn(1024)
  for i := minInstanceId; i < 20000000000; i++ {
    value := fmt.Sprintf("test_%d", i)
    db.Put(WriteOptions{}, uint64(i), []byte(value))
    if i % step == 0 && db.valueStore.file.Name() != current {
      maxInstanceId = uint64(i)
      break
    }
  }

  var minChosenInstanceId = rand.Intn(int(maxInstanceId))
  db.SetMinChosenInstanceID(uint64(minChosenInstanceId))

  test_again := false

again:
  min, _ := db.GetMinChosenInstanceID()
  if min != uint64(minChosenInstanceId) {
    t.Fatalf("min instance %d error, expected:%d, again:%v", min, minChosenInstanceId, test_again)
    return
  }

  max, _ := db.GetMaxInstanceID()
  if max != maxInstanceId {
    t.Fatalf("max instance %d error, expected:%d", max, maxInstanceId)
    return
  }
  for i := minInstanceId; uint64(i) < maxInstanceId; i++ {
    value := fmt.Sprintf("test_%d", i)
    ret, _ := db.Get(uint64(i))
    if string(ret) != value {
      t.Fatalf("value %s error, expected:%s", ret, value)
    }
  }
  if test_again {
    return
  }

  // close and recover db, do test again
  db.Close()
  // create a new db
  db = LogStorage{}
  db.Init(tmp)
  test_again = true
  goto again
}
