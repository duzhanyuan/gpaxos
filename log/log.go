package log

import (
  //"github.com/golang/glog"
  "github.com/cihub/seelog"
)

func init() {
}

func Debugf(format string, params ... interface{}) {
    seelog.Debugf(format, params...)
}

func Infof(format string, params ...interface{}) {
  seelog.Infof(format, params...)
}

func Errorf(format string, params ...interface{}) {
  seelog.Errorf(format, params...)
}
