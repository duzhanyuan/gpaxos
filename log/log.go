package log

import (
  log "github.com/lichuang/log4go"
)

const (
  DEBUG = iota
  INFO
  WARNING
  ERROR
  FATAL
)

func NewLogger(filePattern string, console bool, level int) {
  w1 := log.NewFileWriter()
  //w1.SetPathPattern("/tmp/logs/error%Y%M%D%H.log")
  w1.SetPathPattern(filePattern)
  log.Register(w1)

  if console {
    w2 := log.NewConsoleWriter()
    log.Register(w2)
  }
  log.SetLevel(level)
}

func NewConsoleLogger() {
  w2 := log.NewConsoleWriter()
  log.Register(w2)
  log.SetLevel(log.DEBUG)
}
/*
func Debug(format string, params ... interface{}) {
    log.Debug(format, params...)
}

func Info(format string, params ...interface{}) {
  log.Info(format, params...)
}

func Error(format string, params ...interface{}) {
  log.Error(format, params...)
}
*/