package common

type InsideOptions struct {
}

var gInsideOptions *InsideOptions

func (self *InsideOptions) GetLogFileMaxSize() int64 {
  return 10240
}

func GetLogFileMaxSize() int64 {
  return 10240
}
