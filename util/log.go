package util

import (
	"github.com/skoo87/log4go"
)

const (
	DEBUG = iota
	INFO
	WARNING
	ERROR
	FATAL
)

type LogOptions struct {
	ConsoleOn bool

	Pattern string

	Level int
}

func Debug(fmt string, args ...interface{}) {
	log4go.Debug(fmt, args)
}

func Error(fmt string, args ...interface{}) {
	log4go.Error(fmt, args)
}

func Info(fmt string, args ...interface{}) {
	log4go.Info(fmt, args)
}

func Init(options *LogOptions) error {
	if options.ConsoleOn {
		w := log4go.NewConsoleWriter()
		w.SetColor(true)

		log4go.Register(w)
	}

	log := log4go.NewFileWriter()
	log.SetPathPattern(options.Pattern)
	log.Init()

	log4go.Register(log)

	log4go.SetLevel(log4go.DEBUG)
	log4go.SetLayout("2006-01-02 15:04:05")

	return nil
}
