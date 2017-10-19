package log

import "fmt"

func Debug(format string, params ...interface{}) {
    fmt.Printf(format + "\n", params)
}

func Info(format string, params ...interface{}) {
    fmt.Printf(format + "\n", params)
}

func Error(format string, params ...interface{}) {
    fmt.Printf(format + "\n", params)
}
