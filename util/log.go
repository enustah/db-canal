package util

import (
	log "github.com/sirupsen/logrus"
	"os"
)

var l *log.Logger

func init() {
	l = log.New()
	l.SetFormatter(&log.TextFormatter{
		DisableQuote:    true,
		FullTimestamp:   true,
		TimestampFormat: "2006/01/02 15:04:05",
	})
	l.SetOutput(os.Stdout)
}

func GetLog() *log.Logger {
	return l
}

func SetLogLevel(level log.Level) {
	l.SetLevel(level)
}
