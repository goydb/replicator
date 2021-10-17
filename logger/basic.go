package logger

import (
	"fmt"
	"log"
)

var (
	ldebug = "[\u001b[37;1mDEBUG\u001b[0m] "
	linfo  = "[\u001b[32;1mINFO\u001b[0m] "
	lwarn  = "[\u001b[33;1mWARN\u001b[0m]  "
	lerror = "[\u001b[31;1mERROR\u001b[0m] "
)

// Stdout logs to stdout using Go's log package
type Stdout struct {
}

func (s *Stdout) Debug(args ...interface{}) {
	log.Println(ldebug + fmt.Sprint(args...))
}

func (s *Stdout) Info(args ...interface{}) {
	log.Println(linfo + fmt.Sprint(args...))
}

func (s *Stdout) Warning(args ...interface{}) {
	log.Println(lwarn + fmt.Sprint(args...))
}

func (s *Stdout) Error(args ...interface{}) {
	log.Println(lerror + fmt.Sprint(args...))
}

func (s *Stdout) Debugf(format string, args ...interface{}) {
	log.Printf(ldebug+format, args...)
}

func (s *Stdout) Infof(format string, args ...interface{}) {
	log.Printf(lwarn+format, args...)
}

func (s *Stdout) Warningf(format string, args ...interface{}) {
	log.Printf(lwarn+format, args...)
}

func (s *Stdout) Errorf(format string, args ...interface{}) {
	log.Printf(lerror+format, args...)
}
