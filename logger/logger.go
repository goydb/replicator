package logger

type Logger interface {
	Debug(args ...interface{})

	Info(args ...interface{})

	Warning(args ...interface{})

	Error(args ...interface{})

	Debugf(format string, args ...interface{})

	Infof(format string, args ...interface{})

	Warningf(format string, args ...interface{})

	Errorf(format string, args ...interface{})
}
