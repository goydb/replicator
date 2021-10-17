package logger

// Noops logs nothing
type Noop struct {
}

func (s *Noop) Debug(args ...interface{}) {
}

func (s *Noop) Info(args ...interface{}) {
}

func (s *Noop) Warning(args ...interface{}) {
}

func (s *Noop) Error(args ...interface{}) {
}

func (s *Noop) Debugf(format string, args ...interface{}) {
}

func (s *Noop) Infof(format string, args ...interface{}) {
}

func (s *Noop) Warningf(format string, args ...interface{}) {
}

func (s *Noop) Errorf(format string, args ...interface{}) {
}
