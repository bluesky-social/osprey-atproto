package effector

import (
	"context"
	"log/slog"
)

type SlogLogger struct {
	logger *slog.Logger
}

func NewSlogLogger(slogLogger *slog.Logger) *SlogLogger {
	slogLogger = slogLogger.With("component", "slog_logger")
	return &SlogLogger{logger: slogLogger}
}

func (l *SlogLogger) Name() string {
	return "slog"
}

func (l *SlogLogger) LogEvent(ctx context.Context, log *OspreyEventLog) error {
	return nil
}

func (l *SlogLogger) LogEffect(ctx context.Context, log *OspreyEffectLog) error {
	l.logger.Info("processed effect", "effect", log)
	return nil
}
