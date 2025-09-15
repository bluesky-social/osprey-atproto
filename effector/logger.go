package effector

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
)

type OspreyEventLog struct {
	ActionName string    `bigquery:"action_name" json:"actionName"`
	ActionID   int64     `bigquery:"action_id" json:"actionId"`
	Did        string    `bigquery:"did" json:"did"`
	Uri        string    `bigquery:"uri" json:"uri"`
	Cid        string    `bigquery:"cid" json:"cid"`
	Raw        string    `bigquery:"raw" json:"raw"`
	SendTime   time.Time `bigquery:"send_time" json:"sendTime"`
	CreatedAt  time.Time `bigquery:"created_at" json:"createdAt"`
}

type OspreyEffectLog struct {
	ActionName string              `bigquery:"action_name" json:"actionName"`
	ActionID   int64               `bigquery:"action_id" json:"actionId"`
	Subject    string              `bigquery:"subject" json:"subject"`
	Kind       string              `bigquery:"kind" json:"kind"`
	Rules      string              `bigquery:"rules" json:"rules"`
	Comment    string              `bigquery:"comment" json:"comment"`
	Label      bigquery.NullString `bigquery:"label" json:"label"`
	Tag        bigquery.NullString `bigquery:"tag" json:"tag"`
	Email      bigquery.NullString `bigquery:"email" json:"email"`
	CreatedAt  time.Time           `bigquery:"created_at" json:"createdAt"`
}

type OspreyLogger interface {
	Name() string
	LogEvent(ctx context.Context, log *OspreyEventLog) error
	LogEffect(ctx context.Context, log *OspreyEffectLog) error
}

type OspreyLogManager struct {
	loggers []OspreyLogger
}

func NewOspreyLogManager() *OspreyLogManager {
	return &OspreyLogManager{
		loggers: []OspreyLogger{},
	}
}

func (lm *OspreyLogManager) AddLogger(l OspreyLogger) error {
	if lm.includesLogger(l) {
		return fmt.Errorf("a logger with the same name %s already exists", l.Name())
	}
	lm.loggers = append(lm.loggers, l)
	return nil
}

func (lm *OspreyLogManager) LogEvent(ctx context.Context, log *OspreyEventLog) error {
	var err error
	for _, w := range lm.loggers {
		werr := w.LogEvent(ctx, log)
		if werr != nil {

			err = errors.Join(err, werr)
		}
	}
	return err
}

func (lm *OspreyLogManager) LogEffect(ctx context.Context, log *OspreyEffectLog) error {
	var err error
	for _, w := range lm.loggers {
		werr := w.LogEffect(ctx, log)
		if werr != nil {

			err = errors.Join(err, werr)
		}
	}
	return err
}

func (lm *OspreyLogManager) includesLogger(logger OspreyLogger) bool {
	name := logger.Name()
	includes := false
	for _, w := range lm.loggers {
		if w.Name() == name {
			includes = true
			break
		}
	}
	return includes
}
