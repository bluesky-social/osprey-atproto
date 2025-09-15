package effector

import (
	"context"
	"fmt"
	"log/slog"

	bigqueryinserter "github.com/bluesky-social/osprey-atproto/pkg/bigquery_inserter"
)

type BigQueryLogger struct {
	eventInserter  *bigqueryinserter.BigQueryInserter
	effectInserter *bigqueryinserter.BigQueryInserter
	logger         *slog.Logger
}

type BigQueryLoggerArgs struct {
	CredentialsJson []byte
	ProjectID       string
	DatasetID       string
	Logger          *slog.Logger
}

func NewBigQueryLogger(args *BigQueryLoggerArgs) (*BigQueryLogger, error) {
	slog := args.Logger.With("component", "bigquery_logger")

	logger := &BigQueryLogger{
		logger: slog,
	}

	evtInserter, err := bigqueryinserter.New(context.Background(), &bigqueryinserter.Args{
		BaseArgs: bigqueryinserter.BaseArgs{
			CredentialsJson: args.CredentialsJson,
			ProjectID:       args.ProjectID,
			DatasetID:       args.DatasetID,
			TableID:         "osprey-events",
			MaxPendingSends: 100,
		},
		// We send _all_ events to BigQuery, so we probably don't want this to be anything under the average number of events we process per second
		BatchSize: 500,
		Logger:    slog,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create bigquery inserter: %w", err)
	}
	logger.eventInserter = evtInserter

	effectInserter, err := bigqueryinserter.New(context.Background(), &bigqueryinserter.Args{
		BaseArgs: bigqueryinserter.BaseArgs{
			CredentialsJson: args.CredentialsJson,
			ProjectID:       args.ProjectID,
			DatasetID:       args.DatasetID,
			TableID:         "osprey-effects",
			MaxPendingSends: 100,
		},
		// We only record effects that happen on events here, so a lower value is fine
		BatchSize: 25,
		Logger:    slog,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create bigquery inserter: %w", err)
	}
	logger.effectInserter = effectInserter

	return logger, nil
}

func (l *BigQueryLogger) Name() string {
	return "bigquery"
}

func (l *BigQueryLogger) LogEvent(ctx context.Context, log *OspreyEventLog) error {
	return l.eventInserter.Insert(context.Background(), log)
}

func (l *BigQueryLogger) LogEffect(ctx context.Context, log *OspreyEffectLog) error {
	return l.effectInserter.Insert(context.Background(), log)
}

func (l *BigQueryLogger) Close() {
	l.eventInserter.Close(context.Background())
	l.effectInserter.Close(context.Background())
}
