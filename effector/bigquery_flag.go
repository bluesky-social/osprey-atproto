package effector

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	bigqueryinserter "github.com/bluesky-social/osprey-atproto/pkg/bigquery_inserter"
)

type BigQueryFlag struct {
	Uri        string    `bigquery:"uri"`
	Did        string    `bigquery:"did"`
	Collection string    `bigquery:"collection"`
	Rkey       string    `bigquery:"rkey"`
	Tag        string    `bigquery:"tag"`
	CreatedAt  time.Time `bigquery:"created_at"`
}

type BigQueryFlagClient struct {
	inserter *bigqueryinserter.BigQueryInserter
	logger   *slog.Logger
}

type BigQueryFlagClientArgs struct {
	CredentialsJson []byte
	ProjectID       string
	DatasetID       string
	Logger          *slog.Logger
}

func NewBigQueryFlagClient(args *BigQueryFlagClientArgs) (*BigQueryFlagClient, error) {
	logger := args.Logger.With("component", "bigquery-flags")

	inserter, err := bigqueryinserter.New(context.Background(), &bigqueryinserter.Args{
		BaseArgs: bigqueryinserter.BaseArgs{
			CredentialsJson: args.CredentialsJson,
			ProjectID:       args.ProjectID,
			DatasetID:       args.DatasetID,
			TableID:         "tagged_posts",
			MaxPendingSends: 100,
		},
		// We send _all_ events to BigQuery, so we probably don't want this to be anything under the average number of events we process per second
		BatchSize: 1,
		Logger:    logger,
	})
	if err != nil {
		return nil, err
	}

	return &BigQueryFlagClient{
		inserter: inserter,
		logger:   logger,
	}, nil
}

func (bfc *BigQueryFlagClient) addFlag(ctx context.Context, uri string, tag string) error {
	status := "error"

	defer func() {
		effectsProcessed.WithLabelValues("bigquery-flag", status).Inc()
	}()

	aturi, err := syntax.ParseATURI(uri)
	if err != nil {
		return fmt.Errorf("could not parse uri passed to bigquery flag: %w", err)
	}

	did := aturi.Authority().String()
	collection := aturi.Collection().String()
	rkey := aturi.RecordKey().String()

	flag := BigQueryFlag{
		Uri:        uri,
		Did:        did,
		Collection: collection,
		Rkey:       rkey,
		Tag:        tag,
		CreatedAt:  time.Now(),
	}

	if err := bfc.inserter.Insert(ctx, flag); err != nil {
		bfc.logger.Error("error adding insert for bigquery flag", "error", err)
		return err
	}

	status = "ok"

	return nil
}
