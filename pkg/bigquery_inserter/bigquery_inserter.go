package bigqueryinserter

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/api/option"
)

const defaultPendingMaxSends = 100

type BigQueryInserter struct {
	inserter          *bigquery.Inserter
	insertMu          sync.Mutex
	sendMu            sync.Mutex
	queuedEvents      []any
	batchSize         int
	insertsCounter    *prometheus.CounterVec
	pendingSendsGauge prometheus.Gauge
	pendingSends      int
	histogram         *prometheus.HistogramVec
	logger            *slog.Logger
	prefix            string
	maxPendingSends   int
}

type BaseArgs struct {
	CredentialsPath string
	CredentialsJson []byte
	ProjectID       string
	DatasetID       string
	TableID         string
	MaxPendingSends int
}

type Args struct {
	BaseArgs
	BatchSize               int
	PrometheusCounterPrefix string
	Logger                  *slog.Logger
	Histogram               *prometheus.HistogramVec
}

func New(ctx context.Context, args *Args) (*BigQueryInserter, error) {
	if args.Logger == nil {
		args.Logger = slog.Default()
	}

	var bqc *bigquery.Client
	if args.CredentialsPath != "" {
		maybeBqc, err := bigquery.NewClient(ctx, args.ProjectID, option.WithCredentialsFile(args.CredentialsPath))
		if err != nil {
			return nil, err
		}
		bqc = maybeBqc
	} else if args.CredentialsJson != nil {
		maybeBqc, err := bigquery.NewClient(ctx, args.ProjectID, option.WithCredentialsJSON(args.CredentialsJson))
		if err != nil {
			return nil, err
		}
		bqc = maybeBqc
	} else {
		return nil, fmt.Errorf("no credentials passed to bigquery inserter")
	}

	dataset := bqc.Dataset(args.DatasetID)
	table := dataset.Table(args.TableID)
	inserter := table.Inserter()

	if args.MaxPendingSends == 0 {
		args.MaxPendingSends = defaultPendingMaxSends
	}

	bqi := &BigQueryInserter{
		inserter:        inserter,
		histogram:       args.Histogram,
		batchSize:       args.BatchSize,
		logger:          args.Logger,
		prefix:          args.PrometheusCounterPrefix,
		maxPendingSends: args.MaxPendingSends,
	}

	if args.PrometheusCounterPrefix != "" {
		ie := promauto.NewCounterVec(prometheus.CounterOpts{
			Name:      "bigquery_inserts",
			Namespace: args.PrometheusCounterPrefix,
			Help:      "total inserts into bigquery by status",
		}, []string{"status"})

		ps := promauto.NewGauge(prometheus.GaugeOpts{
			Name:      "bigquery_pending_sends",
			Namespace: args.PrometheusCounterPrefix,
			Help:      "total bigquery insertions that are in progress",
		})

		bqi.insertsCounter = ie
		bqi.pendingSendsGauge = ps
	} else {
		args.Logger.Info("no prometheus prefix provided, no metrics will be registered for this counter", "dataset", args.DatasetID, "table", args.TableID)
	}

	return bqi, nil
}

func (i *BigQueryInserter) Insert(ctx context.Context, e any) error {
	i.insertMu.Lock()

	i.queuedEvents = append(i.queuedEvents, e)

	var toInsert []any
	if len(i.queuedEvents) >= i.batchSize {
		toInsert = slices.Clone(i.queuedEvents)
		i.queuedEvents = nil
	}

	i.insertMu.Unlock()

	if len(toInsert) > 0 {
		i.sendStream(ctx, toInsert)
	}

	return nil
}

func (i *BigQueryInserter) Close(ctx context.Context) error {
	i.insertMu.Lock()

	var toInsert []any

	if len(i.queuedEvents) > 0 {
		toInsert = slices.Clone(i.queuedEvents)
		i.queuedEvents = nil
	}

	i.insertMu.Unlock()

	if len(toInsert) > 0 {
		i.sendStream(ctx, toInsert)
	}

	return nil
}

func (i *BigQueryInserter) sendStream(ctx context.Context, toInsert []any) {
	if i.pendingSends >= i.maxPendingSends {
		i.logger.Warn("dropped bigquery insertion due to too many pending sends", "pending-sends", i.pendingSends, "max-pending-sends", i.maxPendingSends)
		return
	}

	i.sendMu.Lock()
	i.pendingSends++
	if i.pendingSendsGauge != nil {
		i.pendingSendsGauge.Inc()
	}
	i.sendMu.Unlock()

	defer func() {
		i.sendMu.Lock()
		i.pendingSends--
		if i.pendingSendsGauge != nil {
			i.pendingSendsGauge.Dec()
		}
		i.sendMu.Unlock()
	}()

	if i.histogram != nil {
		start := time.Now()
		defer func() {
			i.histogram.WithLabelValues(i.prefix).Observe(time.Since(start).Seconds())
		}()
	}

	if len(toInsert) == 0 {
		return
	}

	status := "ok"
	if err := i.inserter.Put(ctx, toInsert); err != nil {
		status = "error"
		i.logger.Error("unable to insert rows into bigquery", "error", err, "rows", len(toInsert), "prefix", i.prefix)
	} else {
		i.logger.Info("successfully inserted rows to bigquery", "count", len(toInsert), "prefix", i.prefix)
	}

	if i.insertsCounter != nil {
		i.insertsCounter.WithLabelValues(status).Add(float64(len(toInsert)))
	}
}
