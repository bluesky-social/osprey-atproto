package enricher

import (
	"context"
	"log/slog"

	"github.com/bluesky-social/go-util/pkg/bus/consumer"
	"github.com/bluesky-social/go-util/pkg/bus/producer"
	"github.com/bluesky-social/osprey-atproto/effector"
	osprey "github.com/bluesky-social/osprey-atproto/proto/go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	NAMESPACE = "osprey_effector"
)

var (
	eventsReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "firehose_events_received",
		Namespace: NAMESPACE,
		Help:      "number of events received from the firehose, by collection",
	}, []string{"collection"})

	eventsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "events_produced",
		Namespace: NAMESPACE,
		Help:      "number of events produced, by collection and status",
	}, []string{"collection", "status"})

	ozoneRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "ozone_requests",
		Namespace: NAMESPACE,
		Help:      "number of requests to Ozone",
	}, []string{"type", "kind", "status"})

	plcRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "plc_requests",
		Namespace: NAMESPACE,
		Help:      "number of requests to PLC",
	}, []string{"status"})
)

type OspreyEnricher struct {
	logger *slog.Logger

	consumer *consumer.Consumer[]
	producer *producer.Producer[*osprey.ResultEvent]

	ozoneClient *effector.OzoneClient
}

type Args struct {
	Logger *slog.Logger
}

func NewOspreyEnricher(args *Args) (*OspreyEnricher, error) {
	return &OspreyEnricher{
		logger: args.Logger,
	}, nil
}

func (e *OspreyEnricher) Run(ctx context.Context) {

}
