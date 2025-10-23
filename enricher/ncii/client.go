package ncii

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"github.com/bluesky-social/osprey-atproto/enricher/metrics"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

const service = "ncii-client"

var tracer = otel.Tracer(service)

type Client struct {
	logger      *slog.Logger
	client      *milvusclient.Client
	minDistance float64
	collection  string
}

type ClientArgs struct {
	Logger      *slog.Logger
	Client      *milvusclient.Client
	MinDistance float64
	Collection  string
}

func NewClient(ctx context.Context, args *ClientArgs) (*Client, error) {
	if _, err := args.Client.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(args.Collection)); err != nil {
		return nil, fmt.Errorf("failed to load ncii collection: %w", err)
	}

	return &Client{
		logger:      args.Logger,
		client:      args.Client,
		minDistance: args.MinDistance,
		collection:  args.Collection,
	}, nil
}

func (c *Client) Scan(ctx context.Context, pdqHash string) (bool, float64, error) {
	ctx, span := tracer.Start(ctx, "NciiClient.Scan")
	defer span.End()

	span.SetAttributes(attribute.String("hash", pdqHash))

	bin, err := HexToBinary(pdqHash)
	if err != nil {
		return false, 0, fmt.Errorf("failed to convert pdq hash to binary vector: %w", err)
	}

	span.AddEvent("hash converted to binary")

	start := time.Now()
	status := "error"

	defer func() {
		duration := time.Since(start)
		metrics.APIDuration.WithLabelValues(service, status).Observe(duration.Seconds())
	}()

	annSearchParams := index.NewCustomAnnParam()
	annSearchParams.WithRadius(c.minDistance)
	annSearchParams.WithRangeFilter(0)
	annSearchParams.WithExtraParam("nprobe", 10)
	resultSets, err := c.client.Search(ctx, milvusclient.NewSearchOption(
		c.collection,
		1,
		[]entity.Vector{entity.BinaryVector(bin)},
	).WithANNSField("vector").
		WithFilter("status like 'Active'").
		WithOutputFields("status").
		WithAnnParam(annSearchParams))
	if err != nil {
		return false, 0, fmt.Errorf("failed to search for ncii vectors: %w", err)
	}

	if len(resultSets) == 0 || len(resultSets[0].Scores) != 1 {
		return false, 0, nil
	}

	score := resultSets[0].Scores[0]
	hashStatus, err := resultSets[0].GetColumn("status").GetAsString(0)
	if err != nil {
		return false, 0, fmt.Errorf("failed to get ncii hash status: %w", err)
	}

	status = "ok"

	if hashStatus != "Active" {
		return false, 0, nil
	}

	return true, float64(score), nil
}

func HexToBinary(input string) ([]byte, error) {
	hashb, err := hex.DecodeString(input)
	if err != nil {
		return nil, err
	}
	return hashb, nil
}
