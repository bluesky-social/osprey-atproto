package flaggedimage

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

const service = "flagged-image-client"

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

type Result struct {
	IsMatch      bool
	Action       string
	ActionLevel  string
	ActionValue  string
	AlwaysReport bool
	Description  string
	Score        float64
}

func NewClient(ctx context.Context, args *ClientArgs) (*Client, error) {
	hasCollection, err := args.Client.HasCollection(ctx, milvusclient.NewHasCollectionOption(args.Collection))
	if err != nil {
		return nil, fmt.Errorf("failed to check if flagged image collection exists: %w", err)
	}

	if !hasCollection {
		flgVecEntity := newFlaggedImageVectorSchema()
		idxParams := []milvusclient.CreateIndexOption{
			milvusclient.NewCreateIndexOption(args.Collection, "vector", index.NewBinIvfFlatIndex(entity.HAMMING, 128)).WithIndexName("flagged_image_vectors_vector_index"),
			milvusclient.NewCreateIndexOption(args.Collection, "timestamp", index.NewSortedIndex()).WithIndexName("flagged_image_vectors_timestamp_index"),
		}
		if err := args.Client.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(args.Collection, flgVecEntity).WithIndexOptions(idxParams...)); err != nil {
			return nil, fmt.Errorf("failed to create flagged image vector collection: %w", err)
		}
		args.Logger.Info("Successfully created flagged image vector collection in Milvus")
	}

	if _, err := args.Client.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(args.Collection)); err != nil {
		return nil, fmt.Errorf("failed to load flagged image collection: %w", err)
	}

	return &Client{
		logger:      args.Logger,
		client:      args.Client,
		minDistance: args.MinDistance,
		collection:  args.Collection,
	}, nil
}

func (c *Client) Scan(ctx context.Context, pdqHash string) (*Result, error) {
	ctx, span := tracer.Start(ctx, "FlaggedImageClient.Scan")
	defer span.End()

	span.SetAttributes(attribute.String("hash", pdqHash))

	bin, err := HexToBinary(pdqHash)
	if err != nil {
		return nil, fmt.Errorf("failed to convert pdq hash to binary vector: %w", err)
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
		WithOutputFields("action", "action_level", "action_value", "always_report", "description").
		WithAnnParam(annSearchParams))
	if err != nil {
		return nil, fmt.Errorf("failed to search for flagged image vectors: %w", err)
	}

	if len(resultSets) == 0 || len(resultSets[0].Scores) != 1 {
		return &Result{IsMatch: false}, nil
	}

	score := resultSets[0].Scores[0]
	action, err := resultSets[0].GetColumn("action").GetAsString(0)
	if err != nil {
		return nil, fmt.Errorf("failed to get flagged image action: %w", err)
	}
	actionLevel, err := resultSets[0].GetColumn("action_level").GetAsString(0)
	if err != nil {
		return nil, fmt.Errorf("failed to get flagged image action level: %w", err)
	}
	actionValue, err := resultSets[0].GetColumn("action_value").GetAsString(0)
	if err != nil {
		return nil, fmt.Errorf("failed to get flagged image action value: %w", err)
	}
	alwaysReport, err := resultSets[0].GetColumn("always_report").GetAsBool(0)
	if err != nil {
		return nil, fmt.Errorf("failed to get flagged image always report setting: %w", err)
	}
	description, err := resultSets[0].GetColumn("description").GetAsString(0)
	if err != nil {
		return nil, fmt.Errorf("failed to get flagged image description: %w", err)
	}

	status = "ok"

	return &Result{
		IsMatch:      true,
		Action:       action,
		ActionLevel:  actionLevel,
		ActionValue:  actionValue,
		AlwaysReport: alwaysReport,
		Description:  description,
		Score:        float64(score),
	}, nil
}

func HexToBinary(input string) ([]byte, error) {
	hashb, err := hex.DecodeString(input)
	if err != nil {
		return nil, err
	}
	return hashb, nil
}

func newFlaggedImageVectorSchema() *entity.Schema {
	return entity.NewSchema().WithDynamicFieldEnabled(false).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithIsAutoID(true)).
		WithField(entity.NewField().WithName("action").WithDataType(entity.FieldTypeVarChar).WithMaxLength(100).WithNullable(false)).
		WithField(entity.NewField().WithName("action_level").WithDataType(entity.FieldTypeVarChar).WithMaxLength(100).WithNullable(false)).
		WithField(entity.NewField().WithName("action_value").WithDataType(entity.FieldTypeVarChar).WithMaxLength(100).WithNullable(false)).
		WithField(entity.NewField().WithName("always_report").WithDataType(entity.FieldTypeBool).WithNullable(false)).
		WithField(entity.NewField().WithName("description").WithDataType(entity.FieldTypeVarChar).WithMaxLength(1000).WithNullable(false)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeBinaryVector).WithDim(256).WithNullable(false)).
		WithField(entity.NewField().WithName("timestamp").WithDataType(entity.FieldTypeInt64).WithNullable(false))
}
