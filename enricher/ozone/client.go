package ozone

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/bluesky-social/go-util/pkg/robusthttp"
	"github.com/bluesky-social/indigo/api/ozone"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/bluesky-social/osprey-atproto/enricher/metrics"
	"github.com/carlmjohnson/versioninfo"
	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/time/rate"
)

const service = "ozone"

var tracer = otel.Tracer(service)

type Client struct {
	xrpcc   *xrpc.Client
	Limiter *rate.Limiter
	cache   *lru.LRU[string, *ozone.ModerationDefs_RepoViewDetail]
}

// NewClient creates a new Ozone RepoViewDetail client with the given cache size
// If the cache size is zero, the cache is disabled
func NewClient(host, adminToken string, cacheSize int, cacheTTL time.Duration) *Client {
	c := robusthttp.NewClient()
	c.Timeout = 1 * time.Minute

	ua := "tango-enricher/" + versioninfo.Short()

	xrpcc := xrpc.Client{
		Client:     c,
		AdminToken: &adminToken,
		Host:       host,
		UserAgent:  &ua,
	}

	var cache *lru.LRU[string, *ozone.ModerationDefs_RepoViewDetail]
	if cacheSize > 0 {
		cache = lru.NewLRU(cacheSize, func(key string, value *ozone.ModerationDefs_RepoViewDetail) {
			metrics.CacheSize.WithLabelValues(service).Dec()
		}, cacheTTL)
	}

	return &Client{
		xrpcc:   &xrpcc,
		Limiter: rate.NewLimiter(100, 10),
		cache:   cache,
	}
}

// GetRepoView fetches a RepoViewDetail from Ozone
func (c *Client) GetRepoView(ctx context.Context, did string) ([]byte, *ozone.ModerationDefs_RepoViewDetail, error) {
	ctx, span := tracer.Start(ctx, "OzoneClient.GetRepoView")
	defer span.End()

	span.SetAttributes(attribute.String("did", did))

	if c.cache != nil {
		if val, ok := c.cache.Get(did); ok {
			metrics.CacheResults.WithLabelValues(service, "hit").Inc()
			span.AddEvent("cache hit")
			asBytes, err := json.Marshal(val)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to marshal cached repo view detail: %w", err)
			}
			return asBytes, val, nil
		}
		metrics.CacheResults.WithLabelValues(service, "miss").Inc()
		span.AddEvent("cache miss")
	}

	if err := c.Limiter.Wait(ctx); err != nil {
		return nil, nil, fmt.Errorf("failed to wait on rate limiter: %w", err)
	}
	span.AddEvent("rate limit allowed")

	start := time.Now()
	status := "error"
	defer func() {
		duration := time.Since(start)
		metrics.APIDuration.WithLabelValues(service, status).Observe(duration.Seconds())
	}()

	repoViewDetail, err := ozone.ModerationGetRepo(ctx, c.xrpcc, did)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get repo view: %w", err)
	}

	if repoViewDetail == nil {
		return nil, nil, fmt.Errorf("repo not found (empty response)")
	}

	// Cache the result
	if c.cache != nil {
		c.cache.Add(did, repoViewDetail)
		metrics.CacheSize.WithLabelValues(service).Inc()
	}

	asBytes, err := json.Marshal(repoViewDetail)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal repo view detail: %w", err)
	}

	status = "success"
	return asBytes, repoViewDetail, nil
}
