package appview

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/bluesky-social/go-util/pkg/robusthttp"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/bluesky-social/osprey-atproto/enricher/metrics"
	"github.com/carlmjohnson/versioninfo"
	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/time/rate"
)

const service = "appview"

var tracer = otel.Tracer(service)

type Client struct {
	xrpcc   *xrpc.Client
	Limiter *rate.Limiter
	cache   *lru.LRU[string, *bsky.ActorDefs_ProfileViewDetailed]
}

// NewClient creates a new Appview RepoViewDetail client with the given cache size
// If the cache size is zero, the cache is disabled
func NewClient(host, ratelimitBypass string, cacheSize int, cacheTTL time.Duration) *Client {
	c := robusthttp.NewClient()
	c.Timeout = 1 * time.Minute

	ua := "tango-enricher/" + versioninfo.Short()

	xrpcc := xrpc.Client{
		Client:    c,
		Host:      host,
		UserAgent: &ua,
		Headers: map[string]string{
			"X-Ratelimit-Bypass": ratelimitBypass,
		},
	}

	var cache *lru.LRU[string, *bsky.ActorDefs_ProfileViewDetailed]
	if cacheSize > 0 {
		cache = lru.NewLRU(cacheSize, func(key string, value *bsky.ActorDefs_ProfileViewDetailed) {
			metrics.CacheSize.WithLabelValues(service).Dec()
		}, cacheTTL)
	}

	return &Client{
		xrpcc:   &xrpcc,
		Limiter: rate.NewLimiter(500, 100),
		cache:   cache,
	}
}

// GetProfile fetches a ProfileViewDetailed from the Appview
func (c *Client) GetProfile(ctx context.Context, did string) ([]byte, *bsky.ActorDefs_ProfileViewDetailed, error) {
	ctx, span := tracer.Start(ctx, "AppviewClient.GetProfile")
	defer span.End()

	span.SetAttributes(attribute.String("did", did))

	if c.cache != nil {
		if val, ok := c.cache.Get(did); ok {
			metrics.CacheResults.WithLabelValues(service, "hit").Inc()
			span.AddEvent("cache hit")
			asBytes, err := json.Marshal(val)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to marshal cached profile view: %w", err)
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

	profileView, err := bsky.ActorGetProfile(ctx, c.xrpcc, did)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get profile view: %w", err)
	}

	if profileView == nil {
		return nil, nil, fmt.Errorf("profile not found (empty response)")
	}

	// Cache the result
	if c.cache != nil {
		c.cache.Add(did, profileView)
		metrics.CacheSize.WithLabelValues(service).Inc()
	}

	asBytes, err := json.Marshal(profileView)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal profile view: %w", err)
	}

	status = "success"
	return asBytes, profileView, nil
}
