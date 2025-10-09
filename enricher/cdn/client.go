package cdn

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/bluesky-social/go-util/pkg/robusthttp"
	"github.com/bluesky-social/osprey-atproto/enricher/metrics"
	"github.com/carlmjohnson/versioninfo"
	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/time/rate"
)

const service = "cdn"

var tracer = otel.Tracer(service)

type Client struct {
	client  *http.Client
	host    string
	limiter *rate.Limiter
	cache   *lru.LRU[string, []byte]
}

type ClientArgs struct {
	Host      string
	CacheSize int
	CacheTTL  time.Duration
}

func NewClient(args *ClientArgs) *Client {
	var cache *lru.LRU[string, []byte]
	if args.CacheSize > 0 {
		cache = lru.NewLRU[string, []byte](args.CacheSize, nil, args.CacheTTL)
	}

	c := robusthttp.NewClient()

	return &Client{
		host:    args.Host,
		client:  c,
		limiter: rate.NewLimiter(100, 50),
		cache:   cache,
	}
}

// Scan sends an image to the ToxicCheck service for OCR and returns the recognized text.
func (c *Client) GetImageBytes(ctx context.Context, did, cid string) ([]byte, error) {
	ctx, span := tracer.Start(ctx, "Cdn.GetImageBytes")
	defer span.End()

	span.SetAttributes(
		attribute.String("did", did),
		attribute.String("cid", cid),
	)

	cacheKey := fmt.Sprintf("%s/%s", did, cid)
	cached, ok := c.cache.Get(cacheKey)
	if ok {
		return cached, nil
	}

	if err := c.limiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("failed to wait on rate limiter: %w", err)
	}
	span.AddEvent("rate limit allowed")

	ustr := fmt.Sprintf("%s/img/feed_thumbnail/plain/%s/%s@jpeg", c.host, did, cid)
	req, err := http.NewRequestWithContext(ctx, "GET", ustr, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", "tango-enricher/"+versioninfo.Short())

	start := time.Now()
	status := "error"
	defer func() {
		duration := time.Since(start)
		metrics.APIDuration.WithLabelValues(service, status).Observe(duration.Seconds())
	}()

	res, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %v", err)
	}
	defer res.Body.Close()
	respBytes, bodyReadErr := io.ReadAll(res.Body)

	if res.StatusCode != 200 {
		return nil, fmt.Errorf("request failed statusCode=%d", res.StatusCode)
	}
	if bodyReadErr != nil {
		return nil, fmt.Errorf("failed to read resp body: %v", bodyReadErr)
	}

	c.cache.Add(cacheKey, respBytes)

	status = "ok"
	return respBytes, nil
}
