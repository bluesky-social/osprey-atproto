package retinahash

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/bluesky-social/go-util/pkg/robusthttp"
	"github.com/bluesky-social/osprey-atproto/enricher/metrics"
	"github.com/carlmjohnson/versioninfo"
	"github.com/hashicorp/go-cleanhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/time/rate"
)

const service = "retina-hash"

var tracer = otel.Tracer(service)

type Client struct {
	Client  *http.Client
	Host    string
	Limiter *rate.Limiter
}

func NewClient(host string) *Client {
	// disable keepalives and idle conns to more evenly distribute load across retina instances
	c := robusthttp.NewClient(robusthttp.WithTransport(cleanhttp.DefaultTransport()))
	return &Client{
		Client:  c,
		Host:    host,
		Limiter: rate.NewLimiter(100, 10),
	}
}

type Resp struct {
	Hash          string `json:"hash"`
	QualityTooLow bool   `json:"qualityTooLow"`
}

func (c *Client) Hash(ctx context.Context, did, cid string, imageBytes []byte) ([]byte, *Resp, error) {
	ctx, span := tracer.Start(ctx, "RetinaClient.Hash")
	defer span.End()

	span.SetAttributes(
		attribute.String("did", did),
		attribute.String("cid", cid),
		attribute.Int("blob_size", len(imageBytes)),
	)

	if err := c.Limiter.Wait(ctx); err != nil {
		return nil, nil, fmt.Errorf("failed to wait on rate limiter: %w", err)
	}
	span.AddEvent("rate limit allowed")

	req, err := http.NewRequestWithContext(ctx, "POST", c.Host+"/api/hash_blob", bytes.NewReader(imageBytes))
	if err != nil {
		return nil, nil, err
	}

	req.Header.Add("Content-Type", "image/jpeg")
	req.Header.Add("Content-Length", fmt.Sprintf("%d", len(imageBytes)))
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "tango-enricher/"+versioninfo.Short())

	start := time.Now()
	status := "error"
	defer func() {
		duration := time.Since(start)
		metrics.APIDuration.WithLabelValues(service, status).Observe(duration.Seconds())
	}()

	res, err := c.Client.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("request failed: %v", err)
	}
	defer res.Body.Close()
	respBytes, bodyReadErr := io.ReadAll(res.Body)

	if res.StatusCode != 200 {
		return nil, nil, fmt.Errorf("request failed statusCode=%d", res.StatusCode)
	}
	if bodyReadErr != nil {
		return nil, nil, fmt.Errorf("failed to read resp body: %v", bodyReadErr)
	}

	var respObj Resp
	if err := json.Unmarshal(respBytes, &respObj); err != nil {
		return nil, nil, fmt.Errorf("failed to parse resp JSON: %v", err)
	}
	status = "ok"
	return respBytes, &respObj, nil
}
