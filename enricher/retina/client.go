package retina

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

const service = "retina"

var tracer = otel.Tracer(service)

type Client struct {
	Client  *http.Client
	Host    string
	APIKey  string
	Limiter *rate.Limiter
}

func NewRetinaClient(host, apiKey string) *Client {
	// disable keepalives and idle conns to more evenly distribute load across retina instances
	c := robusthttp.NewClient(robusthttp.WithTransport(cleanhttp.DefaultTransport()))
	return &Client{
		Client:  c,
		Host:    host,
		APIKey:  apiKey,
		Limiter: rate.NewLimiter(100, 10),
	}
}

type Resp struct {
	Text  string `json:"text"`
	Error string `json:"error,omitempty"`
}

// Scan sends an image to the Retina service for OCR and returns the recognized text.
func (c *Client) Scan(ctx context.Context, did, cid string, imageBytes []byte) ([]byte, string, error) {
	ctx, span := tracer.Start(ctx, "RetinaClient.Scan")
	defer span.End()

	span.SetAttributes(
		attribute.String("did", did),
		attribute.String("cid", cid),
		attribute.Int("blob_size", len(imageBytes)),
	)

	if err := c.Limiter.Wait(ctx); err != nil {
		return nil, "", fmt.Errorf("failed to wait on rate limiter: %w", err)
	}
	span.AddEvent("rate limit allowed")

	req, err := http.NewRequestWithContext(ctx, "POST", c.Host+"/api/analyze_blob", bytes.NewReader(imageBytes))
	if err != nil {
		return nil, "", err
	}

	// Add the DID to the request query
	q := req.URL.Query()
	if did != "" && cid != "" {
		q.Add("did", did)
		q.Add("cid", cid)
	}
	req.URL.RawQuery = q.Encode()

	req.Header.Add("Content-Type", "image/jpeg")
	req.Header.Add("Content-Length", fmt.Sprintf("%d", len(imageBytes)))
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "tango-enricher/"+versioninfo.Short())
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.APIKey))

	start := time.Now()
	status := "error"
	defer func() {
		duration := time.Since(start)
		metrics.APIDuration.WithLabelValues(service, status).Observe(duration.Seconds())
	}()

	res, err := c.Client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("request failed: %v", err)
	}
	defer res.Body.Close()
	respBytes, bodyReadErr := io.ReadAll(res.Body)

	if res.StatusCode != 200 {
		return nil, "", fmt.Errorf("request failed statusCode=%d", res.StatusCode)
	}
	if bodyReadErr != nil {
		return nil, "", fmt.Errorf("failed to read resp body: %v", bodyReadErr)
	}

	var respObj Resp
	if err := json.Unmarshal(respBytes, &respObj); err != nil {
		return nil, "", fmt.Errorf("failed to parse resp JSON: %v", err)
	}
	status = "ok"
	return respBytes, respObj.Text, nil
}
