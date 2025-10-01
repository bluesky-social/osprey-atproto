package abyss

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/bluesky-social/go-util/pkg/robusthttp"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/osprey-atproto/enricher/metrics"
	"github.com/carlmjohnson/versioninfo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/time/rate"
)

const service = "abyss"

var tracer = otel.Tracer(service)

type Client struct {
	Client   *http.Client
	Host     string
	Password string
	Limiter  *rate.Limiter
}

func NewClient(host, password string) *Client {
	c := robusthttp.NewClient()
	c.Timeout = 1 * time.Minute
	return &Client{
		Client:   c,
		Host:     host,
		Password: password,
		Limiter:  rate.NewLimiter(100, 10),
	}
}

type Resp struct {
	Blob     *lexutil.LexBlob `json:"blob"`
	Match    *MatchResult     `json:"match,omitempty"`
	Classify *ClassifyResult  `json:"classify,omitempty"`
	Review   *ReviewState     `json:"review,omitempty"`
}

type MatchResult struct {
	Status string     `json:"status"`
	Hits   []MatchHit `json:"hits"`
}

type MatchHit struct {
	HashType  string `json:"hashType,omitempty"`
	HashValue string `json:"hashValue,omitempty"`
	Label     string `json:"label,omitempty"`
	// TODO: Corpus
}

type ClassifyResult struct {
	// TODO
}

type ReviewState struct {
	State    string `json:"state,omitempty"`
	TicketID string `json:"ticketId,omitempty"`
}

func (amr *MatchResult) IsAbuseMatch() bool {
	for _, hit := range amr.Hits {
		if hit.Label == "csam" || hit.Label == "csem" {
			return true
		}
	}
	return false
}

// Scan sends an image to the Abyss service for classification.
// It returns the raw response bytes and a boolean indicating if it is an abuse match.
func (c *Client) Scan(ctx context.Context, did string, imageBytes []byte) ([]byte, bool, error) {
	ctx, span := tracer.Start(ctx, "AbyssClient.Scan")
	defer span.End()

	span.SetAttributes(
		attribute.String("did", did),
		attribute.Int("blob_size", len(imageBytes)),
	)

	if err := c.Limiter.Wait(ctx); err != nil {
		return nil, false, fmt.Errorf("failed to wait on rate limiter: %w", err)
	}
	span.AddEvent("rate limit allowed")

	req, err := http.NewRequestWithContext(ctx, "POST", c.Host+"/xrpc/com.atproto.unspecced.scanBlob", bytes.NewReader(imageBytes))
	if err != nil {
		return nil, false, err
	}

	// Add the DID to the request query
	q := req.URL.Query()
	if did != "" {
		q.Add("did", did)
	}
	req.URL.RawQuery = q.Encode()

	req.SetBasicAuth("admin", c.Password)
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
		return nil, false, fmt.Errorf("request failed: %v", err)
	}
	defer res.Body.Close()
	respBytes, bodyReadErr := io.ReadAll(res.Body)

	if res.StatusCode != 200 {
		return nil, false, fmt.Errorf("request failed statusCode=%d", res.StatusCode)
	}
	if bodyReadErr != nil {
		return nil, false, fmt.Errorf("failed to read resp body: %v", bodyReadErr)
	}

	var respObj Resp
	if err := json.Unmarshal(respBytes, &respObj); err != nil {
		return nil, false, fmt.Errorf("failed to parse resp JSON: %v", err)
	}
	status = "ok"

	isMatch := respObj.Match != nil && respObj.Match.IsAbuseMatch()

	return respBytes, isMatch, nil
}
