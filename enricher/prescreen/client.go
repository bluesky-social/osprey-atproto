package prescreen

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"time"

	"github.com/bluesky-social/go-util/pkg/robusthttp"
	"github.com/bluesky-social/osprey-atproto/enricher/metrics"
	"github.com/carlmjohnson/versioninfo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/time/rate"
)

const service = "prescreen"

var tracer = otel.Tracer(service)

type Client struct {
	Client  *http.Client
	Host    string
	Limiter *rate.Limiter
}

func NewClient(host string) *Client {
	c := robusthttp.NewClient()
	c.Timeout = 1 * time.Minute
	return &Client{
		Client:  c,
		Host:    host,
		Limiter: rate.NewLimiter(100, 10),
	}
}

type ClassScore struct {
	Class string  `json:"class"`
	Score float64 `json:"score"`
}

type Resp struct {
	Result      string       `json:"result"`
	ClassScores []ClassScore `json:"class_scores"`
}

// Scan sends an image to the prescreen service for classification.
// It returns a string with the decided class of the image and a byte slice of the raw response.
func (c *Client) Scan(ctx context.Context, did string, imageBytes []byte) (string, []byte, error) {
	ctx, span := tracer.Start(ctx, "PrescreenClient.Scan")
	defer span.End()

	span.SetAttributes(
		attribute.String("did", did),
		attribute.Int("blob_size", len(imageBytes)),
	)

	if err := c.Limiter.Wait(ctx); err != nil {
		return "", nil, fmt.Errorf("failed to wait on rate limiter: %w", err)
	}
	span.AddEvent("rate limit allowed")

	// generic HTTP form file upload, then parse the response JSON
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("media", "image.jpg")
	if err != nil {
		return "", nil, fmt.Errorf("failed to create multipart writer: %v", err)
	}
	_, err = part.Write(imageBytes)
	if err != nil {
		return "", nil, fmt.Errorf("failed to write image to multipart writer: %v", err)
	}
	err = writer.Close()
	if err != nil {
		return "", nil, fmt.Errorf("failed to close multipart writer: %v", err)
	}

	req, err := http.NewRequest("POST", c.Host+"/predict", body)
	if err != nil {
		return "", nil, err
	}

	start := time.Now()
	status := "error"
	defer func() {
		duration := time.Since(start)
		metrics.APIDuration.WithLabelValues(service, status).Observe(duration.Seconds())
	}()

	req.Header.Add("Content-Type", writer.FormDataContentType())
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "tango-enricher/"+versioninfo.Short())

	res, err := c.Client.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("request failed: %v", err)
	}
	defer res.Body.Close()
	respBytes, bodyReadErr := io.ReadAll(res.Body)

	if res.StatusCode != 200 {
		return "", nil, fmt.Errorf("request failed statusCode=%d", res.StatusCode)
	}
	if bodyReadErr != nil {
		return "", nil, fmt.Errorf("failed to read resp body: %v", bodyReadErr)
	}

	var respObj Resp
	if err := json.Unmarshal(respBytes, &respObj); err != nil {
		return "", nil, fmt.Errorf("failed to parse resp JSON: %v", err)
	}
	status = "ok"

	return respObj.Result, respBytes, nil
}
