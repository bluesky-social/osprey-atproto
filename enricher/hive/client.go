package hive

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

const service = "hive"

var tracer = otel.Tracer(service)

type Client struct {
	Client       *http.Client
	ApiToken     string
	scanEndpoint string
	Limiter      *rate.Limiter
}

// schema: https://docs.thehive.ai/reference/classification
type Resp struct {
	Status []RespStatus `json:"status"`
}

type RespStatus struct {
	Response RespResponse `json:"response"`
}

type RespResponse struct {
	Output []RespOut `json:"output"`
}

type RespOut struct {
	Time    float64     `json:"time"`
	Classes []RespClass `json:"classes"`
}

type RespClass struct {
	Class string  `json:"class"`
	Score float64 `json:"score"`
}

func NewClient(token string) *Client {
	c := robusthttp.NewClient()
	return &Client{
		Client:       c,
		ApiToken:     token,
		scanEndpoint: "https://api.thehive.ai/api/v2/task/sync",
		Limiter:      rate.NewLimiter(100, 10),
	}
}

func (c *Client) Scan(ctx context.Context, imageBytes []byte) ([]byte, map[string]float64, error) {
	ctx, span := tracer.Start(ctx, "HiveAIClient.Scan")
	defer span.End()

	span.SetAttributes(
		attribute.Int("image_size", len(imageBytes)),
	)

	if err := c.Limiter.Wait(ctx); err != nil {
		return nil, nil, fmt.Errorf("failed to wait on rate limiter: %w", err)
	}
	span.AddEvent("rate limit allowed")

	// generic HTTP form file upload, then parse the response JSON
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("media", "image.jpg")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create multipart writer: %v", err)
	}
	_, err = part.Write(imageBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to write image to multipart writer: %v", err)
	}
	err = writer.Close()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to close multipart writer: %v", err)
	}

	req, err := http.NewRequest("POST", c.scanEndpoint, body)
	if err != nil {
		return nil, nil, err
	}

	start := time.Now()
	status := "error"
	defer func() {
		duration := time.Since(start)
		metrics.APIDuration.WithLabelValues(service, status).Observe(duration.Seconds())
	}()

	req.Header.Set("Authorization", fmt.Sprintf("Token %s", c.ApiToken))
	req.Header.Add("Content-Type", writer.FormDataContentType())
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "tango-enricher/"+versioninfo.Short())

	req = req.WithContext(ctx)
	res, err := c.Client.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("request failed: %v", err)
	}
	defer res.Body.Close()
	respBytes, bodyReadErr := io.ReadAll(res.Body)

	if res.StatusCode != 200 {
		return nil, nil, fmt.Errorf("request failed  statusCode=%d", res.StatusCode)
	}

	if bodyReadErr != nil {
		return nil, nil, fmt.Errorf("failed to read resp body: %v", bodyReadErr)
	}

	var respObj Resp
	if err := json.Unmarshal(respBytes, &respObj); err != nil {
		return nil, nil, fmt.Errorf("failed to parse resp JSON: %v", err)
	}

	classes := make(map[string]float64)
	for _, status := range respObj.Status {
		for _, out := range status.Response.Output {
			for _, class := range out.Classes {
				classes[class.Class] = class.Score
			}
		}
	}

	status = "ok"
	return respBytes, classes, nil
}
