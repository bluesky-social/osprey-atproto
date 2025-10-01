package did

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/bluesky-social/go-util/pkg/robusthttp"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/osprey-atproto/enricher/metrics"
	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/time/rate"
)

const (
	service      = "did"
	docService   = "did-doc"
	auditService = "did-audit"
)

var tracer = otel.Tracer(service)

type Client struct {
	httpCli    *http.Client
	plcHost    string
	dir        *identity.BaseDirectory
	docCache   *lru.LRU[string, *identity.DIDDocument]
	auditCache *lru.LRU[string, *AuditLog]
}

type OperationService struct {
	Type     string `json:"type"`
	Endpoint string `json:"endpoint"`
}

type DidLogEntry struct {
	Sig                 string                      `json:"sig"`
	Prev                *string                     `json:"prev"`
	Type                string                      `json:"type"`
	Services            map[string]OperationService `json:"services"`
	AlsoKnownAs         []string                    `json:"alsoKnownAs"`
	RotationKeys        []string                    `json:"rotationKeys"`
	VerificationMethods map[string]string           `json:"verificationMethods"`
}

type DidAuditEntry struct {
	Did       string      `json:"did"`
	Operation DidLogEntry `json:"operation"`
	Cid       string      `json:"cid"`
	Nullified bool        `json:"nullified"`
	CreatedAt string      `json:"createdAt"`
}

type DidAuditLog []DidAuditEntry

type AuditLog struct {
	Entries []DidAuditEntry `json:"entries"`
}

// NewClient creates a new DID Doc client with the given cache size
// If the cache size is zero, the cache is disabled
func NewClient(plcHost string, docCacheSize int, docCacheTTL time.Duration, auditCacheSize int, auditCacheTTL time.Duration) *Client {
	c := robusthttp.NewClient(robusthttp.WithMaxRetries(1))
	c.Timeout = 5 * time.Second

	baseDir := identity.BaseDirectory{
		PLCURL:     plcHost,
		PLCLimiter: rate.NewLimiter(rate.Limit(200), 100),
		HTTPClient: *c,
		Resolver: net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				dialer := net.Dialer{Timeout: time.Second * 5}
				nameserver := address
				return dialer.DialContext(ctx, network, nameserver)
			},
		},
		TryAuthoritativeDNS: true,
		// primary Bluesky PDS instance only supports HTTP resolution method
		SkipDNSDomainSuffixes: []string{".bsky.social"},
	}

	var docCache *lru.LRU[string, *identity.DIDDocument]
	if docCacheSize > 0 {
		docCache = lru.NewLRU(docCacheSize, func(key string, value *identity.DIDDocument) {
			metrics.CacheSize.WithLabelValues(docService).Dec()
		}, docCacheTTL)
	}

	var auditCache *lru.LRU[string, *AuditLog]
	if auditCacheSize > 0 {
		auditCache = lru.NewLRU(auditCacheSize, func(key string, value *AuditLog) {
			metrics.CacheSize.WithLabelValues(auditService).Dec()
		}, auditCacheTTL)
	}

	return &Client{httpCli: c, plcHost: plcHost, dir: &baseDir, docCache: docCache, auditCache: auditCache}
}

// GetDIDDoc fetches a DID Doc for the DID
func (c *Client) GetDIDDoc(ctx context.Context, did string) ([]byte, *identity.DIDDocument, error) {
	ctx, span := tracer.Start(ctx, "DidClient.GetDIDDoc")
	defer span.End()

	span.SetAttributes(attribute.String("did", did))

	if c.docCache != nil {
		if val, ok := c.docCache.Get(did); ok {
			metrics.CacheResults.WithLabelValues(docService, "hit").Inc()
			span.AddEvent("cache hit")
			asBytes, err := json.Marshal(val)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to marshal cached DID Document: %w", err)
			}
			return asBytes, val, nil
		}
		metrics.CacheResults.WithLabelValues(docService, "miss").Inc()
		span.AddEvent("cache miss")
	}

	start := time.Now()
	status := "error"
	defer func() {
		duration := time.Since(start)
		metrics.APIDuration.WithLabelValues(docService, status).Observe(duration.Seconds())
	}()

	didDoc, err := c.dir.ResolveDID(ctx, syntax.DID(did))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to lookup DID: %w", err)
	}

	if didDoc == nil {
		return nil, nil, fmt.Errorf("DID Document not found")
	}

	// Cache the result
	if c.docCache != nil {
		c.docCache.Add(did, didDoc)
		metrics.CacheSize.WithLabelValues(docService).Inc()
	}

	asBytes, err := json.Marshal(didDoc)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal DID Document: %w", err)
	}

	status = "success"
	return asBytes, didDoc, nil
}

var ErrAuditLogNotFound = errors.New("audit log not found for DID")

// GetDIDAuditLog fetches the DID audit log for the DID from PLC Directory
func (c *Client) GetDIDAuditLog(ctx context.Context, did string) ([]byte, *AuditLog, error) {
	ctx, span := tracer.Start(ctx, "DidClient.GetDIDAuditLog")
	defer span.End()

	span.SetAttributes(attribute.String("did", did))

	if c.auditCache != nil {
		if val, ok := c.auditCache.Get(did); ok {
			metrics.CacheResults.WithLabelValues(auditService, "hit").Inc()
			span.AddEvent("cache hit")
			asBytes, err := json.Marshal(val)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to marshal cached DID audit log: %w", err)
			}
			return asBytes, val, nil
		}
		metrics.CacheResults.WithLabelValues(auditService, "miss").Inc()
		span.AddEvent("cache miss")
	}

	start := time.Now()
	status := "error"
	defer func() {
		duration := time.Since(start)
		metrics.APIDuration.WithLabelValues(auditService, status).Observe(duration.Seconds())
	}()

	ustr := fmt.Sprintf("%s/%s/log/audit", c.plcHost, did)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ustr, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create http request for DID audit log: %w", err)
	}

	resp, err := c.httpCli.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch DID audit log: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		if resp.StatusCode == http.StatusNotFound {
			status = "not_found"
			return nil, nil, ErrAuditLogNotFound
		}
		return nil, nil, fmt.Errorf("DID audit log fetch returned unexpected status: %s", resp.Status)
	}

	var entries DidAuditLog
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return nil, nil, fmt.Errorf("failed to read DID audit log response bytes into DidAuditLog: %w", err)
	}

	auditLog := AuditLog{Entries: entries}

	if c.auditCache != nil {
		c.auditCache.Add(did, &auditLog)
		metrics.CacheSize.WithLabelValues(auditService).Inc()
	}

	bytes, err := json.Marshal(auditLog)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal AuditLog: %w", err)
	}

	status = "success"

	return bytes, &auditLog, nil
}
