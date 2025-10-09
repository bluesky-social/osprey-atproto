package enricher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bluesky-social/go-util/pkg/bus/consumer"
	"github.com/bluesky-social/go-util/pkg/bus/producer"
	"github.com/bluesky-social/indigo/atproto/data"
	"github.com/bluesky-social/osprey-atproto/enricher/abyss"
	"github.com/bluesky-social/osprey-atproto/enricher/appview"
	"github.com/bluesky-social/osprey-atproto/enricher/cdn"
	"github.com/bluesky-social/osprey-atproto/enricher/did"
	"github.com/bluesky-social/osprey-atproto/enricher/hive"
	"github.com/bluesky-social/osprey-atproto/enricher/ozone"
	"github.com/bluesky-social/osprey-atproto/enricher/prescreen"
	"github.com/bluesky-social/osprey-atproto/enricher/retina"
	osprey "github.com/bluesky-social/osprey-atproto/proto/go"
	"github.com/puzpuzpuz/xsync/v3"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Enricher struct {
	logger          *slog.Logger
	producer        *producer.Producer[*osprey.OspreyInputEvent]
	consumer        *consumer.Consumer[*osprey.FirehoseEvent]
	cdn             *cdn.Client
	abyssClient     *abyss.Client
	hiveClient      *hive.Client
	retinaClient    *retina.Client
	prescreenClient *prescreen.Client
	ozoneClient     *ozone.Client
	appviewClient   *appview.Client
	didClient       *did.Client
}

type Args struct {
	KafkaBootstrapServers  []string
	SASLUsername           string
	SASLPassword           string
	InputTopic             string
	OutputTopic            string
	ImageCdnURL            string
	AbyssURL               string
	AbyssAdminPassword     string
	HiveAPIToken           string
	RetinaURL              string
	RetinaAPIKey           string
	PrescreenHost          string
	OzoneHost              string
	OzoneAdminToken        string
	AppviewHost            string
	AppviewRatelimitBypass string
	PLCHost                string
	Logger                 *slog.Logger
}

func New(ctx context.Context, args *Args) (*Enricher, error) {
	if args.Logger == nil {
		args.Logger = slog.Default()
	}
	logger := args.Logger

	if args.ImageCdnURL == "" {
		return nil, fmt.Errorf("missing image CDN url")
	}

	en := Enricher{
		logger: args.Logger,
		cdn: cdn.NewClient(&cdn.ClientArgs{
			Host: args.ImageCdnURL,
		}),
	}

	if args.AbyssURL != "" {
		abyssClient := abyss.NewClient(args.AbyssURL, args.AbyssAdminPassword)
		en.abyssClient = abyssClient
		logger.Info("initialized Abyss client", "url", args.AbyssURL)
	}
	if args.HiveAPIToken != "" {
		hiveClient := hive.NewClient(args.HiveAPIToken)
		en.hiveClient = hiveClient
		logger.Info("initialized Hive client")
	}
	if args.RetinaURL != "" && args.RetinaAPIKey != "" {
		retinaClient := retina.NewRetinaClient(args.RetinaURL, args.RetinaAPIKey)
		en.retinaClient = retinaClient
		logger.Info("initialized Retina client", "url", args.RetinaURL)
	}
	if args.PrescreenHost != "" {
		prescreenClient := prescreen.NewClient(args.PrescreenHost)
		en.prescreenClient = prescreenClient
		logger.Info("initialized Prescreen client", "host", args.PrescreenHost)
	}
	if args.OzoneHost != "" && args.OzoneAdminToken != "" {
		cacheSize := 50_000
		cacheTTL := time.Minute * 1
		ozoneClient := ozone.NewClient(args.OzoneHost, args.OzoneAdminToken, cacheSize, cacheTTL)
		en.ozoneClient = ozoneClient
		logger.Info("initialized Ozone client", "host", args.OzoneHost)
	}
	if args.AppviewHost != "" {
		cacheSize := 0
		cacheTTL := time.Duration(0)
		appviewClient := appview.NewClient(args.AppviewHost, args.AppviewRatelimitBypass, cacheSize, cacheTTL)
		en.appviewClient = appviewClient
		logger.Info("initialized Appview client", "host", args.AppviewHost)
	}
	if args.PLCHost != "" {
		docCacheSize := 50_000
		docCacheTTL := time.Minute * 1
		auditCacheSize := 100_000
		auditCacheTTL := time.Hour * 1
		didClient := did.NewClient(args.PLCHost, docCacheSize, docCacheTTL, auditCacheSize, auditCacheTTL)
		en.didClient = didClient
		logger.Info("initialized DID client", "host", args.PLCHost)
	}

	secureProducer, err := producer.New(ctx, logger, args.KafkaBootstrapServers, args.OutputTopic,
		producer.WithCredentials[*osprey.OspreyInputEvent](args.SASLUsername, args.SASLPassword),
		producer.WithEnsureTopic[*osprey.OspreyInputEvent](true),
		producer.WithTopicPartitions[*osprey.OspreyInputEvent](100),
		producer.WithMaxMessageBytes[*osprey.OspreyInputEvent](5<<20), // 5 MiB
	)
	if err != nil {
		return nil, errors.Join(err, errors.New("failed to create secure Kafka producer"))
	}
	en.producer = secureProducer

	busConsumer, err := consumer.New(logger, args.KafkaBootstrapServers, args.InputTopic, "enricher-consumers",
		consumer.WithOffset[*osprey.FirehoseEvent](consumer.OffsetEnd),
		consumer.WithMessageHandler(en.handleEvent),
		consumer.WithDeadLetterQueue[*osprey.FirehoseEvent](),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Bus consumer: %w", err)
	}
	en.consumer = busConsumer

	logger.Info("initialized enricher Bus consumer and producer",
		"bootstrap_serers", args.KafkaBootstrapServers,
		"input-topic", args.InputTopic,
		"output-topic", args.OutputTopic,
	)

	return &en, nil
}

func (en *Enricher) Run(ctx context.Context) error {
	defer en.producer.Close()
	defer en.consumer.Close()

	shutdownConsumer := make(chan struct{})
	consumerShutdown := make(chan struct{})
	go func() {
		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)
		go func() {
			for {
				err := en.consumer.Consume(ctx)
				if err != nil {
					if errors.Is(err, consumer.ErrClientClosed) {
						en.logger.Info("consumer client closed, stopping")
						break
					}
					en.logger.Error("failed to consume messages", "err", err)
				}
			}
			close(consumerShutdown)
		}()
		<-shutdownConsumer
		en.consumer.Close()
		cancel()
	}()

	// Handle exit signals.
	en.logger.Debug("registering OS exit signal handler")
	quit := make(chan struct{})
	exitSignals := make(chan os.Signal, 1)
	signal.Notify(exitSignals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		// Trigger the return that causes an exit when we return from this goroutine.
		defer close(quit)

		select {
		case sig := <-exitSignals:
			en.logger.Info("received OS exit signal", "signal", sig)
		case <-ctx.Done():
			en.logger.Info("shutting down on context done")
		}

		// Wait up to 5 seconds for the Consumer to finish processing.
		close(shutdownConsumer)
		select {
		case <-consumerShutdown:
			en.logger.Info("Consumer finished processing")
		case <-time.After(5 * time.Second):
			en.logger.Warn("Consumer did not finish processing in time, forcing shutdown")
		}

		// Flush the producer to ensure all messages are sent.
		en.producer.Close()
	}()

	<-quit
	en.logger.Info("graceful shutdown complete")
	return nil
}

type GenericResult struct {
	Raw       []byte
	Err       error
	ErrString string
}

func (en *Enricher) handleEvent(ctx context.Context, event *osprey.FirehoseEvent) error {
	if event.Commit == nil {
		return nil
	}

	// TODO: we probably want to handle deletes too
	if event.Commit.Operation != osprey.CommitOperation_COMMIT_OPERATION_CREATE && event.Commit.Operation != osprey.CommitOperation_COMMIT_OPERATION_UPDATE {
		return nil
	}

	logger := en.logger.With("did", event.Did, "collection", event.Commit.Collection, "rkey", event.Commit.Rkey, "operation", event.Commit.Operation.String())

	wg := &sync.WaitGroup{}
	hiveResults := xsync.NewMapOf[string, *osprey.ImageDispatchResults_HiveResults]()
	abyssResults := xsync.NewMapOf[string, *osprey.ImageDispatchResults_AbyssResults]()
	retinaResults := xsync.NewMapOf[string, *osprey.ImageDispatchResults_RetinaResults]()
	prescreenResults := xsync.NewMapOf[string, *osprey.ImageDispatchResults_PrescreenResults]()
	var ozoneRepoViewDetail []byte
	var profileView []byte
	var didDoc []byte
	var didAuditLog []byte

	dispatchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	start := time.Now()

	// Dispatch to Ozone for RepoViewDetail
	if en.ozoneClient != nil {
		wg.Go(func() {
			logger := logger.With("processor", "ozone")

			logger.Info("fetching RepoViewDetail from Ozone")
			respBody, repoViewDetail, err := en.ozoneClient.GetRepoView(ctx, event.Did)
			if err != nil {
				logger.Error("failed to fetch RepoViewDetail from Ozone", "err", err)
				return
			}

			if repoViewDetail == nil {
				logger.Warn("empty RepoViewDetail received from Ozone")
				return
			}

			ozoneRepoViewDetail = respBody
			logger.Info("successfully fetched RepoViewDetail from Ozone")
		})
	}

	// Dispatch to AppView for ProfileView
	if en.appviewClient != nil {
		wg.Go(func() {
			logger := logger.With("processor", "appview")

			logger.Info("fetching ProfileView from AppView")
			respBody, prof, err := en.appviewClient.GetProfile(ctx, event.Did)
			if err != nil {
				logger.Error("failed to fetch ProfileView from AppView", "err", err)
				return
			}

			if prof == nil {
				logger.Warn("empty ProfileView received from AppView")
				return
			}

			profileView = respBody
			logger.Info("successfully fetched ProfileView from AppView")
		})
	}

	// Dispatch to DID Resolver for DID Doc
	if en.didClient != nil {
		wg.Go(func() {
			logger := logger.With("processor", "did")

			logger.Info("fetching DID Document from DID resolver")
			respBody, doc, err := en.didClient.GetDIDDoc(ctx, event.Did)
			if err != nil {
				logger.Error("failed to fetch DID Document from DID resolver", "err", err)
				return
			}

			if doc == nil {
				logger.Warn("empty DID Document received from DID resolver")
				return
			}

			didDoc = respBody
			logger.Info("successfully fetched DID Document from DID resolver")
		})

		// Lookup the Audit Log for DID:PLC DIDs to get DID creation time.
		if strings.HasPrefix(event.Did, "did:plc:") {
			wg.Go(func() {
				logger := logger.With("processor", "did-audit")

				logger.Info("fetching DID audit log from DID resolver")
				respBody, log, err := en.didClient.GetDIDAuditLog(ctx, event.Did)
				if err != nil {
					if errors.Is(err, did.ErrAuditLogNotFound) {
						logger.Info("no DID audit log found for DID")
						return
					}
					logger.Error("failed to fetch DID audit log from DID resolver", "err", err)
					return
				}

				if log == nil {
					logger.Warn("empty DID audit log received from DID resolver")
					return
				}

				didAuditLog = respBody
				logger.Info("successfully fetched DID audit log from DID resolver")
			})
		}
	}

	// Download all of the blobs for the record while fetching other information

	rec, err := data.UnmarshalJSON(event.Commit.Record)
	if err != nil {
		return fmt.Errorf("failed to unmarshal commit record: %w", err)
	}

	blobs := data.ExtractBlobs(rec)
	imageCids := []string{}
	videoCids := []string{}

	for _, blob := range blobs {
		mimeType := strings.ToLower(blob.MimeType)
		if strings.HasPrefix(mimeType, "image/") {
			imageCids = append(imageCids, blob.Ref.String())
		} else if strings.HasPrefix(mimeType, "video/") {
			videoCids = append(videoCids, blob.Ref.String())
		}
	}

	images := xsync.NewMapOf[string, []byte]()
	wg.Go(func() {
		var imgWg sync.WaitGroup
		for _, cid := range imageCids {
			imgWg.Add(1)
			func(cid string) {
				defer imgWg.Done()
				bytes, err := en.cdn.GetImageBytes(ctx, event.Did, cid)
				if err != nil {
					logger.Error("failed to fetch image bytes", "did", event.Did, "cid", cid, "err", err)
					return
				}
				images.Store(cid, bytes)
			}(cid)
		}
		imgWg.Wait()
	})

	// Wait for all of the above to complete
	wg.Wait()

	// Dispatch images to enabled enrichers.
	images.Range(func(cid string, img []byte) bool {
		wg.Add(1)
		go func(img []byte) {
			defer wg.Done()

			// Send to the prescreen service first, if we get back "true", send to Hive
			if en.prescreenClient != nil {
				logger := logger.With("processor", "prescreen", "image_cid", cid)
				logger.Info("dispatching image to prescreen")
				decision, res, err := en.prescreenClient.Scan(dispatchCtx, event.Did, img)
				if err != nil {
					logger.Error("failed to scan image with prescreen", "err", err)
					prescreenResults.Store(cid, &osprey.ImageDispatchResults_PrescreenResults{
						Error: asProtoErr(err),
					})
					return
				}
				logger.Info("prescreen scan successful", "decision", decision)

				prescreenResults.Store(cid, &osprey.ImageDispatchResults_PrescreenResults{
					Raw:      res,
					Decision: &decision,
				})

				if decision == "sfw" {
					return
				}
			}

			// If prescreen flags as NSFW, forward to Hive for more detailed analysis.
			if en.hiveClient != nil {
				logger := logger.With("processor", "hive", "image_cid", cid)
				logger.Info("dispatching image")
				res, classes, err := en.hiveClient.Scan(dispatchCtx, img)
				if err != nil {
					logger.Error("failed to scan image", "err", err)
					hiveResults.Store(cid, &osprey.ImageDispatchResults_HiveResults{
						Error: asProtoErr(err),
					})
					return
				}
				logger.Info("scan successful")
				hiveResults.Store(cid, &osprey.ImageDispatchResults_HiveResults{
					Raw:     res,
					Classes: classes,
				})
			}
		}(img)

		if en.abyssClient != nil {
			wg.Add(1)
			go func(img []byte) {
				defer wg.Done()
				logger := logger.With("processor", "abyss", "image_cid", cid)
				logger.Info("dispatching image")
				res, isAbuseMatch, err := en.abyssClient.Scan(dispatchCtx, event.Did, img)
				if err != nil {
					logger.Error("failed to scan image", "err", err)
					abyssResults.Store(cid, &osprey.ImageDispatchResults_AbyssResults{
						Error: asProtoErr(err),
					})
					return
				}
				logger.Info("scan successful")
				abyssResults.Store(cid, &osprey.ImageDispatchResults_AbyssResults{
					Raw:          res,
					IsAbuseMatch: &isAbuseMatch,
				})
			}(img)
		}

		if en.retinaClient != nil {
			wg.Add(1)
			go func(img []byte) {
				defer wg.Done()
				logger := logger.With("processor", "retina", "image_cid", cid)
				logger.Info("dispatching image")
				res, ocrText, err := en.retinaClient.Scan(dispatchCtx, event.Did, cid, img)
				if err != nil {
					logger.Error("failed to scan image", "err", err)
					retinaResults.Store(cid, &osprey.ImageDispatchResults_RetinaResults{
						Error: asProtoErr(err),
					})
					return
				}
				logger.Info("scan successful")
				retinaResults.Store(cid, &osprey.ImageDispatchResults_RetinaResults{
					Raw:  res,
					Text: &ocrText,
				})
			}(img)
		}

		return true
	})

	// Wait on blob processing requests
	wg.Wait()

	imageResults := make(map[string]*osprey.ImageDispatchResults, images.Size())

	images.Range(func(cid string, _ []byte) bool {
		result := &osprey.ImageDispatchResults{Cid: cid}
		result.Hive, _ = hiveResults.Load(cid)
		result.Abyss, _ = abyssResults.Load(cid)
		result.Retina, _ = retinaResults.Load(cid)
		result.Prescreen, _ = prescreenResults.Load(cid)
		imageResults[cid] = result
		return true
	})

	logger.Info("record fully processed", "duration_seconds", time.Since(start).Seconds())

	modEvt := evtToModerationResults(event, ozoneRepoViewDetail, profileView, didDoc, didAuditLog)

	outOspreyEvt, err := modResultsToOspreyEvent(modEvt)
	if err != nil {
		return fmt.Errorf("failed to create OspreyInputEvent from ModerationEnrichedFirehoseRecordEvent: %w", err)
	}

	if err := en.producer.ProduceAsync(context.Background(), event.Did, outOspreyEvt, nil); err != nil {
		return fmt.Errorf("failed to produce OspreyInputEvent: %w", err)
	}

	logger.Info("produced OspreyInputEvent")

	return nil
}

func asProtoErr(err error) *string {
	if err == nil {
		return nil
	}
	errStr := err.Error()
	return &errStr
}

func evtToModerationResults(
	event *osprey.FirehoseEvent,
	ozoneRepoViewDetail []byte,
	profileView []byte,
	didDoc []byte,
	didAuditLog []byte,
) *osprey.ModerationEnrichedFirehoseRecordEvent {
	return &osprey.ModerationEnrichedFirehoseRecordEvent{
		Did:                 event.Did,
		Timestamp:           event.Timestamp,
		Collection:          event.Commit.Collection,
		Rkey:                event.Commit.Rkey,
		Cid:                 event.Commit.Cid,
		Operation:           event.Commit.Operation,
		Record:              event.Commit.Record,
		OzoneRepoViewDetail: ozoneRepoViewDetail,
		ProfileView:         profileView,
		DidDoc:              didDoc,
		DidAuditLog:         didAuditLog,
	}
}

func modResultsToOspreyEvent(
	event *osprey.ModerationEnrichedFirehoseRecordEvent,
) (*osprey.OspreyInputEvent, error) {
	actionName := event.Collection
	switch event.Operation {
	case osprey.CommitOperation_COMMIT_OPERATION_CREATE:
		actionName += "#create"
	case osprey.CommitOperation_COMMIT_OPERATION_UPDATE:
		actionName += "#update"
	case osprey.CommitOperation_COMMIT_OPERATION_DELETE:
		actionName += "#delete"
	}

	dataBytes, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal ModerationEnrichedFirehoseRecordEvent: %w", err)
	}

	return &osprey.OspreyInputEvent{
		SendTime: timestamppb.Now(),
		Data: &osprey.OspreyInputEventData{
			ActionName: actionName,
			ActionId:   time.Now().UnixMicro(),
			Timestamp:  event.Timestamp,
			SecretData: map[string]string{},
			Encoding:   "UTF8",
			Data:       dataBytes,
		},
	}, nil
}
