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
	"github.com/bluesky-social/go-util/pkg/telemetry"
	"github.com/bluesky-social/osprey-atproto/enricher/abyss"
	"github.com/bluesky-social/osprey-atproto/enricher/appview"
	"github.com/bluesky-social/osprey-atproto/enricher/did"
	"github.com/bluesky-social/osprey-atproto/enricher/hive"
	"github.com/bluesky-social/osprey-atproto/enricher/ozone"
	"github.com/bluesky-social/osprey-atproto/enricher/prescreen"
	"github.com/bluesky-social/osprey-atproto/enricher/retina"
	osprey "github.com/bluesky-social/osprey-atproto/proto/go"
	cli "github.com/urfave/cli/v2"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Enricher struct {
	logger          *slog.Logger
	producer        *producer.Producer[*osprey.OspreyInputEvent]
	abyssClient     *abyss.Client
	hiveClient      *hive.Client
	retinaClient    *retina.Client
	prescreenClient *prescreen.Client
	ozoneClient     *ozone.Client
	appviewClient   *appview.Client
	didClient       *did.Client
}

var enricherCmd = &cli.Command{
	Name: "enricher",
	Flags: []cli.Flag{
		&cli.StringSliceFlag{
			Name:     "bootstrap-servers",
			Usage:    "Kafka bootstrap servers",
			Required: true,
			EnvVars:  []string{"KAFKA_BOOTSTRAP_SERVERS"},
		},
		&cli.StringFlag{
			Name:    "input-topic",
			Usage:   "Kafka topic to consume from",
			Value:   "records_and_images",
			EnvVars: []string{"INPUT_KAFKA_TOPIC"},
		},
		&cli.StringFlag{
			Name:    "output-topic",
			Usage:   "Kafka topic to produce to",
			Value:   "enriched_records",
			EnvVars: []string{"OUTPUT_KAFKA_TOPIC"},
		},
		&cli.StringFlag{
			Name:    "sasl-username",
			Usage:   "SASL username for Kafka authentication",
			EnvVars: []string{"SASL_USERNAME"},
		},
		&cli.StringFlag{
			Name:    "sasl-password",
			Usage:   "SASL password for Kafka authentication",
			EnvVars: []string{"SASL_PASSWORD"},
		},
		&cli.StringFlag{
			Name:    "secure-output-topic",
			Usage:   "Kafka topic to produce to on the Secure Cluster",
			Value:   "moderation_enriched_records",
			EnvVars: []string{"SECURE_OUTPUT_KAFKA_TOPIC"},
		},
		&cli.StringFlag{
			Name:    "abyss-url",
			Usage:   "URL for the Abyss service including scheme",
			EnvVars: []string{"ABYSS_URL"},
		},
		&cli.StringFlag{
			Name:    "abyss-admin-password",
			Usage:   "Admin password for the Abyss service",
			EnvVars: []string{"ABYSS_ADMIN_PASSWORD"},
		},
		&cli.StringFlag{
			Name:    "hive-api-token",
			Usage:   "API token for the Hive service",
			EnvVars: []string{"HIVE_API_TOKEN"},
		},
		&cli.StringFlag{
			Name:    "retina-url",
			Usage:   "URL for the Retina service including scheme",
			EnvVars: []string{"RETINA_URL"},
		},
		&cli.StringFlag{
			Name:    "retina-api-key",
			Usage:   "API key for the Retina service",
			EnvVars: []string{"RETINA_API_KEY"},
		},
		&cli.StringFlag{
			Name:    "prescreen-host",
			Usage:   "Host for the prescreen service",
			EnvVars: []string{"PRESCREEN_HOST"},
		},
		&cli.StringFlag{
			Name:    "ozone-host",
			Usage:   "Host for the Ozone service",
			EnvVars: []string{"OZONE_HOST"},
		},
		&cli.StringFlag{
			Name:    "ozone-admin-token",
			Usage:   "Admin token for the Ozone service",
			EnvVars: []string{"OZONE_ADMIN_TOKEN"},
		},
		&cli.StringFlag{
			Name:    "appview-host",
			Usage:   "Host for the Appview service",
			EnvVars: []string{"APPVIEW_HOST"},
		},
		&cli.StringFlag{
			Name:    "appview-ratelimit-bypass",
			Usage:   "Rate limit bypass token for the Appview service",
			EnvVars: []string{"APPVIEW_RATELIMIT_BYPASS"},
		},
		&cli.StringFlag{
			Name:    "plc-host",
			Usage:   "plc host for DID:PLC doc lookups",
			EnvVars: []string{"PLC_HOST"},
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := cctx.Context
		// Flags
		opt := struct {
			KafkaBootstrapServers  []string
			SASLUsername           string
			SASLPassword           string
			InputTopic             string
			OutputTopic            string
			SecureOutputTopic      string
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
		}{
			KafkaBootstrapServers:  cctx.StringSlice("kafka-bootstrap-servers"),
			SASLUsername:           cctx.String("sasl-username"),
			SASLPassword:           cctx.String("sasl-password"),
			InputTopic:             cctx.String("input-topic"),
			OutputTopic:            cctx.String("output-topic"),
			SecureOutputTopic:      cctx.String("secure-output-topic"),
			AbyssURL:               cctx.String("abyss-url"),
			AbyssAdminPassword:     cctx.String("abyss-admin-password"),
			HiveAPIToken:           cctx.String("hive-api-token"),
			RetinaURL:              cctx.String("retina-url"),
			RetinaAPIKey:           cctx.String("retina-api-key"),
			PrescreenHost:          cctx.String("prescreen-host"),
			OzoneHost:              cctx.String("ozone-host"),
			OzoneAdminToken:        cctx.String("ozone-admin-token"),
			AppviewHost:            cctx.String("appview-host"),
			AppviewRatelimitBypass: cctx.String("appview-ratelimit-bypass"),
			PLCHost:                cctx.String("plc-host"),
		}

		logger := telemetry.StartLogger(cctx)
		telemetry.StartMetrics(cctx)

		en := Enricher{
			logger: logger,
		}

		if opt.AbyssURL != "" {
			abyssClient := abyss.NewClient(opt.AbyssURL, opt.AbyssAdminPassword)
			en.abyssClient = abyssClient
			logger.Info("initialized Abyss client", "url", opt.AbyssURL)
		}
		if opt.HiveAPIToken != "" {
			hiveClient := hive.NewClient(opt.HiveAPIToken)
			en.hiveClient = hiveClient
			logger.Info("initialized Hive client")
		}
		if opt.RetinaURL != "" && opt.RetinaAPIKey != "" {
			retinaClient := retina.NewRetinaClient(opt.RetinaURL, opt.RetinaAPIKey)
			en.retinaClient = retinaClient
			logger.Info("initialized Retina client", "url", opt.RetinaURL)
		}
		if opt.PrescreenHost != "" {
			prescreenClient := prescreen.NewClient(opt.PrescreenHost)
			en.prescreenClient = prescreenClient
			logger.Info("initialized Prescreen client", "host", opt.PrescreenHost)
		}
		if opt.OzoneHost != "" && opt.OzoneAdminToken != "" {
			cacheSize := 50_000
			cacheTTL := time.Minute * 1
			ozoneClient := ozone.NewClient(opt.OzoneHost, opt.OzoneAdminToken, cacheSize, cacheTTL)
			en.ozoneClient = ozoneClient
			logger.Info("initialized Ozone client", "host", opt.OzoneHost)
		}
		if opt.AppviewHost != "" {
			cacheSize := 0
			cacheTTL := time.Duration(0)
			appviewClient := appview.NewClient(opt.AppviewHost, opt.AppviewRatelimitBypass, cacheSize, cacheTTL)
			en.appviewClient = appviewClient
			logger.Info("initialized Appview client", "host", opt.AppviewHost)
		}
		if opt.PLCHost != "" {
			docCacheSize := 50_000
			docCacheTTL := time.Minute * 1
			auditCacheSize := 100_000
			auditCacheTTL := time.Hour * 1
			didClient := did.NewClient(opt.PLCHost, docCacheSize, docCacheTTL, auditCacheSize, auditCacheTTL)
			en.didClient = didClient
			logger.Info("initialized DID client", "host", opt.PLCHost)
		}

		secureProducer, err := producer.New(ctx, logger, opt.KafkaBootstrapServers, opt.SecureOutputTopic,
			producer.WithCredentials[*osprey.OspreyInputEvent](opt.SASLUsername, opt.SASLPassword),
			producer.WithEnsureTopic[*osprey.OspreyInputEvent](true),
			producer.WithTopicPartitions[*osprey.OspreyInputEvent](100),
			producer.WithMaxMessageBytes[*osprey.OspreyInputEvent](5<<20), // 5 MiB
		)
		if err != nil {
			return errors.Join(err, errors.New("failed to create secure Kafka producer"))
		}
		defer secureProducer.Close()
		en.producer = secureProducer

		busConsumer, err := consumer.New(logger, opt.KafkaBootstrapServers, opt.InputTopic, "enricher-consumers",
			consumer.WithOffset[*osprey.FirehoseEvent](consumer.OffsetEnd),
			consumer.WithMessageHandler(en.handleEvent),
			consumer.WithDeadLetterQueue[*osprey.FirehoseEvent](),
		)
		if err != nil {
			return fmt.Errorf("failed to create Bus consumer: %w", err)
		}
		defer busConsumer.Close()

		logger.Info("initialized enricher Bus consumer and producer",
			"bootstrap_serers", opt.KafkaBootstrapServers,
			"input-topic", opt.InputTopic,
			"output-topic", opt.OutputTopic,
			"secure-output-topic", opt.SecureOutputTopic,
		)

		shutdownConsumer := make(chan struct{})
		consumerShutdown := make(chan struct{})
		go func() {
			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)
			go func() {
				for {
					err := busConsumer.Consume(ctx)
					if err != nil {
						if errors.Is(err, consumer.ErrClientClosed) {
							logger.Info("consumer client closed, stopping")
							break
						}
						logger.Error("failed to consume messages", "err", err)
					}
				}
				close(consumerShutdown)
			}()
			<-shutdownConsumer
			busConsumer.Close()
			cancel()
		}()

		// Handle exit signals.
		logger.Debug("registering OS exit signal handler")
		quit := make(chan struct{})
		exitSignals := make(chan os.Signal, 1)
		signal.Notify(exitSignals, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			// Trigger the return that causes an exit when we return from this goroutine.
			defer close(quit)

			select {
			case sig := <-exitSignals:
				logger.Info("received OS exit signal", "signal", sig)
			case <-ctx.Done():
				logger.Info("shutting down on context done")
			}

			// Wait up to 5 seconds for the Consumer to finish processing.
			close(shutdownConsumer)
			select {
			case <-consumerShutdown:
				logger.Info("Consumer finished processing")
			case <-time.After(5 * time.Second):
				logger.Warn("Consumer did not finish processing in time, forcing shutdown")
			}

			// Flush the producer to ensure all messages are sent.
			en.producer.Close()
		}()

		<-quit
		logger.Info("graceful shutdown complete")
		return nil
	},
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
	// hiveResults := xsync.NewMapOf[string, *osprey.ImageDispatchResults_HiveResults]()
	// abyssResults := xsync.NewMapOf[string, *osprey.ImageDispatchResults_AbyssResults]()
	// retinaResults := xsync.NewMapOf[string, *osprey.ImageDispatchResults_RetinaResults]()
	// prescreenResults := xsync.NewMapOf[string, *osprey.ImageDispatchResults_PrescreenResults]()
	var ozoneRepoViewDetail []byte
	var profileView []byte
	var didDoc []byte
	var didAuditLog []byte

	// dispatchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	// defer cancel()

	start := time.Now()

	// Dispatch to Ozone for RepoViewDetail
	if en.ozoneClient != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()

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
		}()
	}

	// Dispatch to AppView for ProfileView
	if en.appviewClient != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()

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
		}()
	}

	// Dispatch to DID Resolver for DID Doc
	if en.didClient != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()

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
		}()

		// Lookup the Audit Log for DID:PLC DIDs to get DID creation time.
		if strings.HasPrefix(event.Did, "did:plc:") {
			wg.Add(1)
			go func() {
				defer wg.Done()

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
			}()
		}
	}

	// TODO: Images should get downloaded somewhere. In prod, we actually take the firehose kafka stream and make a "firehose with image bytes"
	// kafka stream, which the enricher consumes.
	// Here, we might optionally just download the images inside the enricher and use those bytes inside of each of these additional helpers...
	// Need to add that image download logic

	// Dispatch images to enabled enrichers.
	// for _, img := range event.Images {
	// 	wg.Add(1)
	// 	go func(img *osprey.RecordImageBatchEvent_Image) {
	// 		defer wg.Done()
	//
	// 		// Send to the prescreen service first, if we get back "true", send to Hive
	// 		if en.prescreenClient != nil {
	// 			logger := logger.With("processor", "prescreen", "image_cid", img.Cid)
	// 			logger.Info("dispatching image to prescreen")
	// 			decision, res, err := en.prescreenClient.Scan(dispatchCtx, event.Did, img.Data)
	// 			if err != nil {
	// 				logger.Error("failed to scan image with prescreen", "err", err)
	// 				prescreenResults.Store(img.Cid, &osprey.ImageDispatchResults_PrescreenResults{
	// 					Error: asProtoErr(err),
	// 				})
	// 				return
	// 			}
	// 			logger.Info("prescreen scan successful", "decision", decision)
	//
	// 			prescreenResults.Store(img.Cid, &osprey.ImageDispatchResults_PrescreenResults{
	// 				Raw:      res,
	// 				Decision: &decision,
	// 			})
	//
	// 			if decision == "sfw" {
	// 				return
	// 			}
	// 		}
	//
	// 		// If prescreen flags as NSFW, forward to Hive for more detailed analysis.
	// 		if en.hiveClient != nil {
	// 			logger := logger.With("processor", "hive", "image_cid", img.Cid)
	// 			logger.Info("dispatching image")
	// 			res, classes, err := en.hiveClient.Scan(dispatchCtx, img.Data)
	// 			if err != nil {
	// 				logger.Error("failed to scan image", "err", err)
	// 				hiveResults.Store(img.Cid, &osprey.ImageDispatchResults_HiveResults{
	// 					Error: asProtoErr(err),
	// 				})
	// 				return
	// 			}
	// 			logger.Info("scan successful")
	// 			hiveResults.Store(img.Cid, &osprey.ImageDispatchResults_HiveResults{
	// 				Raw:     res,
	// 				Classes: classes,
	// 			})
	// 		}
	// 	}(img)
	//
	// 	if en.abyssClient != nil {
	// 		wg.Add(1)
	// 		go func(img *osprey.RecordImageBatchEvent_Image) {
	// 			defer wg.Done()
	// 			logger := logger.With("processor", "abyss", "image_cid", img.Cid)
	// 			logger.Info("dispatching image")
	// 			res, isAbuseMatch, err := en.abyssClient.Scan(dispatchCtx, event.Did, img.Data)
	// 			if err != nil {
	// 				logger.Error("failed to scan image", "err", err)
	// 				abyssResults.Store(img.Cid, &osprey.ImageDispatchResults_AbyssResults{
	// 					Error: asProtoErr(err),
	// 				})
	// 				return
	// 			}
	// 			logger.Info("scan successful")
	// 			abyssResults.Store(img.Cid, &osprey.ImageDispatchResults_AbyssResults{
	// 				Raw:          res,
	// 				IsAbuseMatch: &isAbuseMatch,
	// 			})
	// 		}(img)
	// 	}
	//
	// 	if en.retinaClient != nil {
	// 		wg.Add(1)
	// 		go func(img *osprey.RecordImageBatchEvent_Image) {
	// 			defer wg.Done()
	// 			logger := logger.With("processor", "retina", "image_cid", img.Cid)
	// 			logger.Info("dispatching image")
	// 			res, ocrText, err := en.retinaClient.Scan(dispatchCtx, event.Did, img.Cid, img.Data)
	// 			if err != nil {
	// 				logger.Error("failed to scan image", "err", err)
	// 				retinaResults.Store(img.Cid, &osprey.ImageDispatchResults_RetinaResults{
	// 					Error: asProtoErr(err),
	// 				})
	// 				return
	// 			}
	// 			logger.Info("scan successful")
	// 			retinaResults.Store(img.Cid, &osprey.ImageDispatchResults_RetinaResults{
	// 				Raw:  res,
	// 				Text: &ocrText,
	// 			})
	// 		}(img)
	// 	}
	// }

	wg.Wait()

	// TODO: waiting on the above

	// imageResults := make(map[string]*osprey.ImageDispatchResults, len(event.Images))
	// for cid := range event.Images {
	// 	result := &osprey.ImageDispatchResults{Cid: cid}
	// 	result.Hive, _ = hiveResults.Load(cid)
	// 	result.Abyss, _ = abyssResults.Load(cid)
	// 	result.Retina, _ = retinaResults.Load(cid)
	// 	result.Prescreen, _ = prescreenResults.Load(cid)
	// 	imageResults[cid] = result
	// }

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
