package effector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/bluesky-social/go-util/pkg/bus/consumer"
	osprey "github.com/bluesky-social/osprey-atproto/proto/go"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	NAMESPACE = "osprey_effector"
)

var (
	eventsReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "events_received",
		Namespace: NAMESPACE,
		Help:      "number of events received, by action name",
	}, []string{"action_name"})

	eventsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "events_processed",
		Namespace: NAMESPACE,
		Help:      "number of events processed, by action name and status",
	}, []string{"action_name", "status"})

	effectsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "effects_processed",
		Namespace: NAMESPACE,
		Help:      "number of effects processed, by type and status",
	}, []string{"type", "status"})

	ozoneRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "ozone_requests",
		Namespace: NAMESPACE,
		Help:      "number of requests to Ozone",
	}, []string{"type", "kind", "status"})
)

type OspreyEffector struct {
	logger *slog.Logger

	consumer *consumer.Consumer[*osprey.ResultEvent]

	ozoneClient *OzoneClient

	memClient *memcache.Client

	logManager     *OspreyLogManager
	bigQueryLogger *BigQueryLogger

	bigqueryFlagClient *BigQueryFlagClient

	isProduction bool
}

type Args struct {
	BootstrapServers []string
	InputTopic       string
	ConsumerGroup    string
	Logger           *slog.Logger

	BigQueryCredentialsJson []byte
	BigQueryProjectID       string
	BigQueryDatasetID       string

	OzonePdsHost    string
	OzoneIdentifier string
	OzonePassword   string
	OzoneProxyDid   string

	MemcacheServers []string

	IsProduction bool

	SlackWebhookURL string
}

func New(args *Args) (*OspreyEffector, error) {
	if args.Logger == nil {
		args.Logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}))
	}

	logger := args.Logger

	loginCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	oc, err := NewOzoneClient(loginCtx, &OzoneClientArgs{
		PdsHost:      args.OzonePdsHost,
		Identifier:   args.OzoneIdentifier,
		Password:     args.OzonePassword,
		ProxyDid:     args.OzoneProxyDid,
		IsProduction: args.IsProduction,
	})
	if err != nil {
		return nil, fmt.Errorf("could not create ozone client: %w", err)
	}

	if args.InputTopic == "" {
		return nil, errors.New("must supply an input topic to osprey effector")
	}

	if args.ConsumerGroup == "" {
		logger.Warn("no consumer group provided to osprey effector. using staging consumer group")
		args.ConsumerGroup = "osprey-effector-staging-consumers"
	}

	memcli := memcache.New(args.MemcacheServers...)

	if err := memcli.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping memcache servers: %w", err)
	}

	or := &OspreyEffector{
		logger: args.Logger,

		ozoneClient: oc,
		memClient:   memcli,

		isProduction: args.IsProduction,
	}

	lm := NewOspreyLogManager()

	// Create a BigQuery logger
	if args.IsProduction {
		bql, err := NewBigQueryLogger(&BigQueryLoggerArgs{
			CredentialsJson: args.BigQueryCredentialsJson,
			ProjectID:       args.BigQueryProjectID,
			DatasetID:       args.BigQueryDatasetID,
			Logger:          logger,
		})
		if err != nil {
			return nil, fmt.Errorf("could not create bigquery logger: %w", err)
		}
		lm.AddLogger(bql)
		or.bigQueryLogger = bql
	}

	// Add a Slack channel logger
	if args.SlackWebhookURL != "" {
		lm.AddLogger(NewSlackLogger(args.SlackWebhookURL))
	}

	// Add a slog logger for stdout
	lm.AddLogger(NewSlogLogger(logger))

	or.logManager = lm

	busConsumer, err := consumer.New(args.Logger, args.BootstrapServers, args.InputTopic, args.ConsumerGroup,
		consumer.WithOffset[*osprey.ResultEvent](consumer.OffsetEnd),
		consumer.WithMessageHandler(or.handleEventAsync),
	)
	if err != nil {
		return nil, fmt.Errorf("error creating bus consumer: %w", err)
	}
	or.consumer = busConsumer

	if args.IsProduction {
		bfc, err := NewBigQueryFlagClient(&BigQueryFlagClientArgs{
			CredentialsJson: args.BigQueryCredentialsJson,
			ProjectID:       args.BigQueryProjectID,
			DatasetID:       args.BigQueryDatasetID,
			Logger:          logger,
		})
		if err != nil {
			return nil, fmt.Errorf("error creating bigquery flag client: %w", err)
		}
		or.bigqueryFlagClient = bfc
	}

	return or, nil
}

func (or *OspreyEffector) Run(ctx context.Context) error {
	shutdownConsumer := make(chan struct{})
	consumerShutdown := make(chan struct{})
	go func() {
		logger := or.logger.With("component", "results_consumer")
		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)
		go func() {
			for {
				err := or.consumer.Consume(ctx)
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
		or.consumer.Close()
		cancel()
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	<-signals

	close(shutdownConsumer)
	if or.bigQueryLogger != nil {
		or.bigQueryLogger.Close()
	}

	return nil
}

func (or *OspreyEffector) handleEventAsync(ctx context.Context, evt *osprey.ResultEvent) error {
	go func() {
		ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()

		if err := or.handleEvent(ctx, evt); err != nil {
			or.logger.Error("error handling event async", "error", err)
		}
	}()
	return nil
}

func (or *OspreyEffector) handleEvent(ctx context.Context, evt *osprey.ResultEvent) error {
	if evt == nil {
		or.logger.Warn("attempted to handle nil event")
		return nil
	}

	eventsReceived.WithLabelValues(evt.ActionName).Inc()

	status := "error"
	defer func() {
		eventsProcessed.WithLabelValues(evt.ActionName, status).Inc()
	}()

	evtJsonB, err := json.Marshal(evt)
	if err != nil {
		or.logger.Error("failed to marshal event for logging", "error", err)
	} else {
		evtStr := string(evtJsonB)
		if err := or.logManager.LogEvent(ctx, &OspreyEventLog{
			ActionName: evt.ActionName,
			ActionID:   evt.ActionId,
			Did:        evt.Did,
			Uri:        evt.Uri,
			Cid:        evt.Cid,
			Raw:        evtStr,
			SendTime:   evt.SendTime.AsTime(),
			CreatedAt:  time.Now(),
		}); err != nil {
			or.logger.Error("failed to log event", "error", err)
		}
	}

	for _, e := range evt.Labels {
		ozoneStatus := "error"
		defer func() {
			ozoneRequests.WithLabelValues("label", e.SubjectKind.String(), ozoneStatus).Inc()
		}()

		rules := strings.Join(e.Rules, ",")

		e.Comment = fmt.Sprintf("Actioned by rules %s\n\n%s", rules, e.Comment)

		switch e.SubjectKind {
		// Label actors
		case osprey.AtprotoSubjectKind_ATPROTO_SUBJECT_KIND_ACTOR:
			if or.checkHasActioned(evt.Did, rules, e.ExpirationInHours) {
				or.logger.Info("skipping ozone label effect", "actionId", evt.ActionId)
				ozoneStatus = "skipped"
				continue
			}

			if err := or.ozoneClient.LabelActor(
				ctx,
				evt.Did,
				ModToolMeta{
					Rules: rules,
				},
				e.Label,
				e.Comment,
				e.Email,
				e.ExpirationInHours,
				e.EffectKind == osprey.AtprotoEffectKind_ATPROTO_EFFECT_KIND_REMOVE,
			); err != nil {
				or.logger.Error("error processing actor label effects", "error", err)
			} else {
				ozoneStatus = "ok"
				or.logManager.LogEffect(context.Background(), &OspreyEffectLog{
					ActionName: evt.ActionName,
					ActionID:   evt.ActionId,
					Subject:    evt.Did,
					Kind:       "label",
					Comment:    e.Comment,
					Label: bigquery.NullString{
						StringVal: AtprotoLabelToString(e.Label),
						Valid:     true,
					},
					CreatedAt: time.Now(),
					Rules:     strings.Join(e.Rules, ","),
				})
			}

		// Label records
		case osprey.AtprotoSubjectKind_ATPROTO_SUBJECT_KIND_RECORD:
			if or.checkHasActioned(evt.Uri, rules, e.ExpirationInHours) {
				or.logger.Info("skipping ozone label effect", "actionId", evt.ActionId)
				ozoneStatus = "skipped"
				continue
			}

			if err := or.ozoneClient.LabelRecord(
				ctx,
				evt.Uri,
				evt.Cid,
				ModToolMeta{
					Rules: rules,
				},
				e.Label,
				e.Comment,
				e.Email,
				e.ExpirationInHours,
				e.EffectKind == osprey.AtprotoEffectKind_ATPROTO_EFFECT_KIND_REMOVE,
			); err != nil {
				or.logger.Error("error processing record label effects", "error", err)
			} else {
				ozoneStatus = "ok"
				or.logEffect(&OspreyEffectLog{
					ActionName: evt.ActionName,
					ActionID:   evt.ActionId,
					Subject:    evt.Uri,
					Kind:       "label",
					Comment:    e.Comment,
					Label: bigquery.NullString{
						StringVal: AtprotoLabelToString(e.Label),
						Valid:     true,
					},
					CreatedAt: time.Now(),
					Rules:     strings.Join(e.Rules, ","),
				})
			}
		}
	}

	for _, e := range evt.Tags {
		ozoneStatus := "error"
		defer func() {
			ozoneRequests.WithLabelValues("tag", e.SubjectKind.String(), ozoneStatus).Inc()
		}()

		rules := strings.Join(e.Rules, ",")

		comment := fmt.Sprintf("Actioned by rules %s", rules)
		if e.Comment != nil {
			comment = fmt.Sprintf("%s\n\n%s", comment, *e.Comment)
		}

		switch e.SubjectKind {
		// Tag actors
		case osprey.AtprotoSubjectKind_ATPROTO_SUBJECT_KIND_ACTOR:
			if or.checkHasActioned(evt.Did, rules, nil) {
				or.logger.Info("skipping ozone tag effect", "actionId", evt.ActionId)
				ozoneStatus = "skipped"
				continue
			}

			if err := or.ozoneClient.TagActor(
				ctx,
				evt.Did,
				ModToolMeta{
					Rules: rules,
				},
				e.Tag,
				e.Comment,
				e.EffectKind == osprey.AtprotoEffectKind_ATPROTO_EFFECT_KIND_REMOVE,
			); err != nil {
				or.logger.Error("error processing actor tag effects", "error", err)
			} else {
				ozoneStatus = "ok"
				or.logEffect(&OspreyEffectLog{
					ActionName: evt.ActionName,
					ActionID:   evt.ActionId,
					Subject:    evt.Did,
					Kind:       "tag",
					Comment:    comment,
					Tag: bigquery.NullString{
						StringVal: e.Tag,
						Valid:     true,
					},
					CreatedAt: time.Now(),
					Rules:     strings.Join(e.Rules, ","),
				})
			}

		// Tag records
		case osprey.AtprotoSubjectKind_ATPROTO_SUBJECT_KIND_RECORD:
			if or.checkHasActioned(evt.Uri, rules, nil) {
				or.logger.Info("skipping ozone tag effect", "actionId", evt.ActionId)
				ozoneStatus = "skipped"
				continue
			}

			if err := or.ozoneClient.TagRecord(
				ctx,
				evt.Uri,
				evt.Cid,
				ModToolMeta{
					Rules: rules,
				},
				e.Tag,
				e.Comment,
				e.EffectKind == osprey.AtprotoEffectKind_ATPROTO_EFFECT_KIND_REMOVE,
			); err != nil {
				or.logger.Error("error processing actor tag effects", "error", err)
			} else {
				ozoneStatus = "ok"
				or.logEffect(&OspreyEffectLog{
					ActionName: evt.ActionName,
					ActionID:   evt.ActionId,
					Subject:    evt.Uri,
					Kind:       "tag",
					Comment:    comment,
					Tag: bigquery.NullString{
						StringVal: e.Tag,
						Valid:     true,
					},
					CreatedAt: time.Now(),
					Rules:     strings.Join(e.Rules, ","),
				})
			}
		}
	}

	for _, e := range evt.Takedowns {
		ozoneStatus := "error"
		defer func() {
			ozoneRequests.WithLabelValues("takedown", e.SubjectKind.String(), ozoneStatus).Inc()
		}()

		rules := strings.Join(e.Rules, ",")

		e.Comment = fmt.Sprintf("Actioned by rules %s\n\n%s", rules, e.Comment)

		switch e.SubjectKind {
		case osprey.AtprotoSubjectKind_ATPROTO_SUBJECT_KIND_ACTOR:
			if or.checkHasActioned(evt.Did, rules, nil) {
				or.logger.Info("skipping ozone takedown effect", "actionId", evt.ActionId)
				ozoneStatus = "skipped"
				continue
			}

			if err := or.ozoneClient.TakedownActor(
				ctx,
				evt.Did,
				ModToolMeta{
					Rules: rules,
				},
				e.Comment,
				e.Email,
				e.EffectKind == osprey.AtprotoEffectKind_ATPROTO_EFFECT_KIND_REMOVE,
			); err != nil {
				or.logger.Error("error processing actor takedown effects", "error", err)
			} else {
				ozoneStatus = "ok"
				or.logEffect(&OspreyEffectLog{
					ActionName: evt.ActionName,
					ActionID:   evt.ActionId,
					Subject:    evt.Did,
					Kind:       "takedown",
					Comment:    e.Comment,
					CreatedAt:  time.Now(),
					Rules:      strings.Join(e.Rules, ","),
				})
			}
		case osprey.AtprotoSubjectKind_ATPROTO_SUBJECT_KIND_RECORD:
			if or.checkHasActioned(evt.Uri, rules, nil) {
				or.logger.Info("skipping ozone takedown effect", "actionId", evt.ActionId)
				ozoneStatus = "skipped"
				continue
			}

			if err := or.ozoneClient.TakedownRecord(
				ctx,
				evt.Uri,
				evt.Cid,
				ModToolMeta{
					Rules: rules,
				},
				e.Comment,
				e.Email,
				e.EffectKind == osprey.AtprotoEffectKind_ATPROTO_EFFECT_KIND_REMOVE,
			); err != nil {
				or.logger.Error("error processing record takedown effects", "error", err)
			} else {
				ozoneStatus = "ok"
				or.logEffect(&OspreyEffectLog{
					ActionName: evt.ActionName,
					ActionID:   evt.ActionId,
					Subject:    evt.Uri,
					Kind:       "takedown",
					Comment:    e.Comment,
					CreatedAt:  time.Now(),
					Rules:      strings.Join(e.Rules, ","),
				})
			}
		}
	}

	for _, e := range evt.Reports {
		ozoneStatus := "error"
		defer func() {
			ozoneRequests.WithLabelValues("report", e.SubjectKind.String(), ozoneStatus).Inc()
		}()

		rules := strings.Join(e.Rules, ",")

		e.Comment = fmt.Sprintf("Actioned by rules %s\n\n%s", rules, e.Comment)

		// NOTE: Purposefully do not ignore duplicate actions for reports
		switch e.SubjectKind {
		case osprey.AtprotoSubjectKind_ATPROTO_SUBJECT_KIND_ACTOR:
			if err := or.ozoneClient.ReportActor(
				ctx,
				evt.Did,
				ModToolMeta{
					Rules: rules,
				},
				e.ReportKind,
				e.Comment,
				e.PriorityScore,
			); err != nil {
				or.logger.Error("error processing actor report effects", "error", err)
			} else {
				ozoneStatus = "ok"
				or.logEffect(&OspreyEffectLog{
					ActionName: evt.ActionName,
					ActionID:   evt.ActionId,
					Subject:    evt.Did,
					Kind:       "report",
					Comment:    e.Comment,
					CreatedAt:  time.Now(),
					Rules:      strings.Join(e.Rules, ","),
				})
			}
		case osprey.AtprotoSubjectKind_ATPROTO_SUBJECT_KIND_RECORD:
			if err := or.ozoneClient.ReportRecord(
				ctx,
				evt.Uri,
				evt.Cid,
				ModToolMeta{
					Rules: rules,
				},
				e.ReportKind,
				e.Comment,
				e.PriorityScore,
			); err != nil {
				or.logger.Error("error processing record report effects", "error", err)
			} else {
				ozoneStatus = "ok"
				or.logEffect(&OspreyEffectLog{
					ActionName: evt.ActionName,
					ActionID:   evt.ActionId,
					Subject:    evt.Uri,
					Kind:       "report",
					Comment:    e.Comment,
					CreatedAt:  time.Now(),
					Rules:      strings.Join(e.Rules, ","),
				})
			}
		}
	}

	for _, e := range evt.Comments {
		ozoneStatus := "error"
		defer func() {
			ozoneRequests.WithLabelValues("comment", e.SubjectKind.String(), ozoneStatus).Inc()
		}()

		rules := strings.Join(e.Rules, ",")

		e.Comment = fmt.Sprintf("Actioned by rules %s\n\n%s", rules, e.Comment)

		switch e.SubjectKind {
		case osprey.AtprotoSubjectKind_ATPROTO_SUBJECT_KIND_ACTOR:
			if or.checkHasActioned(evt.Did, rules, nil) {
				or.logger.Info("skipping ozone comment effect", "actionId", evt.ActionId)
				ozoneStatus = "skipped"
				continue
			}

			if err := or.ozoneClient.CommentActor(
				ctx,
				evt.Did,
				ModToolMeta{
					Rules: rules,
				},
				e.Comment,
			); err != nil {
				or.logger.Error("error processing actor comment effects", "error", err)
			} else {
				ozoneStatus = "ok"
				or.logEffect(&OspreyEffectLog{
					ActionName: evt.ActionName,
					ActionID:   evt.ActionId,
					Subject:    evt.Did,
					Kind:       "comment",
					Comment:    e.Comment,
					CreatedAt:  time.Now(),
					Rules:      strings.Join(e.Rules, ","),
				})
			}
		case osprey.AtprotoSubjectKind_ATPROTO_SUBJECT_KIND_RECORD:
			if or.checkHasActioned(evt.Uri, rules, nil) {
				or.logger.Info("skipping ozone comment effect", "actionId", evt.ActionId)
				ozoneStatus = "skipped"
				continue
			}

			if err := or.ozoneClient.CommentRecord(
				ctx,
				evt.Uri,
				evt.Cid,
				ModToolMeta{
					Rules: rules,
				},
				e.Comment,
			); err != nil {
				or.logger.Error("error processing record comment effects", "error", err)
			} else {
				ozoneStatus = "ok"
				or.logEffect(&OspreyEffectLog{
					ActionName: evt.ActionName,
					ActionID:   evt.ActionId,
					Subject:    evt.Uri,
					Kind:       "comment",
					Comment:    e.Comment,
					CreatedAt:  time.Now(),
					Rules:      strings.Join(e.Rules, ","),
				})
			}
		}
	}

	for _, e := range evt.Escalations {
		ozoneStatus := "error"
		defer func() {
			ozoneRequests.WithLabelValues("comment", e.SubjectKind.String(), ozoneStatus).Inc()
		}()

		comment := fmt.Sprintf("Actioned by rules %s", strings.Join(e.Rules, ","))
		if e.Comment != nil {
			comment = fmt.Sprintf("%s\n\n%s", comment, *e.Comment)
		}

		// NOTE: Purposefully do not prevent duplicate actions for escalations
		switch e.SubjectKind {
		case osprey.AtprotoSubjectKind_ATPROTO_SUBJECT_KIND_ACTOR:
			if err := or.ozoneClient.EscalateActor(
				ctx,
				evt.Did,
				ModToolMeta{
					Rules: strings.Join(e.Rules, ","),
				},
				e.Comment,
			); err != nil {
				or.logger.Error("error processing actor escalation effects", "error", err)
			} else {
				ozoneStatus = "ok"
				or.logEffect(&OspreyEffectLog{
					ActionName: evt.ActionName,
					ActionID:   evt.ActionId,
					Subject:    evt.Did,
					Kind:       "escalation",
					Comment:    comment,
					CreatedAt:  time.Now(),
					Rules:      strings.Join(e.Rules, ","),
				})
			}
		case osprey.AtprotoSubjectKind_ATPROTO_SUBJECT_KIND_RECORD:
			if err := or.ozoneClient.EscalateRecord(
				ctx,
				evt.Uri,
				evt.Cid,
				ModToolMeta{
					Rules: strings.Join(e.Rules, ","),
				},
				e.Comment,
			); err != nil {
				or.logger.Error("error processing record escalation effects", "error", err)
			} else {
				ozoneStatus = "ok"
				or.logEffect(&OspreyEffectLog{
					ActionName: evt.ActionName,
					ActionID:   evt.ActionId,
					Subject:    evt.Uri,
					Kind:       "escalation",
					Comment:    comment,
					CreatedAt:  time.Now(),
					Rules:      strings.Join(e.Rules, ","),
				})
			}
		}
	}

	for _, e := range evt.Acknowledgements {
		ozoneStatus := "error"
		defer func() {
			ozoneRequests.WithLabelValues("comment", e.SubjectKind.String(), ozoneStatus).Inc()
		}()

		comment := fmt.Sprintf("Actioned by rules %s", strings.Join(e.Rules, ","))
		if e.Comment != nil {
			comment = fmt.Sprintf("%s\n\n%s", comment, *e.Comment)
		}

		// NOTE: Purposefully do not ignore duplicate actions for acks
		switch e.SubjectKind {
		case osprey.AtprotoSubjectKind_ATPROTO_SUBJECT_KIND_ACTOR:
			if err := or.ozoneClient.AcknowledgeActor(
				ctx,
				evt.Did,
				ModToolMeta{
					Rules: strings.Join(e.Rules, ","),
				},
				e.Comment,
			); err != nil {
				or.logger.Error("error processing actor acknowledgement effects", "error", err)
			} else {
				ozoneStatus = "ok"
				or.logEffect(&OspreyEffectLog{
					ActionName: evt.ActionName,
					ActionID:   evt.ActionId,
					Subject:    evt.Did,
					Kind:       "acknowledgement",
					Comment:    comment,
					CreatedAt:  time.Now(),
					Rules:      strings.Join(e.Rules, ","),
				})
			}
		case osprey.AtprotoSubjectKind_ATPROTO_SUBJECT_KIND_RECORD:
			if err := or.ozoneClient.EscalateRecord(
				ctx,
				evt.Uri,
				evt.Cid,
				ModToolMeta{
					Rules: strings.Join(e.Rules, ","),
				},
				e.Comment,
			); err != nil {
				or.logger.Error("error processing record acknowledgement effects", "error", err)
			} else {
				ozoneStatus = "ok"
				or.logEffect(&OspreyEffectLog{
					ActionName: evt.ActionName,
					ActionID:   evt.ActionId,
					Subject:    evt.Uri,
					Kind:       "acknowledgement",
					Comment:    comment,
					CreatedAt:  time.Now(),
					Rules:      strings.Join(e.Rules, ","),
				})
			}
		}
	}

	for _, e := range evt.Emails {
		ozoneStatus := "error"
		defer func() {
			ozoneRequests.WithLabelValues("comment", "actor", ozoneStatus).Inc()
		}()

		comment := fmt.Sprintf("Actioned by rules %s", strings.Join(e.Rules, ","))
		if e.Comment != nil {
			comment = fmt.Sprintf("%s\n\n%s", comment, *e.Comment)
		}

		// NOTE: Purposefully do not ignore duplicate actions for emails
		if err := or.ozoneClient.SendEmail(ctx, evt.Did, e.Email); err != nil {
			or.logger.Error("error processing email effects", "error", err)
		} else {
			ozoneStatus = "ok"
			or.logEffect(&OspreyEffectLog{
				ActionName: evt.ActionName,
				ActionID:   evt.ActionId,
				Subject:    evt.Uri,
				Kind:       "email",
				Comment:    comment,
				CreatedAt:  time.Now(),
				Rules:      strings.Join(e.Rules, ","),
			})
		}
	}

	for _, e := range evt.BigqueryFlags {
		// make sure we actually have a flag client
		if or.bigqueryFlagClient == nil {
			continue
		}

		// skip over anything that isn't a record, we shouldn't be sending those
		if e.SubjectKind != osprey.AtprotoSubjectKind_ATPROTO_SUBJECT_KIND_RECORD {
			continue
		}

		if err := or.bigqueryFlagClient.addFlag(context.Background(), evt.Uri, e.Tag); err != nil {
			or.logger.Error("error procesing bigquery flag effect", "error", err)
		}

		or.logEffect(&OspreyEffectLog{
			ActionName: evt.ActionName,
			ActionID:   evt.ActionId,
			Subject:    evt.Uri,
			Kind:       "bigquery-flag",
			Tag: bigquery.NullString{
				StringVal: e.Tag,
				Valid:     true,
			},
			CreatedAt: time.Now(),
			Rules:     strings.Join(e.Rules, ","),
		})
	}

	status = "ok"

	return nil
}

func (or *OspreyEffector) logEffect(log *OspreyEffectLog) {
	if err := or.logManager.LogEffect(context.Background(), log); err != nil {
		or.logger.Error("failed to log effect", "error", err)
	}
}

func createActionKey(subject string, ruleName string, expirationInHours *int64) string {
	// create the base key for the action
	key := fmt.Sprintf("%s-%s", subject, ruleName)
	// if there is an expiration in hours set, append it to the string. this allows us to have "incremental step ups"
	// for behavior that persists
	if expirationInHours != nil {
		key = fmt.Sprintf("%s-dur-%d", key, *expirationInHours)
	}
	return key
}

func (or *OspreyEffector) checkHasActioned(subject string, ruleName string, expirationInHours *int64) bool {
	key := createActionKey(subject, ruleName, expirationInHours)

	_, err := or.memClient.Get(key)
	if err != nil {
		if err != memcache.ErrCacheMiss {
			or.logger.Error("memcache lookup error", "err", err)
		}
	} else {
		// if this didn't error, then it's set in the cache
		return true
	}

	if err := or.memClient.Add(&memcache.Item{
		Key:        key,
		Value:      []byte("1"),
		Expiration: 0,
	}); err != nil {
		if err != memcache.ErrNotStored {
			or.logger.Error("memcache insert error", "err", err)
		}
	}

	// after storing, go ahead and return false so that we actually apply the action
	return false
}
