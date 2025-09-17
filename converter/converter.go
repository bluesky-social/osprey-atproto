package converter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bluesky-social/go-util/pkg/bus/cursor"
	"github.com/bluesky-social/go-util/pkg/bus/producer"
	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/data"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/bluesky-social/osprey-atproto/effector"
	osprey "github.com/bluesky-social/osprey-atproto/proto/go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/gorilla/websocket"
)

const (
	NAMESPACE = "osprey_effector"
)

var (
	reg = prometheus.NewRegistry()

	eventsReceived = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name:      "firehose_events_received",
		Namespace: NAMESPACE,
		Help:      "number of events received from the firehose, by collection",
	}, []string{"kind", "status"})

	eventsProduced = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name:      "events_produced",
		Namespace: NAMESPACE,
		Help:      "number of events produced, by collection and status",
	}, []string{"kind", "status"})

	ozoneRequests = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name:      "ozone_requests",
		Namespace: NAMESPACE,
		Help:      "number of requests to Ozone",
	}, []string{"type", "kind", "status"})

	plcRequests = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name:      "plc_requests",
		Namespace: NAMESPACE,
		Help:      "number of requests to PLC",
	}, []string{"status"})
)

type KafkaConverter struct {
	logger *slog.Logger

	producer *producer.Producer[*osprey.FirehoseEvent]
	cursor   *cursor.Cursor[*osprey.Cursor]

	ozoneClient *effector.OzoneClient

	lastCursor *int64
	cursorLk   sync.Mutex

	relayHost string

	bootstrapServers []string
	outputTopic      string
	cursorOverride   int64

	cursorSaveShutdown chan struct{}
	lastCursorSaved    chan struct{}
}

type Args struct {
	RelayHost        string
	BootstrapServers []string
	OutputTopic      string
	CursorOverride   int64
	Logger           *slog.Logger
}

func NewKafkaConverter(args *Args) (*KafkaConverter, error) {
	if len(args.BootstrapServers) == 0 {
		return nil, errors.New("no bootstrap servers provided to kafka converter")
	}

	if args.OutputTopic == "" {
		return nil, errors.New("no output topic provided to kafka converter")
	}

	if args.RelayHost == "" {
		return nil, errors.New("no relay host provided to kafka converter")
	}

	return &KafkaConverter{
		relayHost:          args.RelayHost,
		bootstrapServers:   args.BootstrapServers,
		outputTopic:        args.OutputTopic,
		cursorOverride:     args.CursorOverride,
		logger:             args.Logger,
		cursorSaveShutdown: make(chan struct{}),
		lastCursorSaved:    make(chan struct{}),
	}, nil
}

func (c *KafkaConverter) Run(ctx context.Context) error {
	c.logger.Info("starting kafka converter...")

	busProducer, err := producer.New(
		ctx,
		c.logger,
		c.bootstrapServers,
		c.outputTopic,
		producer.WithEnsureTopic[*osprey.FirehoseEvent](true),
		producer.WithTopicPartitions[*osprey.FirehoseEvent](200),
	)
	if err != nil {
		return fmt.Errorf("error creating new producer: %w", err)
	}
	defer busProducer.Close()
	c.producer = busProducer

	cursorOpts := []cursor.CursorOption[*osprey.Cursor]{}
	cursor, err := cursor.New(ctx, c.bootstrapServers, fmt.Sprintf("%s-producer-cursor", c.outputTopic), cursorOpts...)
	if err != nil {
		return fmt.Errorf("failed to create cursor for producer: %w", err)
	}
	defer cursor.Close()
	c.cursor = cursor

	u, err := url.Parse(c.relayHost)
	if err != nil {
		return fmt.Errorf("failed to parse relay host url: %w", err)
	}

	if c.cursorOverride >= 0 {
		c.logger.Info("cursor override set, starting from specified sequence", "cursor", c.cursorOverride)
		c.lastCursor = &c.cursorOverride
	} else {
		c.logger.Info("fetching last cursor from bus")
		if err := c.loadCursor(ctx); err != nil {
			return fmt.Errorf("failed to fetch or init cursor: %w", err)
		}
	}

	scheduler := parallel.NewScheduler(50, 100, "kafka_converter", c.handleStreamEvent)

	lastSeq := c.getCursor()
	if lastSeq != nil {
		q := u.Query()
		q.Set("cursor", fmt.Sprintf("%d", *lastSeq))
		u.RawQuery = q.Encode()
		c.logger.Info("using last cursor", "cursor", *lastSeq)
	} else {
		c.logger.Info("no previous cursor found, starting from live tail")
	}

	go c.periodicallySaveCursor(ctx)

	c.logger.Info("connecting to websocket", "host", u.String())

	con, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		c.logger.Error("failed to connect to websocket", "error", err)
		return err
	}
	defer con.Close()

	eventsKill := make(chan struct{})
	shutdownRepoStream := make(chan struct{})
	repoStreamShutdown := make(chan struct{})
	go func() {
		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)

		logger := c.logger.With("component", "consumer")

		go func() {
			err = events.HandleRepoStream(ctx, con, scheduler, logger)
			if !errors.Is(err, context.Canceled) || !errors.Is(err, net.ErrClosed) {
				logger.Warn("HandleRepoStream returned unexpectedly, killing converter", "error", err)
				close(eventsKill)
			} else {
				logger.Info("HandleRepoStream closed on context cancel")
			}
			close(repoStreamShutdown)
		}()
		<-shutdownRepoStream
		cancel()
	}()

	c.logger.Debug("registering OS exit signal handler")
	quit := make(chan struct{})
	exitSignals := make(chan os.Signal, 1)
	signal.Notify(exitSignals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		defer close(quit)

		select {
		case sig := <-exitSignals:
			c.logger.Info("received OS exit signal", "signal", sig)
		case <-eventsKill:
			c.logger.Warn("HandleRepoStream closed on events kill")
		}

		close(shutdownRepoStream)
		select {
		case <-repoStreamShutdown:
			c.logger.Info("firehose client finished processing")
		case <-time.After(5 * time.Second):
			c.logger.Warn("firehose client did not finish processing in time, forcing shutdown")
		}

		busProducer.Close()

		close(c.cursorSaveShutdown)
		select {
		case <-c.lastCursorSaved:
			c.logger.Info("cursor save loop finished")
		case <-time.After(5 * time.Second):
			c.logger.Warn("cursor save loop did not finish in time, forcing shutdown")
		}
	}()

	<-quit
	c.logger.Info("graceful shutdown complete")

	return nil
}

func (c *KafkaConverter) handleStreamEvent(ctx context.Context, xe *events.XRPCStreamEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	switch {
	case xe.RepoCommit != nil:
		if xe.RepoCommit.TooBig {
			// TODO: deprecated right?
			c.logger.Warn("repo commit too big", "repo", xe.RepoCommit.Repo, "seq", xe.RepoCommit.Seq, "rev", xe.RepoCommit.Rev)
			return nil
		}
		return c.handleRepoCommit(ctx, xe.RepoCommit)
	case xe.RepoIdentity != nil:
		receiveStatus := "error"
		defer func() {
			c.updateCursor(xe.RepoIdentity.Seq)
			eventsReceived.WithLabelValues("identity", receiveStatus).Inc()
		}()

		t, err := time.Parse(time.RFC3339, xe.RepoIdentity.Time)
		if err != nil {
			c.logger.Error("error parsing time", "error", err)
			return nil
		}

		payload, err := json.Marshal(xe.RepoIdentity)
		if err != nil {
			c.logger.Error("error marshalling identity event", "error", err)
			return nil
		}

		// Emit identity update
		e := osprey.FirehoseEvent{
			Did:       xe.RepoIdentity.Did,
			Timestamp: timestamppb.New(t),
			Kind:      osprey.EventKind_EVENT_KIND_IDENTITY,
			Identity:  payload,
		}

		produceStatus := "error"
		defer func() {
			eventsProduced.WithLabelValues("identity", produceStatus).Inc()
		}()

		if err := c.producer.ProduceAsync(context.Background(), xe.RepoIdentity.Did, &e, nil); err != nil {
			c.logger.Error("failed to produce identity message to Kafka", "error", err, "key", xe.RepoIdentity.Did)
		} else {
			produceStatus = "ok"
		}
		receiveStatus = "ok"

	case xe.RepoAccount != nil:
		receiveStatus := "error"
		defer func() {
			eventsReceived.WithLabelValues("account", receiveStatus).Inc()
			c.updateCursor(xe.RepoAccount.Seq)
		}()

		t, err := time.Parse(time.RFC3339, xe.RepoAccount.Time)
		if err != nil {
			c.logger.Error("error parsing time", "error", err)
			return nil
		}

		payload, err := json.Marshal(xe.RepoAccount)
		if err != nil {
			c.logger.Error("error marshalling account event", "error", err)
			return nil
		}

		e := osprey.FirehoseEvent{
			Did:       xe.RepoAccount.Did,
			Timestamp: timestamppb.New(t),
			Kind:      osprey.EventKind_EVENT_KIND_ACCOUNT,
			Account:   payload,
		}

		produceStatus := "error"
		defer func() {
			eventsProduced.WithLabelValues("account", produceStatus).Inc()
		}()

		if err := c.producer.ProduceAsync(context.Background(), xe.RepoAccount.Did, &e, nil); err != nil {
			c.logger.Error("failed to produce account message to Kafka", "error", err, "key", xe.RepoAccount.Did)
		} else {
			produceStatus = "ok"
		}

		receiveStatus = "ok"

	case xe.Error != nil:
		eventsReceived.WithLabelValues("error", "ok").Inc()
		return fmt.Errorf("error from firehose: %s", xe.Error.Message)
	}
	return nil
}

func (c *KafkaConverter) handleRepoCommit(ctx context.Context, evt *atproto.SyncSubscribeRepos_Commit) error {
	defer func() {
		c.updateCursor(evt.Seq)
	}()

	logger := c.logger.With("repo", evt.Repo, "seq", evt.Seq, "commit", evt.Commit.String())

	rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.Blocks))
	if err != nil {
		logger.Error("failed to read repo from car", "error", err)
		return nil
	}

	evtCreatedAt, err := time.Parse(time.RFC3339, evt.Time)
	if err != nil {
		logger.Error("error parsing time", "error", err)
		return nil
	}

	for _, op := range evt.Ops {
		func() {
			// TODO: we probably should move this to like optsProcessed instead of using eventsReceived
			receiveStatus := "error"
			defer func() {
				eventsReceived.WithLabelValues("commit", receiveStatus).Inc()
			}()

			collection := strings.Split(op.Path, "/")[0]
			rkey := strings.Split(op.Path, "/")[1]

			ek := repomgr.EventKind(op.Action)
			logger = logger.With("action", op.Action, "collection", collection)

			e := osprey.FirehoseEvent{
				Did:       evt.Repo,
				Kind:      osprey.EventKind_EVENT_KIND_COMMIT,
				Timestamp: timestamppb.New(evtCreatedAt),
			}

			switch ek {
			case repomgr.EvtKindCreateRecord:
				if op.Cid == nil {
					logger.Error("update record op missing cid")
					return
				}

				rcid, recB, err := rr.GetRecordBytes(ctx, op.Path)
				if err != nil {
					logger.Error("failed to get record bytes", "error", err)
					return
				}

				recCid := rcid.String()
				if recCid != op.Cid.String() {
					logger.Error("record cid mismatch", "expected", *op.Cid, "actual", rcid)
					return
				}

				rec, err := data.UnmarshalCBOR(*recB)
				if err != nil {
					logger.Error("failed to unmarshal record", "error", err)
					return
				}

				recJSON, err := json.Marshal(rec)
				if err != nil {
					logger.Error("failed to marshal record to json", "error", err)
					return
				}

				e.Commit = &osprey.Commit{
					Rev:        evt.Rev,
					Operation:  osprey.CommitOperation_COMMIT_OPERATION_CREATE,
					Collection: collection,
					Rkey:       rkey,
					Record:     recJSON,
					Cid:        recCid,
				}
			case repomgr.EvtKindUpdateRecord:
				if op.Cid == nil {
					logger.Error("update record op missing cid")
					return
				}

				rcid, recB, err := rr.GetRecordBytes(ctx, op.Path)
				if err != nil {
					logger.Error("failed to get record bytes", "error", err)
					return
				}

				recCid := rcid.String()
				if recCid != op.Cid.String() {
					logger.Error("record cid mismatch", "expected", *op.Cid, "actual", rcid)
					return
				}

				rec, err := data.UnmarshalCBOR(*recB)
				if err != nil {
					logger.Error("failed to unmarshal record", "error", err)
					return
				}

				recJSON, err := json.Marshal(rec)
				if err != nil {
					logger.Error("failed to marshal record to json", "error", err)
					return
				}

				e.Commit = &osprey.Commit{
					Rev:        evt.Rev,
					Operation:  osprey.CommitOperation_COMMIT_OPERATION_UPDATE,
					Collection: collection,
					Rkey:       rkey,
					Record:     recJSON,
					Cid:        recCid,
				}
			case repomgr.EvtKindDeleteRecord:
				// Emit the delete
				e.Commit = &osprey.Commit{
					Rev:        evt.Rev,
					Operation:  osprey.CommitOperation_COMMIT_OPERATION_DELETE,
					Collection: collection,
					Rkey:       rkey,
				}
			default:
				logger.Warn("unknown event kind from op action", "kind", op.Action)
				return
			}

			produceStatus := "error"
			defer func() {
				eventsProduced.WithLabelValues("commit", produceStatus).Inc()
			}()

			if err := c.producer.ProduceAsync(context.Background(), evt.Repo, &e, nil); err != nil {
				logger.Error("failed to produce message to Kafka", "error", err, "key", evt.Repo)
			} else {
				produceStatus = "ok"
			}
			receiveStatus = "ok"
		}()
	}
	return nil
}

func (c *KafkaConverter) updateCursor(seq int64) {
	c.cursorLk.Lock()
	defer c.cursorLk.Unlock()

	if c.lastCursor == nil || seq > *c.lastCursor {
		c.lastCursor = &seq
	}
}

func isFinalConverterCursor(c *osprey.Cursor) bool {
	// If the cursor has SavedOnExit set, it means this is the final cursor save
	return c != nil && c.SavedOnExit
}

func (c *KafkaConverter) loadCursor(ctx context.Context) error {
	c.cursorLk.Lock()
	defer c.cursorLk.Unlock()

	if cur, err := c.cursor.Load(ctx, isFinalConverterCursor); err != nil {
		return errors.Join(err, errors.New("failed to load cursor"))
	} else if cur != nil {
		c.lastCursor = &cur.Sequence
		c.logger.Info("loaded last cursor", "cursor", c.lastCursor)
	} else {
		c.logger.Info("no previous cursor found, starting fresh")
	}
	return nil
}

func (c *KafkaConverter) getCursor() *int64 {
	c.cursorLk.Lock()
	defer c.cursorLk.Unlock()

	return c.lastCursor
}

func (c *KafkaConverter) periodicallySaveCursor(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	defer func() {
		// Save one last time before exiting
		c.cursorLk.Lock()
		if c.lastCursor != nil {
			// Let the next consumer know this was the final cursor save
			finalCursor := osprey.Cursor{Sequence: *c.lastCursor, SavedOnExit: true}
			if err := c.cursor.Save(context.Background(), &finalCursor); err != nil {
				c.logger.Error("failed to save cursor on exit", "err", err)
			} else {
				c.logger.Info("saved cursor on exit", "cursor", *c.lastCursor)
			}
		}
		c.cursorLk.Unlock()
		close(c.lastCursorSaved)
	}()

	for {
		select {
		case <-c.cursorSaveShutdown:
			c.logger.Info("exit signal received, stopping cursor save loop")
			return
		case <-ticker.C:
			c.cursorLk.Lock()
			last := c.lastCursor
			c.cursorLk.Unlock()
			if last != nil {
				if err := c.cursor.Save(ctx, &osprey.Cursor{Sequence: *last}); err != nil {
					c.logger.Error("failed to save cursor", "err", err)
				} else {
					c.logger.Info("saved cursor", "sequence", *last)
				}
			}
		}
	}
}
