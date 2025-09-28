package effector

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/ozone"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/xrpc"
	osprey "github.com/bluesky-social/osprey-atproto/proto/go"
	"github.com/golang-jwt/jwt/v5"
)

const (
	ClientName = "osprey-effector"
)

var (
	NeedsReviewMaxTime = int64(7 * 24)
)

type OzoneClient struct {
	client    atomic.Value
	refreshMu sync.Mutex
	logger    *slog.Logger

	templates []CommunicationTemplate

	isProduction bool
}

type OzoneClientArgs struct {
	PdsHost    string
	Identifier string
	Password   string

	Logger *slog.Logger

	IsProduction bool

	ProxyDid string
}

type ModToolMeta struct {
	Rules    string `json:"rules"`
	ActionID int64  `json:"actionId"`
}

func NewOzoneClient(ctx context.Context, args *OzoneClientArgs) (*OzoneClient, error) {
	if args.Logger == nil {
		args.Logger = slog.Default()
	}

	args.Logger = args.Logger.With("component", "ozone_client")

	oc := &OzoneClient{
		logger:       args.Logger,
		isProduction: args.IsProduction,
	}

	cli := &xrpc.Client{
		Host: args.PdsHost,
		Headers: map[string]string{
			"atproto-proxy": fmt.Sprintf("%s#atproto_labeler", args.ProxyDid),
		},
	}

	resp, err := atproto.ServerCreateSession(ctx, cli, &atproto.ServerCreateSession_Input{
		Identifier: args.Identifier,
		Password:   args.Password,
	})
	if err != nil {
		return nil, fmt.Errorf("could not create auth session: %w", err)
	}

	cli.Auth = &xrpc.AuthInfo{
		AccessJwt:  resp.AccessJwt,
		RefreshJwt: resp.RefreshJwt,
		Handle:     resp.Handle,
		Did:        resp.Did,
	}

	oc.client.Store(cli)

	return oc, nil
}

func (oc *OzoneClient) GetClient(ctx context.Context) (*xrpc.Client, error) {
	client := oc.client.Load().(*xrpc.Client)

	token, _, _ := new(jwt.Parser).ParseUnverified(client.Auth.AccessJwt, jwt.MapClaims{})
	if claims, ok := token.Claims.(jwt.MapClaims); ok {
		if exp, ok := claims["exp"].(float64); ok {
			expiration := time.Unix(int64(exp), 0)
			if time.Until(expiration) > 5*time.Minute {
				return client, nil
			}
		}
	}

	oc.refreshMu.Lock()
	defer oc.refreshMu.Unlock()

	client = oc.client.Load().(*xrpc.Client)
	token, _, _ = new(jwt.Parser).ParseUnverified(client.Auth.AccessJwt, jwt.MapClaims{})
	if claims, ok := token.Claims.(jwt.MapClaims); ok {
		if exp, ok := claims["exp"].(float64); ok {
			expiration := time.Unix(int64(exp), 0)
			if time.Until(expiration) > 5*time.Minute {
				return client, nil
			}
		}
	}

	oc.logger.Info("refreshing auth token...")

	tempClient := &xrpc.Client{
		Host:    client.Host,
		Headers: client.Headers,
		Auth: &xrpc.AuthInfo{
			AccessJwt:  client.Auth.RefreshJwt,
			RefreshJwt: client.Auth.RefreshJwt,
			Handle:     client.Auth.Handle,
			Did:        client.Auth.Did,
		},
	}

	res, err := atproto.ServerRefreshSession(ctx, tempClient)
	if err != nil {
		oc.logger.Error("error refreshing session", "error", err)
		return client, fmt.Errorf("failed to refresh token: %w", err)
	}

	newClient := &xrpc.Client{
		Host:    client.Host,
		Headers: client.Headers,
		Auth: &xrpc.AuthInfo{
			AccessJwt:  res.AccessJwt,
			RefreshJwt: res.RefreshJwt,
			Handle:     client.Auth.Handle,
			Did:        client.Auth.Did,
		},
	}

	oc.client.Store(newClient)
	oc.logger.Info("ozone session refreshed")

	return newClient, nil
}

func (oc *OzoneClient) TakedownActor(ctx context.Context, did string, meta ModToolMeta, comment string, emailTemplate *osprey.AtprotoEmail, reverse bool) error {
	status := "error"
	defer func() {
		effectsProcessed.WithLabelValues("takedown-actor", status).Inc()
	}()

	if oc.isProduction {
		cli, err := oc.GetClient(ctx)
		if err != nil {
			return err
		}

		t := true
		var met *ozone.ModerationDefs_ModEventTakedown
		var mert *ozone.ModerationDefs_ModEventReverseTakedown
		if !reverse {
			met = &ozone.ModerationDefs_ModEventTakedown{
				Comment:                    &comment,
				AcknowledgeAccountSubjects: &t,
			}
		} else {
			mert = &ozone.ModerationDefs_ModEventReverseTakedown{
				Comment: &comment,
			}
		}

		if _, err := ozone.ModerationEmitEvent(ctx, cli, &ozone.ModerationEmitEvent_Input{
			CreatedBy: cli.Auth.Did,
			Event: &ozone.ModerationEmitEvent_Input_Event{
				ModerationDefs_ModEventTakedown:        met,
				ModerationDefs_ModEventReverseTakedown: mert,
			},
			Subject: &ozone.ModerationEmitEvent_Input_Subject{
				AdminDefs_RepoRef: &atproto.AdminDefs_RepoRef{
					Did: did,
				},
			},
			ModTool: &ozone.ModerationDefs_ModTool{
				Name: ClientName,
				Meta: metaToInterface(meta),
			},
		}); err != nil {
			return err
		}

		if emailTemplate != nil {
			if err := oc.SendEmail(ctx, did, *emailTemplate); err != nil {
				return err
			}
		}
	}

	status = "ok"
	return nil
}

func (oc *OzoneClient) TakedownRecord(ctx context.Context, uri string, cid string, meta ModToolMeta, comment string, emailTemplate *osprey.AtprotoEmail, reverse bool) error {
	status := "error"
	defer func() {
		effectsProcessed.WithLabelValues("takedown-record", status).Inc()
	}()

	aturi, err := syntax.ParseATURI(uri)
	if err != nil {
		return fmt.Errorf("failed to parse aturi paseed to TakedownRecord: %w", err)
	}

	if oc.isProduction {
		cli, err := oc.GetClient(ctx)
		if err != nil {
			return err
		}

		t := true
		var met *ozone.ModerationDefs_ModEventTakedown
		var mert *ozone.ModerationDefs_ModEventReverseTakedown
		if !reverse {
			met = &ozone.ModerationDefs_ModEventTakedown{
				Comment:                    &comment,
				AcknowledgeAccountSubjects: &t,
			}
		} else {
			mert = &ozone.ModerationDefs_ModEventReverseTakedown{
				Comment: &comment,
			}
		}

		if _, err := ozone.ModerationEmitEvent(ctx, cli, &ozone.ModerationEmitEvent_Input{
			CreatedBy: cli.Auth.Did,
			Event: &ozone.ModerationEmitEvent_Input_Event{
				ModerationDefs_ModEventTakedown:        met,
				ModerationDefs_ModEventReverseTakedown: mert,
			},
			Subject: &ozone.ModerationEmitEvent_Input_Subject{
				RepoStrongRef: &atproto.RepoStrongRef{
					Uri: uri,
					Cid: cid,
				},
			},
			ModTool: &ozone.ModerationDefs_ModTool{
				Name: ClientName,
				Meta: metaToInterface(meta),
			},
		}); err != nil {
			return err
		}

		if emailTemplate != nil {
			if err := oc.SendEmail(ctx, aturi.Authority().String(), *emailTemplate); err != nil {
				return err
			}
		}
	}

	status = "ok"
	return nil
}

func (oc *OzoneClient) LabelActor(ctx context.Context, did string, meta ModToolMeta, label osprey.AtprotoLabel, comment string, email *osprey.AtprotoEmail, durationInHours *int64, neg bool) error {
	status := "error"
	defer func() {
		effectsProcessed.WithLabelValues("label-actor", status).Inc()
	}()

	if oc.isProduction {
		if label == osprey.AtprotoLabel_ATPROTO_LABEL_NEEDS_REVIEW {
			if durationInHours == nil || *durationInHours > NeedsReviewMaxTime {
				durationInHours = &NeedsReviewMaxTime
			}
		}

		labelStr := AtprotoLabelToString(label)

		cli, err := oc.GetClient(ctx)
		if err != nil {
			return err
		}

		cvals := []string{}
		nvals := []string{}
		if neg {
			nvals = append(nvals, labelStr)
		} else {
			cvals = append(cvals, labelStr)
		}

		if _, err := ozone.ModerationEmitEvent(ctx, cli, &ozone.ModerationEmitEvent_Input{
			CreatedBy: cli.Auth.Did,
			Event: &ozone.ModerationEmitEvent_Input_Event{
				ModerationDefs_ModEventLabel: &ozone.ModerationDefs_ModEventLabel{
					CreateLabelVals: cvals,
					NegateLabelVals: nvals,
					Comment:         &comment,
					DurationInHours: durationInHours,
				},
			},
			Subject: &ozone.ModerationEmitEvent_Input_Subject{
				AdminDefs_RepoRef: &atproto.AdminDefs_RepoRef{
					Did: did,
				},
			},
			ModTool: &ozone.ModerationDefs_ModTool{
				Name: ClientName,
				Meta: metaToInterface(meta),
			},
		}); err != nil {
			return err
		}

		if email != nil {
			if err := oc.SendEmail(ctx, did, *email); err != nil {
				return err
			}
		}
	}

	status = "ok"
	return nil
}

func (oc *OzoneClient) LabelRecord(ctx context.Context, uri string, cid string, meta ModToolMeta, label osprey.AtprotoLabel, comment string, email *osprey.AtprotoEmail, durationInHours *int64, neg bool) error {
	status := "error"
	defer func() {
		effectsProcessed.WithLabelValues("label-record", status).Inc()
	}()

	aturi, err := syntax.ParseATURI(uri)
	if err != nil {
		return fmt.Errorf("failed to parse aturi paseed to LabelRecord: %w", err)
	}

	if oc.isProduction {
		if label == osprey.AtprotoLabel_ATPROTO_LABEL_NEEDS_REVIEW {
			if durationInHours == nil || *durationInHours > NeedsReviewMaxTime {
				durationInHours = &NeedsReviewMaxTime
			}
		}

		labelStr := AtprotoLabelToString(label)

		cli, err := oc.GetClient(ctx)
		if err != nil {
			return err
		}

		cvals := []string{}
		nvals := []string{}
		if neg {
			nvals = append(nvals, labelStr)
		} else {
			cvals = append(cvals, labelStr)
		}

		if _, err := ozone.ModerationEmitEvent(ctx, cli, &ozone.ModerationEmitEvent_Input{
			CreatedBy: cli.Auth.Did,
			Event: &ozone.ModerationEmitEvent_Input_Event{
				ModerationDefs_ModEventLabel: &ozone.ModerationDefs_ModEventLabel{
					CreateLabelVals: cvals,
					NegateLabelVals: nvals,
					Comment:         &comment,
					DurationInHours: durationInHours,
				},
			},
			Subject: &ozone.ModerationEmitEvent_Input_Subject{
				RepoStrongRef: &atproto.RepoStrongRef{
					Uri: uri,
					Cid: cid,
				},
			},
			ModTool: &ozone.ModerationDefs_ModTool{
				Name: ClientName,
				Meta: metaToInterface(meta),
			},
		}); err != nil {
			return err
		}

		if email != nil {
			if err := oc.SendEmail(ctx, aturi.Authority().String(), *email); err != nil {
				return err
			}
		}
	}

	status = "ok"
	return nil
}

func (oc *OzoneClient) TagActor(ctx context.Context, did string, meta ModToolMeta, tag string, comment *string, neg bool) error {
	status := "error"
	defer func() {
		effectsProcessed.WithLabelValues("tag-actor", status).Inc()
	}()

	if oc.isProduction {
		cli, err := oc.GetClient(ctx)
		if err != nil {
			return err
		}

		add := []string{}
		remove := []string{}
		if neg {
			remove = append(remove, tag)
		} else {
			add = append(add, tag)
		}

		if _, err := ozone.ModerationEmitEvent(ctx, cli, &ozone.ModerationEmitEvent_Input{
			CreatedBy: cli.Auth.Did,
			Event: &ozone.ModerationEmitEvent_Input_Event{
				ModerationDefs_ModEventTag: &ozone.ModerationDefs_ModEventTag{
					Add:     add,
					Remove:  remove,
					Comment: comment,
				},
			},
			Subject: &ozone.ModerationEmitEvent_Input_Subject{
				AdminDefs_RepoRef: &atproto.AdminDefs_RepoRef{
					Did: did,
				},
			},
			ModTool: &ozone.ModerationDefs_ModTool{
				Name: ClientName,
				Meta: metaToInterface(meta),
			},
		}); err != nil {
			return err
		}
	}

	status = "ok"
	return nil
}

func (oc *OzoneClient) TagRecord(ctx context.Context, uri string, cid string, meta ModToolMeta, tag string, comment *string, neg bool) error {
	status := "error"
	defer func() {
		effectsProcessed.WithLabelValues("tag-record", status).Inc()
	}()

	_, err := syntax.ParseATURI(uri)
	if err != nil {
		return fmt.Errorf("failed to parse aturi paseed to TagRecord: %w", err)
	}

	if oc.isProduction {
		cli, err := oc.GetClient(ctx)
		if err != nil {
			return err
		}

		add := []string{}
		remove := []string{}
		if neg {
			remove = append(remove, tag)
		} else {
			add = append(add, tag)
		}

		if _, err := ozone.ModerationEmitEvent(ctx, cli, &ozone.ModerationEmitEvent_Input{
			CreatedBy: cli.Auth.Did,
			Event: &ozone.ModerationEmitEvent_Input_Event{
				ModerationDefs_ModEventTag: &ozone.ModerationDefs_ModEventTag{
					Add:     add,
					Remove:  remove,
					Comment: comment,
				},
			},
			Subject: &ozone.ModerationEmitEvent_Input_Subject{
				RepoStrongRef: &atproto.RepoStrongRef{
					Uri: uri,
					Cid: cid,
				},
			},
			ModTool: &ozone.ModerationDefs_ModTool{
				Name: ClientName,
				Meta: metaToInterface(meta),
			},
		}); err != nil {
			return err
		}
	}

	status = "ok"
	return nil
}

func (oc *OzoneClient) CommentActor(ctx context.Context, did string, meta ModToolMeta, comment string) error {
	status := "error"
	defer func() {
		effectsProcessed.WithLabelValues("comment-actor", status).Inc()
	}()

	if oc.isProduction {
		cli, err := oc.GetClient(ctx)
		if err != nil {
			return err
		}

		if _, err := ozone.ModerationEmitEvent(ctx, cli, &ozone.ModerationEmitEvent_Input{
			CreatedBy: cli.Auth.Did,
			Event: &ozone.ModerationEmitEvent_Input_Event{
				ModerationDefs_ModEventComment: &ozone.ModerationDefs_ModEventComment{
					Comment: &comment,
				},
			},
			Subject: &ozone.ModerationEmitEvent_Input_Subject{
				AdminDefs_RepoRef: &atproto.AdminDefs_RepoRef{
					Did: did,
				},
			},
			ModTool: &ozone.ModerationDefs_ModTool{
				Name: ClientName,
				Meta: metaToInterface(meta),
			},
		}); err != nil {
			return err
		}
	}

	status = "ok"
	return nil
}

func (oc *OzoneClient) CommentRecord(ctx context.Context, uri string, cid string, meta ModToolMeta, comment string) error {
	status := "error"
	defer func() {
		effectsProcessed.WithLabelValues("comment-record", status).Inc()
	}()

	aturi, err := syntax.ParseATURI(uri)
	if err != nil {
		return fmt.Errorf("failed to parse aturi paseed to CommentRecord: %w", err)
	}

	// TODO: remove this debug comment
	isHailey := aturi.Authority().String() == "did:plc:oisofpd7lj26yvgiivf3lxsi"

	if oc.isProduction || isHailey {
		cli, err := oc.GetClient(ctx)
		if err != nil {
			return err
		}

		if _, err := ozone.ModerationEmitEvent(ctx, cli, &ozone.ModerationEmitEvent_Input{
			CreatedBy: cli.Auth.Did,
			Event: &ozone.ModerationEmitEvent_Input_Event{
				ModerationDefs_ModEventComment: &ozone.ModerationDefs_ModEventComment{
					Comment: &comment,
				},
			},
			Subject: &ozone.ModerationEmitEvent_Input_Subject{
				RepoStrongRef: &atproto.RepoStrongRef{
					Uri: uri,
					Cid: cid,
				},
			},
			ModTool: &ozone.ModerationDefs_ModTool{
				Name: ClientName,
				Meta: metaToInterface(meta),
			},
		}); err != nil {
			return err
		}
	}

	status = "ok"
	return nil
}

func (oc *OzoneClient) ReportActor(ctx context.Context, did string, meta ModToolMeta, reportType osprey.AtprotoReportKind, comment string, priorityScore *int64) error {
	status := "error"
	defer func() {
		effectsProcessed.WithLabelValues("report-actor", status).Inc()
	}()

	if oc.isProduction {
		cli, err := oc.GetClient(ctx)
		if err != nil {
			return err
		}

		reportTypeStr := AtprotoReportKindToString(reportType)

		if _, err := ozone.ModerationEmitEvent(ctx, cli, &ozone.ModerationEmitEvent_Input{
			CreatedBy: cli.Auth.Did,
			Event: &ozone.ModerationEmitEvent_Input_Event{
				ModerationDefs_ModEventReport: &ozone.ModerationDefs_ModEventReport{
					ReportType: &reportTypeStr,
					Comment:    &comment,
				},
			},
			Subject: &ozone.ModerationEmitEvent_Input_Subject{
				AdminDefs_RepoRef: &atproto.AdminDefs_RepoRef{
					Did: did,
				},
			},
			ModTool: &ozone.ModerationDefs_ModTool{
				Name: ClientName,
				Meta: metaToInterface(meta),
			},
		}); err != nil {
			return err
		}

		if priorityScore != nil {
			_, err := ozone.ModerationEmitEvent(ctx, cli, &ozone.ModerationEmitEvent_Input{
				CreatedBy: cli.Auth.Did,
				Event: &ozone.ModerationEmitEvent_Input_Event{
					ModerationDefs_ModEventPriorityScore: &ozone.ModerationDefs_ModEventPriorityScore{
						Comment: &comment,
						Score:   *priorityScore,
					},
				},
				Subject: &ozone.ModerationEmitEvent_Input_Subject{
					AdminDefs_RepoRef: &atproto.AdminDefs_RepoRef{
						Did: did,
					},
				},
				ModTool: &ozone.ModerationDefs_ModTool{
					Name: ClientName,
					Meta: metaToInterface(meta),
				},
			})

			if err != nil {
				return err
			}
		}
	}

	status = "ok"

	return nil
}

func (oc *OzoneClient) ReportRecord(ctx context.Context, uri string, cid string, meta ModToolMeta, reportType osprey.AtprotoReportKind, comment string, priorityScore *int64) error {
	status := "error"
	defer func() {
		effectsProcessed.WithLabelValues("report-record", status).Inc()
	}()

	_, err := syntax.ParseATURI(uri)
	if err != nil {
		return fmt.Errorf("failed to parse aturi paseed to ReportRecord: %w", err)
	}

	if oc.isProduction {
		cli, err := oc.GetClient(ctx)
		if err != nil {
			return err
		}

		reportTypeStr := AtprotoReportKindToString(reportType)

		if _, err := ozone.ModerationEmitEvent(ctx, cli, &ozone.ModerationEmitEvent_Input{
			CreatedBy: cli.Auth.Did,
			Event: &ozone.ModerationEmitEvent_Input_Event{
				ModerationDefs_ModEventReport: &ozone.ModerationDefs_ModEventReport{
					ReportType: &reportTypeStr,
					Comment:    &comment,
				},
			},
			Subject: &ozone.ModerationEmitEvent_Input_Subject{
				RepoStrongRef: &atproto.RepoStrongRef{
					Uri: uri,
					Cid: cid,
				},
			},
			ModTool: &ozone.ModerationDefs_ModTool{
				Name: ClientName,
				Meta: metaToInterface(meta),
			},
		}); err != nil {
			return err
		}

		if priorityScore != nil {
			_, err := ozone.ModerationEmitEvent(ctx, cli, &ozone.ModerationEmitEvent_Input{
				CreatedBy: cli.Auth.Did,
				Event: &ozone.ModerationEmitEvent_Input_Event{
					ModerationDefs_ModEventPriorityScore: &ozone.ModerationDefs_ModEventPriorityScore{
						Comment: &comment,
						Score:   *priorityScore,
					},
				},
				Subject: &ozone.ModerationEmitEvent_Input_Subject{
					RepoStrongRef: &atproto.RepoStrongRef{
						Uri: uri,
						Cid: cid,
					},
				},
				ModTool: &ozone.ModerationDefs_ModTool{
					Name: ClientName,
					Meta: metaToInterface(meta),
				},
			})

			if err != nil {
				return err
			}
		}
	}

	status = "ok"
	return nil
}

func (oc *OzoneClient) EscalateActor(ctx context.Context, did string, meta ModToolMeta, comment *string) error {
	status := "error"
	defer func() {
		effectsProcessed.WithLabelValues("escalate-actor", status).Inc()
	}()

	if oc.isProduction {
		cli, err := oc.GetClient(ctx)
		if err != nil {
			return err
		}

		if _, err := ozone.ModerationEmitEvent(ctx, cli, &ozone.ModerationEmitEvent_Input{
			CreatedBy: cli.Auth.Did,
			Event: &ozone.ModerationEmitEvent_Input_Event{
				ModerationDefs_ModEventEscalate: &ozone.ModerationDefs_ModEventEscalate{
					Comment: comment,
				},
			},
			Subject: &ozone.ModerationEmitEvent_Input_Subject{
				AdminDefs_RepoRef: &atproto.AdminDefs_RepoRef{
					Did: did,
				},
			},
			ModTool: &ozone.ModerationDefs_ModTool{
				Name: ClientName,
				Meta: metaToInterface(meta),
			},
		}); err != nil {
			return err
		}
	}

	status = "ok"
	return nil
}

func (oc *OzoneClient) EscalateRecord(ctx context.Context, uri string, cid string, meta ModToolMeta, comment *string) error {
	status := "error"
	defer func() {
		effectsProcessed.WithLabelValues("escalate-", status).Inc()
	}()

	_, err := syntax.ParseATURI(uri)
	if err != nil {
		return fmt.Errorf("failed to parse aturi paseed to EscalateRecord: %w", err)
	}

	if oc.isProduction {
		cli, err := oc.GetClient(ctx)
		if err != nil {
			return err
		}

		if _, err := ozone.ModerationEmitEvent(ctx, cli, &ozone.ModerationEmitEvent_Input{
			CreatedBy: cli.Auth.Did,
			Event: &ozone.ModerationEmitEvent_Input_Event{
				ModerationDefs_ModEventEscalate: &ozone.ModerationDefs_ModEventEscalate{
					Comment: comment,
				},
			},
			Subject: &ozone.ModerationEmitEvent_Input_Subject{
				RepoStrongRef: &atproto.RepoStrongRef{
					Uri: uri,
					Cid: cid,
				},
			},
			ModTool: &ozone.ModerationDefs_ModTool{
				Name: ClientName,
				Meta: metaToInterface(meta),
			},
		}); err != nil {
			return err
		}
	}

	status = "ok"
	return nil
}

func (oc *OzoneClient) AcknowledgeActor(ctx context.Context, did string, meta ModToolMeta, comment *string) error {
	status := "error"
	defer func() {
		effectsProcessed.WithLabelValues("acknowledge-actor", status).Inc()
	}()

	if oc.isProduction {
		cli, err := oc.GetClient(ctx)
		if err != nil {
			return err
		}

		if _, err := ozone.ModerationEmitEvent(ctx, cli, &ozone.ModerationEmitEvent_Input{
			CreatedBy: cli.Auth.Did,
			Event: &ozone.ModerationEmitEvent_Input_Event{
				ModerationDefs_ModEventAcknowledge: &ozone.ModerationDefs_ModEventAcknowledge{
					Comment: comment,
				},
			},
			Subject: &ozone.ModerationEmitEvent_Input_Subject{
				AdminDefs_RepoRef: &atproto.AdminDefs_RepoRef{
					Did: did,
				},
			},
			ModTool: &ozone.ModerationDefs_ModTool{
				Name: ClientName,
				Meta: metaToInterface(meta),
			},
		}); err != nil {
			return err
		}
	}

	status = "ok"
	return nil
}

func (oc *OzoneClient) AcknowledgeRecord(ctx context.Context, uri string, cid string, meta ModToolMeta, comment *string) error {
	status := "error"
	defer func() {
		effectsProcessed.WithLabelValues("acknowledge-record", status).Inc()
	}()

	_, err := syntax.ParseATURI(uri)
	if err != nil {
		return fmt.Errorf("failed to parse aturi paseed to AcknowledgeRecord: %w", err)
	}

	if oc.isProduction {
		cli, err := oc.GetClient(ctx)
		if err != nil {
			return err
		}

		if _, err := ozone.ModerationEmitEvent(ctx, cli, &ozone.ModerationEmitEvent_Input{
			CreatedBy: cli.Auth.Did,
			Event: &ozone.ModerationEmitEvent_Input_Event{
				ModerationDefs_ModEventAcknowledge: &ozone.ModerationDefs_ModEventAcknowledge{
					Comment: comment,
				},
			},
			Subject: &ozone.ModerationEmitEvent_Input_Subject{
				RepoStrongRef: &atproto.RepoStrongRef{
					Uri: uri,
					Cid: cid,
				},
			},
			ModTool: &ozone.ModerationDefs_ModTool{
				Name: ClientName,
				Meta: metaToInterface(meta),
			},
		}); err != nil {
			return err
		}
	}

	status = "ok"
	return nil
}

func (oc *OzoneClient) ResolveHandle(ctx context.Context, did string) (string, error) {
	// TODO: actually return a handle
	return "", nil
}

// TODO: send an email
func (oc *OzoneClient) SendEmail(ctx context.Context, did string, emailTemplate osprey.AtprotoEmail) error {
	return nil
}

func AtprotoReportKindToString(kind osprey.AtprotoReportKind) string {
	switch kind {
	case osprey.AtprotoReportKind_ATPROTO_REPORT_KIND_SPAM:
		return "com.atproto.moderation.defs#reasonSpam"
	case osprey.AtprotoReportKind_ATPROTO_REPORT_KIND_VIOLATION:
		return "com.atproto.moderation.defs#reasonViolation"
	case osprey.AtprotoReportKind_ATPROTO_REPORT_KIND_MISLEADING:
		return "com.atproto.moderation.defs#reasonMisleading"
	case osprey.AtprotoReportKind_ATPROTO_REPORT_KIND_SEXUAL:
		return "com.atproto.moderation.defs#reasonSexual"
	case osprey.AtprotoReportKind_ATPROTO_REPORT_KIND_RUDE:
		return "com.atproto.moderation.defs#reasonRude"
	case osprey.AtprotoReportKind_ATPROTO_REPORT_KIND_OTHER:
		return "com.atproto.moderation.defs#reasonOther"
	default:
		panic(fmt.Sprintf("unexpected osprey.AtprotoReportKind: %#v", kind))
	}
}

func AtprotoLabelToString(label osprey.AtprotoLabel) string {
	switch label {
	case osprey.AtprotoLabel_ATPROTO_LABEL_HIDE:
		return "!hide"
	case osprey.AtprotoLabel_ATPROTO_LABEL_NEEDS_REVIEW:
		return "needs-review"
	case osprey.AtprotoLabel_ATPROTO_LABEL_PORN:
		return "porn"
	case osprey.AtprotoLabel_ATPROTO_LABEL_RUDE:
		return "rude"
	case osprey.AtprotoLabel_ATPROTO_LABEL_SEXUAL:
		return "sexual"
	case osprey.AtprotoLabel_ATPROTO_LABEL_SPAM:
		return "spam"
	case osprey.AtprotoLabel_ATPROTO_LABEL_WARN:
		return "!warn"
	case osprey.AtprotoLabel_ATPROTO_LABEL_MISLEADING:
		return "misleading"
	default:
		panic(fmt.Sprintf("unexpected osprey.AtprotoLabel: %#v", label))
	}
}

type ListTemplatesResponse struct {
	CommunicationTemplates []CommunicationTemplate `json:"communicationTemplates"`
}

type CommunicationTemplate struct {
	Id              string `json:"id"`
	Name            string `json:"name"`
	ContentMarkdown string `json:"contentMarkdown"`
	Disabled        bool   `json:"disabled"`
	Lang            string `json:"lang"`
	CreatedAt       string `json:"createdAt"`
	UpdatedAt       string `json:"updatedAt"`
	LastUpdatedBy   string `json:"lastUpdatedBy"`
	Subject         string `json:"subject"`
}

func metaToInterface(meta ModToolMeta) *interface{} {
	var metaInterface interface{} = meta
	return &metaInterface
}
