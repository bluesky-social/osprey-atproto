package effector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
)

type SlackLogger struct {
	webhookUrl string
}

type slackMessage struct {
	Text string `json:"text"`
}

func NewSlackLogger(webhookUrl string) *SlackLogger {
	return &SlackLogger{
		webhookUrl: webhookUrl,
	}
}

func (l *SlackLogger) Name() string {
	return "slack"
}

// No-op, we don't log events to Slack
func (l *SlackLogger) LogEvent(ctx context.Context, log *OspreyEventLog) error {
	return nil
}

func (l *SlackLogger) LogEffect(ctx context.Context, log *OspreyEffectLog) error {
	var bskyUrl string
	var ozoneUrl string

	if strings.HasPrefix(log.Subject, "did:") {
		did := log.Subject
		bskyUrl = fmt.Sprintf("https://bsky.app/profile/%s", did)
		ozoneUrl = fmt.Sprintf("https://admin.prod.bsky.dev/repositories/%s", did)
	} else {
		aturi, err := syntax.ParseATURI(log.Subject)
		if err != nil {
			return fmt.Errorf("failed to parse effect subject as aturi: %w", err)
		}
		did := aturi.Authority().String()
		collection := aturi.Collection().String()
		rkey := aturi.RecordKey().String()

		switch collection {
		case "app.bsky.feed.post":
			bskyUrl = fmt.Sprintf("https://bsky.app/profile/%s/post/%s", did, rkey)
		case "app.bsky.actor.profile":
			bskyUrl = fmt.Sprintf("https://bsky.app/profile/%s", did)
		case "app.bsky.graph.list":
			bskyUrl = fmt.Sprintf("https://bsky.app/profile/%s/list/%s", did, rkey)
		}
		ozoneUrl = fmt.Sprintf("https://admin.prod.bsky.dev/repositories/%s/%s/%s", did, collection, rkey)
	}

	msg := fmt.Sprintf(`
Action ID: %d
Action Name: %s
Rules: %s
Created At: %s
Subject: %s
Bsky URL: %s
Ozone URL: %s
Comment: %s`, log.ActionID, log.ActionName, log.Rules, log.CreatedAt.Format(time.RFC3339Nano), log.Subject, bskyUrl, ozoneUrl, log.Comment)

	if log.Label.Valid {
		msg += fmt.Sprintf("\nLabel: %s", log.Label.StringVal)
	}
	if log.Tag.Valid {
		msg += fmt.Sprintf("\nTag: %s", log.Tag.StringVal)
	}
	if log.Email.Valid {
		msg += fmt.Sprintf("\nEmail: %s", log.Label.StringVal)
	}

	return l.log(ctx, msg)
}

func (l *SlackLogger) log(ctx context.Context, msg string) error {
	// wrap in backticks so it looks nice
	msg = fmt.Sprintf("```\n%s\n```", msg)

	payload := slackMessage{Text: msg}

	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", l.webhookUrl, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	io.Copy(io.Discard, resp.Body)

	return nil
}
