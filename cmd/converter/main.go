package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/bluesky-social/go-util/pkg/telemetry"
	"github.com/bluesky-social/osprey-atproto/converter"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:  "converter",
		Usage: "a smol guy for converting the atproto firehose to kafka",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "relay-host",
				Usage:   "full websockt url to the relay firehose endpoint",
				Value:   "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos",
				EnvVars: []string{"OSPREY_CONVERTER_RELAY_HOST"},
			},
			&cli.StringSliceFlag{
				Name:     "bootstrap-servers",
				Usage:    "kafka bootstrap servers",
				Required: true,
				EnvVars:  []string{"KAFKA_BOOTSTRAP_SERVERS"},
			},
			&cli.StringFlag{
				Name:    "output-topic",
				Usage:   "kafka topic to produce to",
				Value:   "firehose_events_staging",
				EnvVars: []string{"OSPREY_CONVERTER_OUTPUT_TOPIC"},
			},
			&cli.Int64Flag{
				Name:    "cursor-override",
				Usage:   "override the cursor to start from a specific sequence number",
				EnvVars: []string{"OSPREY_CONVERTER_CURSOR_OVERRIDE"},
				Value:   -1,
			},
		},
		Action: run,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func run(cmd *cli.Context) error {
	ctx := context.Background()

	logger := telemetry.StartLogger(cmd)
	telemetry.StartMetrics(cmd)

	cvtr, err := converter.NewKafkaConverter(&converter.Args{
		RelayHost:        cmd.String("relay-host"),
		BootstrapServers: cmd.StringSlice("bootstrap-servers"),
		OutputTopic:      cmd.String("output-topic"),
		CursorOverride:   cmd.Int64("cursor-override"),
		Logger:           logger,
	})
	if err != nil {
		return fmt.Errorf("failed to create new converter: %w", err)
	}

	if err := cvtr.Run(ctx); err != nil {
		return fmt.Errorf("error running converter: %w", err)
	}

	return nil
}
