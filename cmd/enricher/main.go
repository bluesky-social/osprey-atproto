package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/bluesky-social/go-util/pkg/telemetry"
	enricher "github.com/bluesky-social/osprey-atproto/enricher/server"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
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
		Action: func(cmd *cli.Context) error {
			ctx := context.Background()

			logger := telemetry.StartLogger(cmd)
			telemetry.StartMetrics(cmd)

			args := enricher.Args{
				KafkaBootstrapServers:  cmd.StringSlice("kafka-bootstrap-servers"),
				SASLUsername:           cmd.String("sasl-username"),
				SASLPassword:           cmd.String("sasl-password"),
				InputTopic:             cmd.String("input-topic"),
				OutputTopic:            cmd.String("output-topic"),
				SecureOutputTopic:      cmd.String("secure-output-topic"),
				AbyssURL:               cmd.String("abyss-url"),
				AbyssAdminPassword:     cmd.String("abyss-admin-password"),
				HiveAPIToken:           cmd.String("hive-api-token"),
				RetinaURL:              cmd.String("retina-url"),
				RetinaAPIKey:           cmd.String("retina-api-key"),
				PrescreenHost:          cmd.String("prescreen-host"),
				OzoneHost:              cmd.String("ozone-host"),
				OzoneAdminToken:        cmd.String("ozone-admin-token"),
				AppviewHost:            cmd.String("appview-host"),
				AppviewRatelimitBypass: cmd.String("appview-ratelimit-bypass"),
				PLCHost:                cmd.String("plc-host"),
				Logger:                 logger,
			}

			en, err := enricher.New(ctx, &args)
			if err != nil {
				return fmt.Errorf("Failed to create new enricher: %w", err)
			}

			if err := en.Run(ctx); err != nil {
				return fmt.Errorf("Failed to run enricher: %w", err)
			}

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
