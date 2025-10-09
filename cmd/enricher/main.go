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
				Name:    "image-cdn-url",
				Usage:   "URL for the CDN",
				EnvVars: []string{"IMAGE_CDN_URL"},
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
				Name:    "retina-ocr-url",
				Usage:   "URL for the Retina OCR service including scheme",
				EnvVars: []string{"RETINA_OCR_URL"},
			},
			&cli.StringFlag{
				Name:    "retina-hash-url",
				Usage:   "URL for the Retina Hash service including scheme",
				EnvVars: []string{"RETINA_HASH_URL"},
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
			&cli.StringFlag{
				Name:    "milvus-host",
				Usage:   "Host for the Milvus vector database",
				EnvVars: []string{"MILVUS_HOST"},
			},
			&cli.StringFlag{
				Name:    "ncii-vector-collection",
				Usage:   "Milvus collection that Stop-NCII hashes are stored in",
				EnvVars: []string{"NCII_VECTOR_COLLECTION"},
			},
			&cli.Float64Flag{
				Name:    "ncii-min-distance",
				Usage:   "Minimum hamming distance for NCII hash matches",
				EnvVars: []string{"NCII_MIN_DISTANCE"},
			},
			&cli.StringFlag{
				Name:    "flagged-image-collection",
				Usage:   "Milvus collection that flagged image hashes are stored in",
				EnvVars: []string{"FLAGGED_IMAGE_COLLECTION"},
			},
			&cli.Float64Flag{
				Name:    "flagged-image-min-distance",
				Usage:   "Minimum hamming distance for flagged image matches",
				EnvVars: []string{"FLAGGED_IMAGE_MIN_DISTANCE"},
			},
		},
		Action: func(cmd *cli.Context) error {
			ctx := context.Background()

			logger := telemetry.StartLogger(cmd)
			telemetry.StartMetrics(cmd)

			args := enricher.Args{
				KafkaBootstrapServers:   cmd.StringSlice("kafka-bootstrap-servers"),
				SASLUsername:            cmd.String("sasl-username"),
				SASLPassword:            cmd.String("sasl-password"),
				InputTopic:              cmd.String("input-topic"),
				OutputTopic:             cmd.String("output-topic"),
				ImageCdnURL:             cmd.String("image-cdn-url"),
				AbyssURL:                cmd.String("abyss-url"),
				AbyssAdminPassword:      cmd.String("abyss-admin-password"),
				HiveAPIToken:            cmd.String("hive-api-token"),
				RetinaOcrURL:            cmd.String("retina-ocr-url"),
				RetinaHashURL:           cmd.String("retina-hash-url"),
				PrescreenHost:           cmd.String("prescreen-host"),
				OzoneHost:               cmd.String("ozone-host"),
				OzoneAdminToken:         cmd.String("ozone-admin-token"),
				AppviewHost:             cmd.String("appview-host"),
				AppviewRatelimitBypass:  cmd.String("appview-ratelimit-bypass"),
				PLCHost:                 cmd.String("plc-host"),
				MilvusHost:              cmd.String("milvus-host"),
				NciiCollection:          cmd.String("ncii-vector-collection"),
				NciiMinDistance:         cmd.Float64("ncii-min-distance"),
				FlaggedImageCollection:  cmd.String("flagged-image-collection"),
				FlaggedImageMinDistance: cmd.Float64("flagged-image-min-distance"),
				Logger:                  logger,
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
