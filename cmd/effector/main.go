package main

import (
	"context"
	"log"
	"os"

	"github.com/bluesky-social/go-util/pkg/telemetry"
	"github.com/bluesky-social/osprey-atproto/effector"
	_ "github.com/joho/godotenv/autoload"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name: "osprey-effector",
		Flags: []cli.Flag{
			telemetry.CLIFlagDebug,
			telemetry.CLIFlagMetricsListenAddress,
			&cli.StringSliceFlag{
				Name:     "bootstrap-servers",
				Required: true,
				EnvVars:  []string{"KAFKA_BOOTSTRAP_SERVERS"},
			},
			&cli.StringFlag{
				Name:     "input-topic",
				Required: true,
				EnvVars:  []string{"INPUT_KAFKA_TOPIC"},
			},
			&cli.StringFlag{
				Name:    "consumer-group",
				EnvVars: []string{"KAFKA_CONSUMER_GROUP"},
				Value:   "osprey-effector-consumers",
			},
			&cli.StringFlag{
				Name:     "ozone-proxy-did",
				Required: true,
				EnvVars:  []string{"OSPREY_OZONE_PROXY_DID"},
			},
			&cli.StringFlag{
				Name:     "ozone-pds-host",
				Required: true,
				EnvVars:  []string{"OSPREY_OZONE_PDS_HOST"},
			},
			&cli.StringFlag{
				Name:     "ozone-identifier",
				Required: true,
				EnvVars:  []string{"OSPREY_OZONE_IDENTIFIER"},
			},
			&cli.StringFlag{
				Name:     "ozone-password",
				Required: true,
				EnvVars:  []string{"OSPREY_OZONE_PASSWORD"},
			},
			&cli.StringFlag{
				Name:    "bigquery-credentials-json",
				EnvVars: []string{"OSPREY_BIGQUERY_CREDENTIALS_JSON"},
			},
			&cli.StringFlag{
				Name:    "bigquery-project-id",
				EnvVars: []string{"OSPREY_BIGQUERY_PROJECT_ID"},
			},
			&cli.StringFlag{
				Name:    "bigquery-dataset-id",
				EnvVars: []string{"OSPREY_BIGQUERY_DATASET_ID"},
			},
			&cli.StringFlag{
				Name:    "environment",
				Usage:   "Values other than `production` do not take actions in Ozone.",
				EnvVars: []string{"OSPREY_ENVIRONMENT"},
				Value:   "staging",
			},
			&cli.StringFlag{
				Name:    "slack-webhook-url",
				EnvVars: []string{"OSPREY_SLACK_WEBHOOK_URL"},
			},
			&cli.StringSliceFlag{
				Name:     "memcached-servers",
				EnvVars:  []string{"OSPREY_MEMCACHED_SERVERS"},
				Required: true,
			},
		},
		Action: func(cmd *cli.Context) error {
			ctx := context.Background()

			logger := telemetry.StartLogger(cmd)
			telemetry.StartMetrics(cmd)

			effector, err := effector.New(&effector.Args{
				BootstrapServers:        cmd.StringSlice("bootstrap-servers"),
				InputTopic:              cmd.String("input-topic"),
				ConsumerGroup:           cmd.String("consumer-group"),
				BigQueryCredentialsJson: []byte(cmd.String("bigquery-credentials-json")),
				BigQueryProjectID:       cmd.String("bigquery-project-id"),
				BigQueryDatasetID:       cmd.String("bigquery-dataset-id"),
				OzonePdsHost:            cmd.String("ozone-pds-host"),
				OzoneIdentifier:         cmd.String("ozone-identifier"),
				OzonePassword:           cmd.String("ozone-password"),
				OzoneProxyDid:           cmd.String("ozone-proxy-did"),
				IsProduction:            cmd.String("environment") == "production",
				SlackWebhookURL:         cmd.String("slack-webhook-url"),
				MemcacheServers:         cmd.StringSlice("memcached-servers"),
				Logger:                  logger,
			})
			if err != nil {
				return err
			}

			if err := effector.Run(ctx); err != nil {
				return err
			}

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
