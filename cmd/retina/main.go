package main

import (
	"log"
	"os"

	"github.com/bluesky-social/osprey-atproto/retina"
	_ "github.com/joho/godotenv/autoload"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:  "retina",
		Usage: "an ocr service using tesseract",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "api-listen-addr",
				EnvVars: []string{"RETINA_API_LISTEN_ADDR"},
				Value:   ":8080",
			},
			&cli.StringFlag{
				Name:    "metrics-addr",
				EnvVars: []string{"RETINA_METRICS_ADDR"},
				Value:   ":8081",
			},
			&cli.BoolFlag{
				Name:    "debug",
				EnvVars: []string{"RETINA_DEBUG"},
				Value:   false,
			},
			&cli.Int64Flag{
				Name:    "max-concurrent-ocr-execs",
				EnvVars: []string{"RETINA_MAX_CONCURRENT_OCR_EXECS"},
				Value:   5,
			},
			&cli.StringFlag{
				Name:    "pdq-path",
				EnvVars: []string{"RETINA_PDQ_PATH"},
				Value:   "/usr/bin/pdq-photo-hasher",
			},
		},
		Action: func(cmd *cli.Context) error {
			r, err := retina.New(&retina.Args{
				APIListenAddr:         cmd.String("api-listen-addr"),
				MetricsAddr:           cmd.String("metrics-addr"),
				Debug:                 cmd.Bool("debug"),
				MaxConcurrentOCRExecs: cmd.Int64("max-concurrent-ocr-execs"),
				PdqPath:               cmd.String("pdq-path"),
			})
			if err != nil {
				return err
			}

			if err := r.Run(cmd.Context); err != nil {
				return err
			}

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
