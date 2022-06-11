package main

import (
	"os"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

func main() {
	zapConfig := zap.NewProductionConfig()
	if os.Getenv("SERVICE_DEBUG") == "1" {
		zapConfig.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	}

	logger, err := zapConfig.Build()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	fdb.MustAPIVersion(630)

	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "cluster",
				Required: true,
				Value:    "/etc/foundationdb/fdb.cluster",
				Usage:    "fdb cluster file path",
				EnvVars:  []string{"SERVICE_CLUSTER"},
			},
			&cli.StringFlag{
				Name:     "kafka-server",
				Required: true,
				Usage:    "kafka server address",
				EnvVars:  []string{"SERVICE_KAFKA_SERVER"},
			},
			&cli.StringFlag{
				Name:     "kafka-topic",
				Required: true,
				Usage:    "kafka topic",
				EnvVars:  []string{"SERVICE_KAFKA_TOPIC"},
			},
			&cli.StringFlag{
				Name:     "cursor-path",
				Required: true,
				Usage:    "json-encoded fdb string tuple path to the cursor",
				EnvVars:  []string{"SERVICE_CURSOR_PATH"},
			},
			&cli.StringFlag{
				Name:     "log-path",
				Required: true,
				Usage:    "json-encoded fdb string tuple path to the log",
				EnvVars:  []string{"SERVICE_LOG_PATH"},
			},
			&cli.IntFlag{
				Name:     "batch-size",
				Required: true,
				Value:    1000,
				Usage:    "max batch size",
				EnvVars:  []string{"SERVICE_BATCH_SIZE"},
			},
			&cli.IntFlag{
				Name:     "key-query",
				Required: false,
				Usage:    "gabs query to extract key from message",
				EnvVars:  []string{"SERVICE_KEY_QUERY"},
			},
		},
		Commands: []*cli.Command{
			{
				Name: "run",
				Action: func(c *cli.Context) error {
					svc, err := NewService(logger, ServiceConfig{
						Cluster:     c.String("cluster"),
						CursorPath:  c.String("cursor-path"),
						LogPath:     c.String("log-path"),
						BatchSize:   c.Int("batch-size"),
						KafkaServer: c.String("kafka-server"),
						KafkaTopic:  c.String("kafka-topic"),
						KeyQuery:    c.String("key-query"),
					})
					if err != nil {
						return err
					}
					logger.Info("initialized service")
					for {
						err := svc.ShipOnce()
						if err != nil {
							return err
						}
					}
				},
			},
		},
	}

	err = app.Run(os.Args)
	if err != nil {
		logger.Panic("failed to run app", zap.Error(err))
	}
}
