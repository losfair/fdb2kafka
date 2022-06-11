package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	mathRand "math/rand"
	"os"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/losfair/fdb2kafka/util"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

type Payload struct {
	Key   string  `json:"key"`
	Value float64 `json:"value"`
}

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
				Required: false,
				Value:    "/etc/foundationdb/fdb.cluster",
				Usage:    "fdb cluster file path",
				EnvVars:  []string{"SERVICE_CLUSTER"},
			},
			&cli.StringFlag{
				Name:     "log-path",
				Required: true,
				Usage:    "json-encoded fdb string tuple path to the log",
				EnvVars:  []string{"SERVICE_LOG_PATH"},
			},
		},
		Commands: []*cli.Command{
			{
				Name: "run",
				Action: func(c *cli.Context) error {
					db, err := fdb.OpenDatabase(c.String("cluster"))
					if err != nil {
						return err
					}

					logPrefix_, err := util.ParseTuple(c.String("log-path"))
					if err != nil {
						return err
					}
					logPrefix := append([]byte(logPrefix_.Pack()), 0x32)

					keyTemplate := make([]byte, len(logPrefix)+10+4)
					copy(keyTemplate, logPrefix)
					binary.LittleEndian.PutUint32(keyTemplate[len(logPrefix)+10:], uint32(len(logPrefix)))

					for {
						sleepDur := int(mathRand.Float64() * 1000)
						time.Sleep(time.Duration(sleepDur) * time.Millisecond)

						msgCount := mathRand.Intn(100) + 1
						for i := 0; i < msgCount; i++ {
							payload := &Payload{
								Key:   fmt.Sprintf("%d", mathRand.Intn(1000)),
								Value: mathRand.Float64(),
							}
							payloadBytes, err := json.Marshal(payload)
							if err != nil {
								return err
							}
							_, err = db.Transact(func(t fdb.Transaction) (interface{}, error) {
								t.SetVersionstampedKey(fdb.Key(keyTemplate), payloadBytes)
								return nil, nil
							})
							if err != nil {
								return err
							}
						}
						logger.Info("written logs", zap.Int("count", msgCount))
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
