package main

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/losfair/fdb2kafka/util"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

const VersionstampSize = 10

type Service struct {
	db        fdb.Database
	kwriter   *kafka.Writer
	config    ServiceConfig
	cursorKey []byte
	logPrefix []byte
	logger    *zap.Logger
	cursor    [VersionstampSize]byte
}

type ServiceConfig struct {
	Cluster     string
	CursorPath  string
	LogPath     string
	BatchSize   int
	KafkaServer string
	KafkaTopic  string
	KeyQuery    string
}

func NewService(logger *zap.Logger, config ServiceConfig) (*Service, error) {
	db, err := fdb.OpenDatabase(config.Cluster)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open fdb database")
	}

	kwriter := &kafka.Writer{
		Addr:                   kafka.TCP(config.KafkaServer),
		Topic:                  config.KafkaTopic,
		Balancer:               &kafka.CRC32Balancer{Consistent: true},
		AllowAutoTopicCreation: true,
		BatchTimeout:           10 * time.Millisecond,
	}

	cursorTuple, err := util.ParseTuple(config.CursorPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse cursor path")
	}

	logTuple, err := util.ParseTuple(config.LogPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse log path")
	}

	cursorKey := cursorTuple.Pack()
	logPrefix := append(logTuple.Pack(), 0x32)

	initialCursorHex_, err := db.ReadTransact(func(rt fdb.ReadTransaction) (interface{}, error) {
		return rt.Get(fdb.Key(cursorKey)).Get()
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to read initial cursor")
	}
	initialCursorHex := initialCursorHex_.([]byte)
	var cursor [VersionstampSize]byte

	if initialCursorHex != nil {
		if hex.DecodedLen(len(initialCursorHex)) != VersionstampSize {
			return nil, errors.New("invalid initial cursor length")
		}

		_, err = hex.Decode(cursor[:], initialCursorHex)
		if err != nil {
			return nil, errors.Wrap(err, "failed to decode initial cursor")
		}
	}

	logger.Info("initial cursor", zap.String("cursor", hex.EncodeToString(cursor[:])))

	return &Service{
		db:        db,
		kwriter:   kwriter,
		config:    config,
		cursorKey: cursorKey,
		logPrefix: logPrefix,
		logger:    logger,
		cursor:    cursor,
	}, nil
}

type LogEntry struct {
	Versionstamp [VersionstampSize]byte
	Data         []byte
}

func (e LogEntry) Key(logger *zap.Logger, query string) []byte {
	if query == "" {
		return nil
	}

	parsed, err := gabs.ParseJSON(e.Data)
	if err != nil {
		logger.Warn("failed to parse log entry as json", zap.String("data", string(e.Data)))
		return nil
	}
	value, ok := parsed.Path(query).Data().(string)
	if !ok {
		logger.Warn("the selected field is not a string", zap.String("data", string(e.Data)), zap.String("query", string(query)))
		return nil
	}

	return []byte(value)
}

func (s *Service) ShipOnce() error {
	r, err := fdb.PrefixRange(s.logPrefix)
	if err != nil {
		return err
	}
	r.Begin = fdb.Key(append(append(append([]byte{}, s.logPrefix...), s.cursor[:]...), 0x00))

	var log []LogEntry
	_, err = s.db.ReadTransact(func(rt fdb.ReadTransaction) (interface{}, error) {
		it := rt.GetRange(r, fdb.RangeOptions{
			Limit: s.config.BatchSize,
			Mode:  fdb.StreamingModeWantAll,
		}).Iterator()
		log = nil
		for it.Advance() {
			kv := it.MustGet()
			versionstamp := kv.Key[len(s.logPrefix):]
			if len(versionstamp) != VersionstampSize {
				s.logger.Warn("invalid versionstamp, skipping entry", zap.Binary("versionstamp", versionstamp))
			} else {
				log = append(log, LogEntry{
					Versionstamp: *(*[VersionstampSize]byte)(versionstamp),
					Data:         kv.Value,
				})
			}
		}
		return nil, nil
	})

	if err != nil {
		return errors.Wrap(err, "failed to read log")
	}

	if len(log) == 0 {
		time.Sleep(200 * time.Millisecond)
		return nil
	}

	localLogger := s.logger.With(
		zap.Int("count", len(log)),
		zap.String("start", hex.EncodeToString(log[0].Versionstamp[:])),
		zap.String("end", hex.EncodeToString(log[len(log)-1].Versionstamp[:])),
	)

	messages := make([]kafka.Message, len(log))

	for i, entry := range log {
		messages[i].Value = entry.Data
		messages[i].Key = entry.Key(localLogger, s.config.KeyQuery)
		messages[i].Headers = []kafka.Header{
			{
				Key:   "versionstamp",
				Value: []byte(hex.EncodeToString(entry.Versionstamp[:])),
			},
		}
	}

	for {
		err := s.kwriter.WriteMessages(context.Background(), messages...)
		if err != nil {
			localLogger.Warn("failed to write messages to kafka", zap.Error(err))
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}

	s.cursor = log[len(log)-1].Versionstamp
	_, err = s.db.Transact(func(t fdb.Transaction) (interface{}, error) {
		t.Set(fdb.Key(s.cursorKey), []byte(hex.EncodeToString(s.cursor[:])))
		return nil, nil
	})
	if err != nil {
		return errors.Wrap(err, "failed to commit cursor")
	}

	localLogger.Info("shipped logs")
	return nil
}
