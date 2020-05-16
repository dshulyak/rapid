package main

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dshulyak/rapid"
	"github.com/dshulyak/rapid/monitor/prober"
	"github.com/dshulyak/rapid/types"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

var (
	config    = pflag.StringP("configuration", "c", "example/rapid.json", "JSON file with configuration.")
	ip        = pflag.String("ip", "0.0.0.0", "")
	port      = pflag.Uint64("port", 4001, "")
	verbosity = pflag.Int8P("verbosity", "v", -1, "logger verbosity")
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	pflag.Parse()

	rand.Seed(time.Now().Unix())

	lconf := zap.Config{
		Level:            zap.NewAtomicLevelAt(zapcore.Level(*verbosity)),
		Development:      true,
		Encoding:         "console",
		EncoderConfig:    zap.NewDevelopmentEncoderConfig(),
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
	logger, err := lconf.Build()
	must(err)
	ctx, cancel := context.WithCancel(context.Background())

	conf := rapid.Config{}
	bytes, err := ioutil.ReadFile(*config)
	must(err)
	err = json.Unmarshal(bytes, &conf)
	must(err)

	conf.IP = *ip
	conf.Port = *port

	fd := prober.New(logger.Sugar(), 2*time.Second, 2*time.Second, 3)

	updates := make(chan *types.Configuration, 1)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT)

	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-signals:
		}
		cancel()
		return errors.New("interrupted")
	})
	group.Go(func() error {
		return rapid.New(logger, conf, fd).Run(ctx, updates)
	})
	group.Go(func() error {
		<-ctx.Done()
		close(updates)
		return nil
	})
	for update := range updates {
		logger.Sugar().With("configuration", update).Info("UPDATE")
	}
	must(group.Wait())
}
