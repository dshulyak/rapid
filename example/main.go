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

	_ "net/http/pprof"

	"github.com/dshulyak/rapid"
	"github.com/dshulyak/rapid/monitor/prober"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

var (
	config    = pflag.StringP("configuration", "c", "example/rapid.json", "JSON file with configuration.")
	ip        = pflag.StringP("ip", "i", "0.0.0.0", "")
	port      = pflag.Uint64P("port", "p", 4001, "")
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
		return context.Canceled
	})
	instance := rapid.New(logger, conf, fd)
	group.Go(func() error {
		return instance.Run(ctx)
	})
	group.Go(func() error {
		<-ctx.Done()
		return nil
	})

	configuration, update := instance.Configuration()
	for {
		select {
		case <-ctx.Done():
			if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
				must(err)
			}
			logger.Sugar().Info("rapid service stopped")
			return
		case <-update:
			configuration, update = instance.Configuration()
			logger.Sugar().With("configuration", configuration).Info("UPDATE")
		}
	}
}
