package log

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
)

type Options struct {
	Context        context.Context
	Options        option.LogOptions
	Observable     bool
	DefaultWriter  io.Writer
	BaseTime       time.Time
	PlatformWriter PlatformWriter
}

func New(options Options) (Factory, error) {
	// Every factory is the switchable one. A core that started with logging on is not a
	// different kind of core from one that started with it off: OFF at runtime has to
	// reach it just as ON reaches a disabled start, or the app can only offer half the
	// switch and OFF is a threshold on a factory nobody stopped.
	wrapped := newDisabledFactory(options)
	if options.Options.Disabled {
		return wrapped, nil
	}
	active, err := newActiveFactory(options)
	if err != nil {
		return nil, err
	}
	wrapped.install(active)
	return wrapped, nil
}

func newActiveFactory(options Options) (Factory, error) {
	logOptions := options.Options

	var logWriter io.Writer
	var logFilePath string

	switch logOptions.Output {
	case "":
		logWriter = options.DefaultWriter
		if logWriter == nil {
			logWriter = os.Stderr
		}
	case "stderr":
		logWriter = os.Stderr
	case "stdout":
		logWriter = os.Stdout
	default:
		logWriter = io.Discard
		logFilePath = logOptions.Output
	}
	logFormatter := Formatter{
		BaseTime:         options.BaseTime,
		DisableColors:    logOptions.DisableColor || logFilePath != "",
		DisableTimestamp: !logOptions.Timestamp && logFilePath != "",
		FullTimestamp:    logOptions.Timestamp,
		TimestampFormat:  "-0700 2006-01-02 15:04:05",
	}
	factory := NewDefaultFactory(
		options.Context,
		logFormatter,
		logWriter,
		logFilePath,
		options.PlatformWriter,
		options.Observable,
	)
	if logOptions.Level != "" {
		logLevel, err := ParseLevel(logOptions.Level)
		if err != nil {
			return nil, E.Cause(err, "parse log level")
		}
		factory.SetLevel(logLevel)
	} else {
		factory.SetLevel(LevelTrace)
	}
	return factory, nil
}
