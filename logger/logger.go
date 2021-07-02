package logger

import (
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io"
	"logical/config"
	"os"
)

// initLogger init logger configuration
func initLogger(level string, writer io.Writer, encoder zapcore.Encoder) (err error) {
	var loggerLevel = new(zapcore.Level)
	if err = loggerLevel.UnmarshalText([]byte(level)); err != nil {
		return err
	}
	core := zapcore.NewCore(encoder, zapcore.AddSync(writer), loggerLevel)
	zap.ReplaceGlobals(zap.New(core, zap.AddCaller()))
	return nil
}

// InitDebugLogger log set to console standard output
func InitDebugLogger(cfg *config.LoggerConfig) (err error) {
	return initLogger(
		cfg.Level, os.Stdout,
		zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
	)
}

// InitReleaseLogger the log is formatted as json and output to the log
func InitReleaseLogger(cfg *config.LoggerConfig) (err error) {
	return initLogger(
		cfg.Level,
		&lumberjack.Logger{
			Filename:   cfg.Path,
			MaxSize:    cfg.MaxSize,
			MaxBackups: cfg.Backup,
			MaxAge:     cfg.MaxAge,
		},
		zapcore.NewJSONEncoder(zapcore.EncoderConfig{
			TimeKey:        "time",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			FunctionKey:    zapcore.OmitKey,
			MessageKey:     "message",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.CapitalLevelEncoder,
			EncodeTime:     zapcore.TimeEncoderOfLayout(config.TimeLayout),
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		}),
	)
}
