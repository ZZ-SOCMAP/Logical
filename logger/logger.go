package logger

import (
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io"
	"logical/config"
	"os"
)

// initLogger 初始化日志配置
func initLogger(level string, writer io.Writer, encoder zapcore.Encoder) (err error) {
	var loggerLevel = new(zapcore.Level)
	err = loggerLevel.UnmarshalText([]byte(level))
	if err != nil {
		return
	}
	core := zapcore.NewCore(encoder, zapcore.AddSync(writer), loggerLevel)
	// 替换zap包中全局的logger实例，使用直接调用 zap.L()
	zap.ReplaceGlobals(zap.New(core, zap.AddCaller()))
	return err
}

// InitDebugLogger 日志设置为控制台标准输出
func InitDebugLogger(cfg *config.Logger) (err error) {
	return initLogger(
		cfg.Level,
		os.Stdout,
		zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
	)
}

// InitReleaseLogger 日志格式化为json并输出到日志
func InitReleaseLogger(cfg *config.Logger) (err error) {
	return initLogger(
		cfg.Level,
		newReleaseWriter(cfg.Savepath, cfg.Maxsize, cfg.MaxBackup, cfg.MaxAge),
		zapcore.NewJSONEncoder(newReleaseEncoderConfig()),
	)
}

// NewReleaseEncoderConfig 生产环境下的日志环境配置
func newReleaseEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
}

// NewReleaseWriter 生产环境下，将日志输出到文件(自动分块)
func newReleaseWriter(filename string, maxSize, maxBackup, maxAge int) io.Writer {
	return &lumberjack.Logger{
		Filename:   filename,
		MaxSize:    maxSize,
		MaxBackups: maxBackup,
		MaxAge:     maxAge,
	}
}
