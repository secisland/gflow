package util

import (
	rotatelogs "github.com/lestrrat/go-file-rotatelogs"
	"github.com/pkg/errors"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"log"
	"strings"
	"time"
)

func NewTaskLogger(fn string) *logrus.Logger{
	maxAgeHours := viper.GetDuration("log.maxAgeHours")
	rotationHours := viper.GetDuration("log.rotationHours")

	// 日志路径必须是绝对路径，不能使用相对路径
	Log := NewLogger(fn, maxAgeHours * time.Hour,rotationHours * time.Hour)
	switch strings.ToLower(viper.GetString("log.level")) {
	case "debug":
		Log.Level = logrus.DebugLevel
	case "info":
		Log.Level = logrus.InfoLevel
	case "warn":
		Log.Level = logrus.WarnLevel
	case "error":
		Log.Level = logrus.ErrorLevel
	}
	return Log
}

func NewLogger(logFile string, maxAge time.Duration, rotationTime time.Duration) *logrus.Logger {
	writer, err := rotatelogs.New(
		logFile+".%Y%m%d",
		rotatelogs.WithLinkName(logFile),      //生成软链，指向最新日志文件
		rotatelogs.WithMaxAge(maxAge),             //文件最大保存时间
		rotatelogs.WithRotationTime(rotationTime), //日志切割时间间隔
	)
	if err != nil {
		log.Println("Create log file error.%+v", errors.WithStack(err))
	}

	lfHook := lfshook.NewHook(lfshook.WriterMap{
		logrus.DebugLevel: writer, // 为不同级别设置不同的输出目的
		logrus.InfoLevel:  writer,
		logrus.WarnLevel:  writer,
		logrus.ErrorLevel: writer,
		logrus.FatalLevel: writer,
		logrus.PanicLevel: writer,
	},
		&logrus.TextFormatter{},
	)

	Log := logrus.New()
	Log.AddHook(lfHook)
	return Log
}
