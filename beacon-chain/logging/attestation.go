package logging

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/OffchainLabs/prysm/v6/io/file"
	"github.com/sirupsen/logrus"
)

var AttestationLogger *logrus.Logger

func init() {
	logDir := "logs"
	if err := file.MkdirAll(logDir); err != nil {
		logrus.WithError(err).Fatal("Failed to create log directory")
	}

	AttestationLogger = logrus.New()

	timestamp := time.Now().Format("2006-01-02_15-04-05")
	logFile := filepath.Join(logDir, fmt.Sprintf("attestations_%s.log", timestamp))
	file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to open log file")
	}

	AttestationLogger.SetOutput(file)
	AttestationLogger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339,
	})
} 