package util

import (
	"github.com/sirupsen/logrus"
)

// SetLogger set an instance of logrus
func SetLogger(ll, lf string) {
	// set format
	switch lf {
	case "json":
		logrus.SetFormatter(&logrus.JSONFormatter{
			FieldMap: logrus.FieldMap{
				logrus.FieldKeyLevel: "severity",
				logrus.FieldKeyMsg:   "message",
			},
		})
	default:
		logrus.SetFormatter(&logrus.TextFormatter{
			QuoteEmptyFields:       true,
			FullTimestamp:          true,
			ForceColors:            true,
			DisableLevelTruncation: true,
		})
	}

	logLevel, err := logrus.ParseLevel(ll)
	if err != nil {
		logrus.Errorf("log level is not ok, setting to info by default : %v", err.Error())
		logrus.SetLevel(logrus.InfoLevel)
	} else {
		logrus.SetLevel(logLevel)
	}
}
