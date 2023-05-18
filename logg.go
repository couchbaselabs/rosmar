package rosmar

import (
	"log"
	"os"
)

type LogLevel uint32

const (
	// LevelNone disables all logging
	LevelNone LogLevel = iota
	// LevelError enables only error logging.
	LevelError
	// LevelWarn enables warn and error logging.
	LevelWarn
	// LevelInfo enables info, warn, and error logging.
	LevelInfo
	// LevelDebug enables debug, info, warn, and error logging.
	LevelDebug
	// LevelTrace enables trace, debug, info, warn, and error logging.
	LevelTrace
)

var (
	logLevelNames      = []string{"none", "error", "warn", "info", "debug", "trace"}
	logLevelNamesPrint = []string{"Rosmar: [NON] ", "Rosmar: [ERR] ", "Rosmar: [WRN] ", "Rosmar: [INF] ", "Rosmar: [DBG] ", "Rosmar: [TRC] "}
)

// Set this to configure logging, or `setenv SG_ROSMAR_LOGGING=true`
var Logging LogLevel = LevelNone

// Set this callback to redirect logging elsewhere. Default value writes to Go `log.Printf`
var LoggingCallback = func(level LogLevel, fmt string, args ...any) {
	log.Printf(logLevelNamesPrint[level]+fmt, args...)
}

func init() {
	env := os.Getenv("SG_ROSMAR_LOGGING")
	if env != "" && env != "0" && env != "false" && env != "FALSE" {
		Logging = LevelInfo
		for i, name := range logLevelNames {
			if env == name {
				Logging = LogLevel(i)
				break
			}
		}
	}
}

func logError(fmt string, args ...any) {
	if Logging >= LevelError {
		LoggingCallback(LevelError, fmt, args...)
	}
}

func warn(fmt string, args ...any) {
	if Logging >= LevelWarn {
		LoggingCallback(LevelWarn, fmt, args...)
	}
}

func info(fmt string, args ...any) {
	if Logging >= LevelInfo {
		LoggingCallback(LevelInfo, fmt, args...)
	}
}

func debug(fmt string, args ...any) {
	if Logging >= LevelDebug {
		LoggingCallback(LevelDebug, fmt, args...)
	}
}

func trace(fmt string, args ...any) {
	if Logging >= LevelTrace {
		LoggingCallback(LevelTrace, fmt, args...)
	}
}
