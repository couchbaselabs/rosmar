// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rosmar

import (
	"fmt"
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

func traceEnter(fnName string, format string, args ...any) {
	if Logging >= LevelTrace {
		format = fmt.Sprintf("rosmar.%s (%s)", fnName, format)
		LoggingCallback(LevelTrace, format, args...)
	}
}

func traceExit(fnName string, err error, fmt string, args ...any) {
	if Logging >= LevelTrace {
		if err == nil {
			LoggingCallback(LevelTrace, "\trosmar."+fnName+" --> "+fmt, args...)
		} else {
			// Log error returns at Error level, but only if overall logging is at Trace
			LoggingCallback(LevelError, "\trosmar.%s --> %T: %s", fnName, err, err)
		}
	}
}
