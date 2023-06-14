package rosmar

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"runtime"
	"sync"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	sqlite3 "github.com/mattn/go-sqlite3"
)

// Error returned from API calls on a closed Bucket.
var ErrBucketClosed = fmt.Errorf("this Rosmar bucket has been closed")

// An unimplemented feature
type ErrUnimplemented struct{ reason string }

func (err *ErrUnimplemented) Error() string { return err.reason }

// Indicates a SQL database error that wasn't mapped to a more specific error value.
// The underlying error is available via the Unwrap method.
type DatabaseError struct {
	original error
}

func (err *DatabaseError) Error() string { return "Rosmar database error: " + err.original.Error() }
func (err *DatabaseError) Unwrap() error { return err.original }

// Tries to convert a SQL[ite] error into a sgbucket one, or at least wrap it in a DatabaseError.
func remapError(err error) error {
	if sqliteErr, ok := err.(sqlite3.Error); ok {
		info("SQLite error: %v (code %d)", err, sqliteErr.Code)
		return &DatabaseError{original: err}
	} else if err == sql.ErrConnDone || err == sql.ErrNoRows || err == sql.ErrTxDone {
		info("SQL error: %T %v", err, err)
		return &DatabaseError{original: err}
	} else {
		return err
	}
}

// Like remapError but takes a docID/key; converts sql.ErrNoRows to sgbucket.MissingError.
func remapKeyError(err error, key string) error {
	if err == sql.ErrNoRows {
		return sgbucket.MissingError{Key: key}
	} else {
		return remapError(err)
	}
}

// If the error is a SQLite error, returns its error code (`sqlite3.SQLITE_*`); else 0.
func sqliteErrCode(err error) sqlite3.ErrNo {
	if sqliteErr, ok := err.(*sqlite3.Error); ok {
		return sqliteErr.Code
	} else {
		return 0
	}
}

// Something like C's `?:` operator
func ifelse[T any](cond bool, ifTrue T, ifFalse T) T {
	if cond {
		return ifTrue
	} else {
		return ifFalse
	}
}

// Quick and dirty heuristic to check whether a byte-string is a JSON object.
func looksLikeJSON(data []byte) bool {
	return len(data) >= 2 && data[0] == '{' && data[len(data)-1] == '}'
}

// Encodes an arbitrary value to raw bytes to be stored in a document.
// If `isJSON` is true, the value will be marshaled to JSON, or used as-is if it's a
// byte array or pointer to one. Otherwise it must be a byte array.
func encodeAsRaw(val interface{}, isJSON bool) (data []byte, err error) {
	if val != nil {
		if isJSON {
			// Check for already marshalled JSON
			switch typedVal := val.(type) {
			case []byte:
				data = typedVal
			case *[]byte:
				data = *typedVal
			default:
				data, err = json.Marshal(val)
			}
		} else {
			if typedVal, ok := val.([]byte); ok {
				data = typedVal
			} else {
				err = fmt.Errorf("raw value must be []byte")
			}
		}
	}
	return
}

// Unmarshals a document's raw value to a return value.
// If the return value is a pointer to []byte it will receive the raw value.
func decodeRaw(raw []byte, rv any) error {
	if raw == nil || rv == nil {
		return nil
	} else if bytesPtr, ok := rv.(*[]byte); ok {
		*bytesPtr = raw
		return nil
	} else {
		err := json.Unmarshal(raw, rv)
		if err != nil {
			logError("Error unmarshaling `%s` to %v : %s", raw, rv, err)
		}
		return err
	}
}

// Returns a CRC32c checksum formatted as a hex string.
func encodedCRC32c(data []byte) string {
	table := crc32.MakeTable(crc32.Castagnoli)
	checksum := crc32.Checksum(data, table)
	return fmt.Sprintf("0x%08x", checksum)
}

// Feeds the input channel through a number of copies of the function in parallel.
// This call is asynchronous. Output can be read from the returned channel.
func parallelize[IN any, OUT any](input <-chan IN, parallelism int, f func(input IN) OUT) <-chan OUT {
	if parallelism == 0 {
		parallelism = runtime.GOMAXPROCS(0)
	}
	output := make(chan OUT, len(input))
	var waiter sync.WaitGroup
	for j := 0; j < parallelism; j++ {
		waiter.Add(1)
		go func() {
			defer waiter.Done()
			for item := range input {
				output <- f(item)
			}
		}()
	}
	go func() {
		waiter.Wait()
		close(output)
	}()
	return output
}

const kMaxDeltaTtl = 60 * 60 * 24 * 30 // Constant used by CBS

// The current time, as an expiry value
func nowAsExpiry() Exp {
	return uint32(time.Now().Unix())
}

// Converts an input expiry value, which may be either an absolute Unix timestamp or a relative
// offset from the current time, into the absolute form.
func absoluteExpiry(exp Exp) Exp {
	if exp <= kMaxDeltaTtl && exp > 0 {
		exp += nowAsExpiry()
	}
	return exp
}

var (
	// Enforce interface conformance:
	_ error = &ErrUnimplemented{}
	_ error = &DatabaseError{}
)
