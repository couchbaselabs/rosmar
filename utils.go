package rosmar

import (
	"database/sql"
	"encoding/json"
	"fmt"

	sgbucket "github.com/couchbase/sg-bucket"
	sqlite3 "github.com/mattn/go-sqlite3"
)

type ErrUnimplemented struct{ reason string }

func (err *ErrUnimplemented) Error() string { return err.reason }

type DatabaseError struct {
	original error
}

func (err *DatabaseError) Error() string { return "Rosmar database error: " + err.original.Error() }
func (err *DatabaseError) Unwrap() error { return err.original }

func remapKeyError(err error, key string) error {
	if err == sql.ErrNoRows {
		return sgbucket.MissingError{Key: key}
	} else {
		return remapError(err)
	}
}

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

var (
	// Enforce interface conformance:
	_ error = &DatabaseError{}
)
