// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rosmar

import (
	"encoding/json"
	"fmt"
)

// Payload is a struct that holds data in up to three forms:
// - a parsed JSON value (slice, map...),
// - marshaled JSON,
// - or raw non-JSON bytes.
// You can initialize it directly, setting one of its three properties.
//
// Note: It's OK to initialize `parsed` with a byte array of marshaled JSON;
//
//	it'll be moved to `marshaled` on the first API call.
type payload struct {
	raw       []byte // Raw, *non-JSON* data
	marshaled []byte // Marshaled JSON
	parsed    any    // A JSON-marshalable Go value (map, slice, string...)
}

// True if it contains JSON data.
func (p *payload) isJSON() bool {
	return p.parsed != nil || p.marshaled != nil
}

// True if it contains no data, only nil
func (p *payload) isNil() bool {
	return p.raw == nil && p.parsed == nil && p.marshaled == nil
}

// The data as a `[]byte`, whether or not that's JSON.
func (p *payload) asByteArray() (data []byte, err error) {
	if p.raw != nil {
		return p.raw, nil
	} else {
		return p.marshalJSON()
	}
}

// The payload as marshaled JSON.
func (p *payload) marshalJSON() (data []byte, err error) {
	p.normalize()
	if p.marshaled != nil {
		return p.marshaled, nil
	} else if p.parsed != nil {
		data, err = encodeAsRaw(p.parsed, true)
		if err == nil {
			p.marshaled = data
		}
		return data, err
	} else if p.raw != nil {
		return nil, fmt.Errorf("unexpected non-JSON data")
	} else {
		return nil, nil
	}
}

// The payload unmarshaled to a Go value, if possible.
func (p *payload) unmarshalJSON() (result any, err error) {
	p.normalize()
	if p.parsed != nil {
		return p.parsed, nil
	} else if p.marshaled != nil {
		err = decodeRaw(p.marshaled, &result)
		if err == nil {
			p.parsed = result
		}
		return
	} else if p.raw != nil {
		return nil, fmt.Errorf("unexpected non-JSON data")
	} else {
		return nil, fmt.Errorf("unexpected nil value")
	}
}

// Stores a parsed value in a payload. (Clears `marshaled` to invalidate it.)
func (p *payload) setParsed(parsed any) {
	p.parsed = parsed
	p.marshaled = nil
}

// Stores marshaled JSON in a payload. (Clears `parsed` to invalidate it.)
func (p *payload) setMarshaled(marshaled []byte) {
	p.marshaled = marshaled
	p.parsed = nil
}

func (p *payload) normalize() {
	// Subroutine: If the `parsed` field contains a byte array, move it to `marshaled`
	if p.parsed != nil {
		switch val := p.parsed.(type) {
		case []byte:
			p.marshaled = val
			p.parsed = nil
		case *[]byte:
			p.marshaled = *val
			p.parsed = nil
		}
	}
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
				return typedVal, nil
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
