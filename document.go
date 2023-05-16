package rosmar

import "fmt"

var MaxDocSize int = 20 * 1024 * 1024

type DocTooBigErr struct{}

func (err DocTooBigErr) Error() string {
	return "document value was too large!"
}

type CasMismatchErr struct {
	expected CAS
}

func (err CasMismatchErr) Error() string {
	return fmt.Sprintf("cas mismatch; expected %x", err.expected)
}

var ErrCasFailureShouldRetry = fmt.Errorf("cas failure; please retry")

type document struct {
	key    string            // Doc ID
	value  []byte            // Raw data content, or nil if deleted
	isJSON bool              // Is the data a JSON document?
	cas    CAS               // Sequence in collection
	xattrs map[string][]byte // Extended attributes
}
