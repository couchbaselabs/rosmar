package rosmar

var MaxDocSize int = 20 * 1024 * 1024

type document struct {
	key    string            // Doc ID
	value  []byte            // Raw data content, or nil if deleted
	isJSON bool              // Is the data a JSON document?
	xattrs map[string][]byte // Extended attributes
	cas    CAS               // Sequence in collection
	exp    uint32            // Expiration time
}
