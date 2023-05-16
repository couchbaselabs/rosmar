package rosmar

import (
	"errors"
	"regexp"

	sgbucket "github.com/couchbase/sg-bucket"
)

// Key for looking up collections. Implements sgbucket.DataStoreName.
type DataStoreName struct {
	scope, collection string
}

const (
	defaultCollectionName = "_default"
	defaultScopeName      = "_default"
	defaultCollectionID   = 0
)

var scopeCollectionNameRegexp = regexp.MustCompile("^[a-zA-Z0-9-][a-zA-Z0-9%_-]{0,250}$")

func (sc DataStoreName) ScopeName() string {
	return sc.scope
}

func (sc DataStoreName) CollectionName() string {
	return sc.collection
}

func (sc DataStoreName) String() string {
	return sc.scope + "." + sc.collection
}

func (sc DataStoreName) isDefault() bool {
	return sc.scope == defaultScopeName && sc.collection == defaultCollectionName
}

// newValidScopeAndCollection validates the names and creates new scope and collection pair
func newValidScopeAndCollection(scope, collection string) (id DataStoreName, err error) {
	if !isValidDataStoreName(scope, collection) {
		return id, errors.New("invalid scope/collection name - only supports [A-Za-z0-9%-_]")
	}
	return DataStoreName{scope, collection}, nil
}

func isValidDataStoreName(scope, collection string) bool {
	if scope != defaultScopeName {
		return scopeCollectionNameRegexp.MatchString(scope) && scopeCollectionNameRegexp.MatchString(collection)
	}

	if collection != defaultCollectionName {
		return scopeCollectionNameRegexp.MatchString(collection)
	}

	return true
}

var (
	// Enforce interface conformance:
	_ sgbucket.DataStoreName = &DataStoreName{"a", "b"}
)
