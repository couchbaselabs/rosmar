// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rosmar

import (
	"context"
	"reflect"
	"testing"
)

func TestHierarchicalXattrs(t *testing.T) {
	bucket, err := OpenBucket(InMemoryURL, "test_xattrs", CreateOrOpen)
	if err != nil {
		t.Fatalf("Failed to open bucket: %v", err)
	}
	defer bucket.Close(context.Background())

	ds := bucket.DefaultDataStore()
	if ds == nil {
		t.Fatalf("Failed to get default datastore")
	}

	collection := ds.(*Collection)
	ctx := context.Background()

	key := "doc1"
	body := []byte(`{"hello": "world"}`)
	_, err = collection.WriteCas(key, 0, 0, body, 0)
	if err != nil {
		t.Fatalf("Failed to write document: %v", err)
	}

	var cas uint64

	// 1. Set hierarchical Xattr using SetXattrs
	t.Run("SetXattrs", func(t *testing.T) {
		xattrs := map[string][]byte{
			"_sync.compact.id1": []byte(`"val1"`),
			"_sync.compact.id2": []byte(`"val2"`),
		}
		casOut, err := collection.SetXattrs(ctx, key, xattrs)
		if err != nil {
			t.Fatalf("Failed to set xattrs: %v", err)
		}
		cas = casOut

		// Verify retrieval of exact path
		retrieved, _, err := collection.GetXattrs(ctx, key, []string{"_sync.compact.id1"})
		if err != nil {
			t.Fatalf("Failed to get xattr: %v", err)
		}
		if string(retrieved["_sync.compact.id1"]) != `"val1"` {
			t.Errorf("Expected '\"val1\"', got %s", string(retrieved["_sync.compact.id1"]))
		}

		// Verify retrieval of top level object
		retrievedFull, casOut, err := collection.GetXattrs(ctx, key, []string{"_sync"})
		if err != nil {
			t.Fatalf("Failed to get full xattr: %v", err)
		}
		cas = casOut
		expectedFull := `{"compact":{"id1":"val1","id2":"val2"}}`
		if string(retrievedFull["_sync"]) != expectedFull {
			t.Errorf("Expected %s, got %s", expectedFull, string(retrievedFull["_sync"]))
		}
	})

	// 2. Update hierarchical Xattr using UpdateXattrs
	t.Run("UpdateXattrs", func(t *testing.T) {
		xattrs := map[string][]byte{
			"_sync.compact.id1": []byte(`"val1-updated"`),
		}
		casOut, err := collection.UpdateXattrs(ctx, key, 0, cas, xattrs, nil)
		if err != nil {
			t.Fatalf("Failed to update xattrs: %v", err)
		}
		cas = casOut

		retrieved, _, err := collection.GetXattrs(ctx, key, []string{"_sync.compact.id1"})
		if err != nil {
			t.Fatalf("Failed to get xattr: %v", err)
		}
		if string(retrieved["_sync.compact.id1"]) != `"val1-updated"` {
			t.Errorf("Expected '\"val1-updated\"', got %s", string(retrieved["_sync.compact.id1"]))
		}
	})

	// 3. Retrieve body and hierarchical Xattr using GetWithXattrs
	t.Run("GetWithXattrs", func(t *testing.T) {
		val, xattrs, casOut, err := collection.GetWithXattrs(ctx, key, []string{"_sync.compact.id1"})
		if err != nil {
			t.Fatalf("Failed to get with xattrs: %v", err)
		}
		cas = casOut
		if string(val) != string(body) {
			t.Errorf("Expected body %s, got %s", string(body), string(val))
		}
		if string(xattrs["_sync.compact.id1"]) != `"val1-updated"` {
			t.Errorf("Expected xattr '\"val1-updated\"', got %s", string(xattrs["_sync.compact.id1"]))
		}
	})

	// 4. Update body and Xattrs using WriteWithXattrs
	t.Run("WriteWithXattrs", func(t *testing.T) {
		newBody := []byte(`{"hello": "updated"}`)
		xattrs := map[string][]byte{
			"_sync.compact.id3": []byte(`"val3"`),
		}
		toDelete := []string{"_sync.compact.id1"}
		casOut, err := collection.WriteWithXattrs(ctx, key, 0, cas, newBody, xattrs, toDelete, nil)
		if err != nil {
			t.Fatalf("Failed to write with xattrs: %v", err)
		}
		cas = casOut

		// Verify body and xattrs
		val, retrievedXattrs, casOut, err := collection.GetWithXattrs(ctx, key, []string{"_sync.compact.id3", "_sync.compact.id1"})
		if err != nil {
			t.Fatalf("Failed to get after write: %v", err)
		}
		cas = casOut
		if string(val) != string(newBody) {
			t.Errorf("Expected body %s, got %s", string(newBody), string(val))
		}
		if string(retrievedXattrs["_sync.compact.id3"]) != `"val3"` {
			t.Errorf("Expected xattr '\"val3\"', got %s", string(retrievedXattrs["_sync.compact.id3"]))
		}
		if _, ok := retrievedXattrs["_sync.compact.id1"]; ok {
			t.Errorf("Expected xattr id1 to be deleted, but found %s", string(retrievedXattrs["_sync.compact.id1"]))
		}
	})

	// 5. Remove hierarchical Xattr using RemoveXattrs
	t.Run("RemoveXattrs", func(t *testing.T) {
		err = collection.RemoveXattrs(ctx, key, []string{"_sync.compact.id3"}, cas)
		if err != nil {
			t.Fatalf("Failed to remove xattr: %v", err)
		}

		retrievedFull, casOut, err := collection.GetXattrs(ctx, key, []string{"_sync"})
		if err != nil {
			t.Fatalf("Failed to get full xattr: %v", err)
		}
		cas = casOut
		expectedFull := `{"compact":{"id2":"val2"}}`
		if string(retrievedFull["_sync"]) != expectedFull {
			t.Errorf("Expected %s, got %s", expectedFull, string(retrievedFull["_sync"]))
		}
	})

	// 6. Delete subdoc path using DeleteSubDocPaths
	t.Run("DeleteSubDocPaths", func(t *testing.T) {
		err = collection.DeleteSubDocPaths(ctx, key, "_sync.compact.id2")
		if err != nil {
			t.Fatalf("Failed to delete subdoc path: %v", err)
		}

		retrievedFull, casOut, err := collection.GetXattrs(ctx, key, []string{"_sync"})
		if err != nil {
			t.Fatalf("Failed to get full xattr: %v", err)
		}
		cas = casOut
		expectedFull := `{"compact":{}}`
		if string(retrievedFull["_sync"]) != expectedFull {
			t.Errorf("Expected %s, got %s", expectedFull, string(retrievedFull["_sync"]))
		}
	})

	// 7. Delete body and Xattrs using DeleteWithXattrs
	t.Run("DeleteWithXattrs", func(t *testing.T) {
		err = collection.DeleteWithXattrs(ctx, key, []string{"_sync"})
		if err != nil {
			t.Fatalf("Failed to delete with xattrs: %v", err)
		}

		// Doc should be tombstoned
		exists, err := collection.Exists(key)
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if exists {
			t.Errorf("Expected document to be tombstoned")
		}

		// Verify Xattrs are also removed
		_, retrievedXattrs, _, err := collection.GetWithXattrs(ctx, key, []string{"_sync"})
		if err == nil {
			t.Errorf("Expected error getting deleted xattr from tombstone, got %v", retrievedXattrs)
		}
	})
}

func TestSetXattrValue(t *testing.T) {
	tests := []struct {
		name     string
		source   map[string]any
		path     []string
		value    any
		expected map[string]any
		wantErr  bool
	}{
		{
			name:   "Set nested value",
			source: map[string]any{},
			path:   []string{"a", "b", "c"},
			value:  "test",
			expected: map[string]any{
				"a": map[string]any{
					"b": map[string]any{
						"c": "test",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "Update existing nested value",
			source: map[string]any{
				"a": map[string]any{
					"b": map[string]any{
						"c": "old",
					},
				},
			},
			path:  []string{"a", "b", "c"},
			value: "new",
			expected: map[string]any{
				"a": map[string]any{
					"b": map[string]any{
						"c": "new",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "Delete nested value",
			source: map[string]any{
				"a": map[string]any{
					"b": map[string]any{
						"c": "test",
						"d": "keep",
					},
				},
			},
			path:  []string{"a", "b", "c"},
			value: nil,
			expected: map[string]any{
				"a": map[string]any{
					"b": map[string]any{
						"d": "keep",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "Path mismatch",
			source: map[string]any{
				"a": "string_not_map",
			},
			path:     []string{"a", "b"},
			value:    "test",
			expected: map[string]any{"a": "string_not_map"}, // Unchanged
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := setXattrValue(tt.source, tt.path, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("setXattrValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(tt.source, tt.expected) {
				t.Errorf("setXattrValue() got = %v, want %v", tt.source, tt.expected)
			}
		})
	}
}
