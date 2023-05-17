package rosmar

import (
	"database/sql"
	"encoding/json"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
)

// A single view stored in a Bucket.
type rosmarView struct {
	id             int64                   // Database primary key (views.id)
	mapFnSource    string                  // Map function source code
	reduceFnSource string                  // Reduce function (if any)
	lastCas        uint64                  // Collection's lastCas when indexed
	mapFunction    *sgbucket.JSMapFunction // The compiled map function
}

type viewKey struct {
	designDoc string
	name      string
}

func (vn *viewKey) String() string { return vn.designDoc + "/" + vn.name }

const kMapFnTimeout = 5 * time.Second

func (c *Collection) View(designDoc string, viewName string, params map[string]interface{}) (result sgbucket.ViewResult, err error) {
	staleOK := true
	if params != nil {
		if staleParam, found := params["stale"].(bool); found {
			staleOK = staleParam
		}
	}
	// Look up the view and its index:
	view, upToDate, err := c.findView(designDoc, viewName)
	if err != nil {
		return result, err
	}
	// Update the view index if it's out of date:
	if !upToDate && !staleOK {
		if err = c.updateView(view); err != nil {
			return
		}
	}
	// Fetch the view index:
	if result, err = c.getViewRows(view, params); err != nil {
		return
	}
	// Filter and reduce:
	err = result.Process(params, c, view.reduceFnSource)
	return
}

func (c *Collection) ViewQuery(designDoc string, viewName string, params map[string]interface{}) (sgbucket.QueryResultIterator, error) {
	viewResult, err := c.View(designDoc, viewName, params)
	return &viewResult, err
}

func (c *Collection) ViewCustom(designDoc string, viewName string, params map[string]interface{}, vres interface{}) error {
	result, err := c.View(designDoc, viewName, params)
	if err != nil {
		return err
	}
	marshaled, _ := json.Marshal(result)
	return json.Unmarshal(marshaled, vres)
}

func (c *Collection) GetStatsVbSeqno(maxVbno uint16, useAbsHighSeqNo bool) (uuids map[uint16]uint64, highSeqnos map[uint16]uint64, err error) {
	err = ErrUNIMPLEMENTED
	return
}

//////// IMPLEMENTATION:

// Returns an up-to-date `rosmarView` for a given view name.
func (c *Collection) findView(designDoc string, viewName string) (view *rosmarView, upToDate bool, err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	key := viewKey{designDoc, viewName}
	row := c.db.QueryRow(`SELECT id, mapFn, reduceFn, lastCas FROM views
		 					WHERE collection=$1 AND designDoc=$2 AND name=$3`,
		c.id, designDoc, viewName)
	view = &rosmarView{}
	err = row.Scan(&view.id, &view.mapFnSource, &view.reduceFnSource, &view.lastCas)
	if err != nil {
		if err == sql.ErrNoRows {
			err = sgbucket.MissingError{Key: key.String()}
			delete(c.viewCache, key) // Remove any cached copy
		}
		return
	}

	if cachedView, found := c.viewCache[key]; found {
		// Reuse cached compiled map function:
		if cachedView.mapFnSource == view.mapFnSource {
			view.mapFunction = cachedView.mapFunction
		}
	}

	// Cache it:
	if c.viewCache == nil {
		c.viewCache = map[viewKey]*rosmarView{}
	}
	c.viewCache[key] = view

	lastCas, err := c.getLastCas()
	upToDate = (err == nil && view.lastCas == lastCas)
	return
}

// Remove in-memory view objects for a design doc: [Collection must be locked]
func (c *Collection) forgetCachedViews(designDoc string) {
	for name := range c.viewCache {
		if name.designDoc == designDoc {
			delete(c.viewCache, name)
		}
	}
}

// Updates the view index if necessary.
func (c *Collection) updateView(view *rosmarView) error {
	if view.mapFunction == nil {
		view.mapFunction = sgbucket.NewJSMapFunction(view.mapFnSource, kMapFnTimeout)
	}

	return c.inTransaction(func(txn *sql.Tx) error {
		var latestCas CAS
		row := txn.QueryRow("SELECT lastCas FROM collections WHERE id=$1", c.id)
		err := row.Scan(&latestCas)
		if err != nil {
			return err
		}
		info("\t... updating index to seq %d (from %d)", latestCas, view.lastCas)

		// First delete all obsolete index rows, i.e. those whose source doc has been
		// updated since view.lastCas:
		_, err = txn.Exec(`DELETE FROM mapped WHERE view=$1 AND doc IN
						(SELECT id FROM documents WHERE collection=$2 AND cas > $3)`,
			view.id, c.id, view.lastCas)
		if err != nil {
			return err
		}

		//TODO: Parallelize the below: SELECT -> mapFunction -> INSERT

		// Now iterate over all those updated docs:
		rows, err := txn.Query(`SELECT id, key, value, cas, isJSON FROM documents
							    WHERE collection=$1 AND cas > $2 AND value NOT NULL`,
			c.id, view.lastCas)
		if err != nil {
			return err
		}
		for rows.Next() {
			// Read the document from the query row:
			var input sgbucket.JSMapFunctionInput
			var doc_id int
			var isJSON bool
			if err = rows.Scan(&doc_id, &input.DocID, &input.Doc, &input.VbSeq, &isJSON); err != nil {
				return err
			}
			input.VbNo = sgbucket.VBHash(input.DocID, kNumVbuckets)
			if !isJSON {
				input.Doc = "{}" // ignore body of non-JSON docs
			}

			// Call the map function:
			viewRows, err := view.mapFunction.CallFunction(&input)
			if err != nil {
				logError("Error running map function on doc %q: %s", input.DocID, err)
				continue
			}

			for _, viewRow := range viewRows {
				// Insert each emitted row into the `mapped` table:
				var key, value []byte
				if key, err = json.Marshal(viewRow.Key); err != nil {
					return err
				} else if value, err = json.Marshal(viewRow.Value); err != nil {
					return err
				}
				debug("EMIT %s , %s  (doc=%s or %d; CAS %d)", key, value, input.DocID, doc_id, input.VbSeq)
				_, err = txn.Exec(`INSERT INTO mapped (view,doc,key,value)
									VALUES ($1, $2, $3, $4)`,
					view.id, doc_id, key, value)
				if err != nil {
					return err
				}
			}
		}
		if err = rows.Close(); err != nil {
			return err
		}

		_, err = txn.Exec(`UPDATE views SET lastCas=$1 WHERE id=$2`, latestCas, view.id)

		if err == nil {
			view.lastCas = latestCas
		}
		return err
	})
}

// Returns all the view rows from the database.
// They are sorted, but not filtered or reduced.
// TODO: Do the sorting/filtering in the query. Will require SQLite being able to collate mapped.key (custom encoding or custom collator fn)
func (c *Collection) getViewRows(view *rosmarView, params map[string]interface{}) (result sgbucket.ViewResult, err error) {
	rows, err := c.db.Query(`SELECT documents.key, mapped.key, mapped.value
				FROM mapped INNER JOIN documents ON mapped.doc=documents.id
				WHERE mapped.view=$1`, view.id)
	if err != nil {
		return
	}
	for rows.Next() {
		var viewRow sgbucket.ViewRow
		var jsonKey, jsonValue []byte
		if err = rows.Scan(&viewRow.ID, &jsonKey, &jsonValue); err != nil {
			return
		} else if err = json.Unmarshal(jsonKey, &viewRow.Key); err != nil {
			return
		} else if err = json.Unmarshal(jsonValue, &viewRow.Value); err != nil {
			return
		}
		// TODO: Populate viewRow.Doc (when?)
		result.Rows = append(result.Rows, &viewRow)
		trace("QUERY found %+v", viewRow)
	}
	err = rows.Close()
	result.Sort()
	result.TotalRows = len(result.Rows)
	return
}
