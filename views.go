package rosmar

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
)

// A single view stored in a Bucket.
type rosmarView struct {
	fullName       string                  // "collection/designdoc/name"
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

//////// API:

func (c *Collection) View(designDoc string, viewName string, params map[string]interface{}) (result sgbucket.ViewResult, err error) {
	debug("View(%q, %q, %+v)", designDoc, viewName, params)
	return c.view(designDoc, viewName, params)
}

func (c *Collection) ViewQuery(designDoc string, viewName string, params map[string]interface{}) (sgbucket.QueryResultIterator, error) {
	debug("ViewQuery(%q, %q, %+v)", designDoc, viewName, params)
	viewResult, err := c.view(designDoc, viewName, params)
	return &viewResult, err
}

func (c *Collection) ViewCustom(designDoc string, viewName string, params map[string]interface{}, vres interface{}) error {
	debug("ViewCustom(%q, %q, %+v)", designDoc, viewName, params)
	result, err := c.view(designDoc, viewName, params)
	if err != nil {
		return err
	}
	marshaled, _ := json.Marshal(result)
	return json.Unmarshal(marshaled, vres)
}

func (c *Collection) GetStatsVbSeqno(maxVbno uint16, useAbsHighSeqNo bool) (uuids map[uint16]uint64, highSeqnos map[uint16]uint64, err error) {
	err = &ErrUnimplemented{reason: "Rosmar does not implement GetStatsVbSeqno"}
	return
}

//////// IMPLEMENTATION:

func (c *Collection) view(
	designDoc string,
	viewName string,
	jsonParams map[string]interface{},
) (result sgbucket.ViewResult, err error) {
	params, err := sgbucket.ParseViewParams(jsonParams)
	if err != nil {
		return
	}
	// Look up the view and its index:
	view, upToDate, err := c.findView(designDoc, viewName)
	if err != nil {
		return result, err
	}
	// Update the view index if it's out of date:
	if !upToDate {
		var staleVal any
		if jsonParams != nil {
			staleVal = jsonParams["stale"]
		}
		if staleVal == "updateAfter" {
			go func() {
				debug("\t{updating view in background...}")
				c.updateView(view)
				debug("\t{...done updating view in background}")
			}()
		} else if staleVal != true && staleVal != "ok" {
			if err = c.updateView(view); err != nil {
				return
			}
		}
	}
	// Fetch the view index:
	if result, err = c.getViewRows(view, &params); err != nil {
		return
	}
	// Filter and reduce:
	err = result.ProcessParsed(params, c, view.reduceFnSource)
	debug("\tView --> %d rows", result.TotalRows)
	return
}

// Returns an up-to-date `rosmarView` for a given view name.
func (c *Collection) findView(designDoc string, viewName string) (view *rosmarView, upToDate bool, err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	key := viewKey{designDoc, viewName}
	row := c.db().QueryRow(`SELECT views.id, views.mapFn, views.reduceFn, views.lastCas
							FROM views JOIN designDocs ON views.designDoc=designDocs.id
		 					WHERE designDocs.collection=?1 AND designDocs.name=?2 AND views.name=?3`,
		c.id, designDoc, viewName)
	view = &rosmarView{
		fullName: fmt.Sprintf("%s/%s/%s", c, designDoc, viewName),
	}
	err = scan(row, &view.id, &view.mapFnSource, &view.reduceFnSource, &view.lastCas)
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

	return c.bucket.inTransaction(func(txn *sql.Tx) error {
		var latestCas CAS
		row := txn.QueryRow("SELECT lastCas FROM collections WHERE id=?1", c.id)
		err := scan(row, &latestCas)
		if err != nil {
			return err
		}
		info("\t... updating view %s index to seq %d (from %d)", view.fullName, latestCas, view.lastCas)

		// First delete all obsolete index rows, i.e. those whose source doc has been
		// updated since view.lastCas:
		_, err = txn.Exec(`DELETE FROM mapped WHERE view=?1 AND doc IN
						(SELECT id FROM documents WHERE collection=?2 AND cas > ?3)`,
			view.id, c.id, view.lastCas)
		if err != nil {
			return err
		}

		//TODO: Parallelize the below: SELECT -> mapFunction -> INSERT

		// Now iterate over all those updated docs:
		rows, err := txn.Query(`SELECT id, key, value, cas, isJSON, xattrs FROM documents
							    WHERE collection=?1 AND cas > ?2
									AND (value NOT NULL OR xattrs NOT NULL)`,
			c.id, view.lastCas)
		if err != nil {
			return err
		}
		for rows.Next() {
			// Read the document from the query row:
			var input sgbucket.JSMapFunctionInput
			var doc_id int
			var value []byte
			var isJSON bool
			var rawXattrs []byte
			if err = rows.Scan(&doc_id, &input.DocID, &value, &input.VbSeq, &isJSON, &rawXattrs); err != nil {
				return err
			}
			input.VbNo = sgbucket.VBHash(input.DocID, kNumVbuckets)

			if isJSON && value != nil {
				input.Doc = string(value)
			} else {
				input.Doc = "{}"
			}

			if len(rawXattrs) > 0 {
				var semiParsed semiParsedXattrs
				err = json.Unmarshal(rawXattrs, &semiParsed)
				if err != nil {
					logError("Error unmarshaling xattrs: %s", err)
				} else {
					input.Xattrs = make(map[string][]byte, len(semiParsed))
					for key, val := range semiParsed {
						input.Xattrs[key] = val
					}
				}
			}

			// Call the map function:
			trace("\tMAP %v", input)
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
				trace("\tEMIT %s , %s  (doc %d %q; CAS %d)", key, value, doc_id, input.DocID, input.VbSeq)
				_, err = txn.Exec(`INSERT INTO mapped (view,doc,key,value)
									VALUES (?1, ?2, ?3, ?4)`,
					view.id, doc_id, string(key), string(value))
				if err != nil {
					return err
				}
			}
		}
		if err = rows.Close(); err != nil {
			return err
		}

		_, err = txn.Exec(`UPDATE views SET lastCas=?1 WHERE id=?2`, latestCas, view.id)

		if err == nil {
			view.lastCas = latestCas
		}
		return err
	})
}

// Returns all the view rows from the database.
// Handles key ranges, descending order and limit (and clears the corresponding params)
// but does not reduce.
func (c *Collection) getViewRows(view *rosmarView, params *sgbucket.ViewParams) (result sgbucket.ViewResult, err error) {
	trace("querying view %s", view.fullName)

	args := []any{sql.Named(`VIEW`, view.id)}
	sel := `SELECT documents.key, mapped.key, mapped.value, `
	sel += ifelse(params.IncludeDocs, `documents.value `, `null `)
	sel += `FROM mapped INNER JOIN documents ON mapped.doc=documents.id WHERE mapped.view=$VIEW `

	setMinMax := func(minmax *any, inclusive bool, cmp string, arg string) error {
		if *minmax != nil {
			sel += `AND mapped.key ` + cmp
			if inclusive {
				sel += `=`
			}
			sel += ` $` + arg + ` `
			if jsonKey, jsonErr := json.Marshal(*minmax); jsonErr == nil {
				args = append(args, sql.Named(arg, string(jsonKey)))
			} else {
				return jsonErr
			}
			*minmax = nil
		}
		return nil
	}
	if err = setMinMax(&params.MinKey, params.IncludeMinKey, `>`, "MINKEY"); err != nil {
		return
	}
	if err = setMinMax(&params.MaxKey, params.IncludeMaxKey, `<`, "MAXKEY"); err != nil {
		return
	}

	sel += `ORDER BY mapped.key `
	if params.Descending {
		sel += `DESC `
		params.Descending = false
	}
	if params.Limit != nil {
		sel += fmt.Sprintf(`LIMIT %d `, *params.Limit)
		params.Limit = nil
	}
	trace("\t SQL = %s", sel)
	trace("\t args = %+v", args)

	rows, err := c.db().Query(sel, args...)
	if err != nil {
		return
	}
	for rows.Next() {
		var viewRow sgbucket.ViewRow
		var jsonKey, jsonValue, jsonDoc []byte
		if err = rows.Scan(&viewRow.ID, &jsonKey, &jsonValue, &jsonDoc); err != nil {
			return
		} else if err = json.Unmarshal(jsonKey, &viewRow.Key); err != nil {
			return
		} else if err = json.Unmarshal(jsonValue, &viewRow.Value); err != nil {
			return
		}
		if params.IncludeDocs {
			if err = json.Unmarshal(jsonDoc, &viewRow.Doc); err != nil {
				return
			}
		}
		result.Rows = append(result.Rows, &viewRow)
		trace("\tRow --> %s  =  %s  (doc %q)", jsonKey, jsonValue, viewRow.ID)
	}
	err = rows.Close()
	result.TotalRows = len(result.Rows)
	params.IncludeDocs = false // we already did it
	return
}
