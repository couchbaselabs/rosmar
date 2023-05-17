package rosmar

import (
	"database/sql"
	"encoding/json"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
)

type ViewName struct {
	designDoc string
	name      string
}

func (vn *ViewName) String() string { return vn.designDoc + "/" + vn.name }

// A single view stored in a Bucket.
type rosmarView struct {
	id             int64                   // Database primary key (views.id)
	mapFnSource    string                  // Map function source code
	reduceFnSource string                  // Reduce function (if any)
	lastCas        uint64                  // Collection's lastCas when indexed
	mapFunction    *sgbucket.JSMapFunction // The compiled map function
}

// Returns a rosmarView for a given view name.
func (c *Collection) findView(name ViewName) (view *rosmarView, upToDate bool, err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	view, found := c.views[name]
	if !found {
		view = &rosmarView{}
		row := c.db.QueryRow(`SELECT id, mapFn, reduceFn, lastCas FROM views
		 						WHERE collection=$1 AND designDoc=$2 AND name=$3`,
			c.id, name.designDoc, name.name)
		err = row.Scan(&view.id, &view.mapFnSource, &view.reduceFnSource, &view.lastCas)
		if err != nil {
			if err == sql.ErrNoRows {
				err = sgbucket.MissingError{Key: name.String()}
			}
			return
		}
		if c.views == nil {
			c.views = map[ViewName]*rosmarView{}
		}
		c.views[name] = view
	}

	lastCas, err := c.getLastCas()
	if err == nil {
		upToDate = (view.lastCas == lastCas)
	}
	return
}

func (c *Collection) View(name ViewName, params map[string]interface{}) (result sgbucket.ViewResult, err error) {
	staleOK := true
	if params != nil {
		if staleParam, found := params["stale"].(bool); found {
			staleOK = staleParam
		}
	}

	// Look up the view and its index:
	view, upToDate, err := c.findView(name)
	if err != nil {
		return result, err
	}

	if !upToDate && !staleOK {
		if err = c.updateView(view, 0); err != nil {
			return
		}
	}

	if result, err = c.getViewRows(view, params); err != nil {
		return
	}

	err = result.Process(params, c, view.reduceFnSource)
	return
}

func (c *Collection) ViewQuery(name ViewName, params map[string]interface{}) (sgbucket.QueryResultIterator, error) {
	viewResult, err := c.View(name, params)
	return &viewResult, err
}

func (c *Collection) ViewCustom(name ViewName, params map[string]interface{}, vres interface{}) error {
	result, err := c.View(name, params)
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

// Updates the view index if necessary.
func (c *Collection) updateView(view *rosmarView, toSequence uint64) error {
	if view.mapFunction == nil {
		view.mapFunction = sgbucket.NewJSMapFunction(view.mapFnSource, 10*time.Second)
	}

	lastCas, err := c.getLastCas()
	if err != nil {
		return err
	}
	if toSequence == 0 {
		toSequence = lastCas
	}
	if view.lastCas >= toSequence {
		return nil
	}

	return c.inTransaction(func(txn *sql.Tx) error {
		info("\t... updating index to seq %d (from %d)", toSequence, view.lastCas)

		// First delete all obsolete rows from the index:
		_, err := txn.Exec(`DELETE FROM mapped WHERE view=$1 AND doc IN
							(SELECT id FROM documents WHERE collection=$2 AND cas > $3)`,
			view.id, c.id, view.lastCas)
		if err != nil {
			return err
		}

		rows, err := txn.Query(`SELECT id, key, value, cas, isJSON FROM documents
							   WHERE collection=$1 AND cas > $2 AND value NOT NULL`,
			c.id, view.lastCas)
		if err != nil {
			return err
		}

		for rows.Next() {
			var input sgbucket.JSMapFunctionInput
			var doc_id int
			var isJSON bool
			if err = rows.Scan(&doc_id, &input.DocID, &input.Doc, &input.VbSeq, &isJSON); err != nil {
				return err
			}
			if !isJSON {
				input.Doc = "{}" // ignore body of non-JSON docs
			}
			viewRows, err := view.mapFunction.CallFunction(&input)
			if err != nil {
				warn("Error running map function: %s", err)
				continue
			}
			for _, viewRow := range viewRows {
				var key, value []byte
				if key, err = json.Marshal(viewRow.Key); err != nil {
					return err
				} else if value, err = json.Marshal(viewRow.Value); err != nil {
					return err
				}
				debug("EMIT %s , %s  (doc=%s or %d; CAS %d)", key, value, input.DocID, doc_id, input.VbSeq)
				_, err = txn.Exec(`INSERT INTO mapped (view,doc,cas,key,value)
									VALUES ($1, $2, $3, $4, $5)`,
					view.id, doc_id, input.VbSeq, key, value)
				if err != nil {
					return err
				}
			}
		}
		if err = rows.Close(); err != nil {
			return err
		}
		view.lastCas = lastCas
		return nil
	})
}

// Returns all the view rows from the database. Sorted but not filtered or reduced.
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
		err = rows.Scan(&viewRow.ID, &jsonKey, &jsonValue)
		if err != nil {
			return
		} else if err = json.Unmarshal(jsonKey, &viewRow.Key); err != nil {
			return
		} else if err = json.Unmarshal(jsonValue, &viewRow.Value); err != nil {
			return
		}
		result.Rows = append(result.Rows, &viewRow)
		trace("QUERY found %+v", viewRow)
	}
	err = rows.Close()
	result.Sort()
	result.TotalRows = len(result.Rows)
	return
}
