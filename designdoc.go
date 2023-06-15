package rosmar

import (
	"database/sql"
	"reflect"

	sgbucket "github.com/couchbase/sg-bucket"
)

func (c *Collection) GetDDocs() (ddocs map[string]sgbucket.DesignDoc, err error) {
	traceEnter("GetDDocs", "")
	return c.getDDocs(c.db())
}

func (c *Collection) getDDocs(q queryable) (ddocs map[string]sgbucket.DesignDoc, err error) {
	var rows *sql.Rows
	rows, err = q.Query(`SELECT designDocs.name, views.name, views.mapFn, views.reduceFn
						 FROM views RIGHT JOIN designDocs ON views.designDoc=designDocs.id
						 WHERE designDocs.collection=?1`, c.id)
	if err != nil {
		return
	}
	ddocs = map[string]sgbucket.DesignDoc{}
	for rows.Next() {
		var ddocName string
		var vName, mapFn, reduceFn sql.NullString
		if err = rows.Scan(&ddocName, &vName, &mapFn, &reduceFn); err != nil {
			return
		}
		ddoc, found := ddocs[ddocName]
		if !found {
			ddoc.Language = "javascript"
			ddoc.Views = sgbucket.ViewMap{}
		}
		if vName.Valid {
			ddoc.Views[vName.String] = sgbucket.ViewDef{Map: mapFn.String, Reduce: reduceFn.String}
		}
		ddocs[ddocName] = ddoc
	}
	return ddocs, rows.Close()
}

func (c *Collection) GetDDoc(designDoc string) (ddoc sgbucket.DesignDoc, err error) {
	traceEnter("GetDDoc", "%q", designDoc)
	ddoc, err = c.getDDoc(c.db(), designDoc)
	traceExit("GetDDoc", err, "ok")
	return
}

func (c *Collection) getDDoc(q queryable, designDoc string) (ddoc sgbucket.DesignDoc, err error) {
	ddocs, err := c.getDDocs(q)
	if err != nil {
		return
	}
	ddoc, found := ddocs[designDoc]
	if !found {
		err = sgbucket.MissingError{Key: designDoc}
	}
	return
}

func (c *Collection) PutDDoc(designDoc string, ddoc *sgbucket.DesignDoc) error {
	traceEnter("PutDDoc", "%q, %d views", designDoc, len(ddoc.Views))
	err := c.bucket.inTransaction(func(txn *sql.Tx) error {
		if existing, err := c.getDDoc(txn, designDoc); err == nil {
			if reflect.DeepEqual(ddoc, &existing) {
				return nil // unchanged
			}
		}
		_, err := txn.Exec(`DELETE FROM designDocs WHERE collection=?1 AND name=?2`,
			c.id, designDoc)
		if err != nil {
			return err
		}
		result, err := txn.Exec(`INSERT INTO designDocs (collection,name) VALUES (?1,?2)`,
			c.id, designDoc)
		if err != nil {
			return err
		}
		ddocID, _ := result.LastInsertId()
		for name, view := range ddoc.Views {
			_, err := txn.Exec(`INSERT INTO views (designDoc,name,mapFn,reduceFn)
							VALUES(?1, ?2, ?3, ?4)`,
				ddocID, name, view.Map, view.Reduce)
			if err != nil {
				return err
			}
		}
		// Remove in-memory view objects for the affected views:
		for name := range c.viewCache {
			if name.designDoc == designDoc {
				delete(c.viewCache, name)
			}
		}
		c.forgetCachedViews(designDoc)
		return nil
	})
	traceExit("PutDDoc", err, "ok")
	return err
}

func (c *Collection) DeleteDDoc(designDoc string) error {
	traceEnter("DeleteDDoc", "%q", designDoc)
	err := c.bucket.inTransaction(func(txn *sql.Tx) error {
		result, err := txn.Exec(`DELETE FROM designDocs WHERE collection=?1 AND name=?2`,
			c.id, designDoc)
		if err == nil {
			if n, err2 := result.RowsAffected(); n == 0 && err2 == nil {
				err = sgbucket.MissingError{Key: designDoc}
			} else {
				c.forgetCachedViews(designDoc)
			}
		}
		return err
	})
	traceExit("DeleteDDoc", err, "ok")
	return err
}

var (
	// Enforce interface conformance:
	_ sgbucket.ViewStore = &Collection{}
)
