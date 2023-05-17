package rosmar

import (
	"database/sql"
	"reflect"

	sgbucket "github.com/couchbase/sg-bucket"
)

func (c *Collection) GetDDocs() (ddocs map[string]sgbucket.DesignDoc, err error) {
	var rows *sql.Rows
	rows, err = c.db.Query(`SELECT designDoc, name, mapFn, reduceFn FROM views
							 WHERE collection=$1`, c.id)
	if err != nil {
		return
	}
	ddocs = map[string]sgbucket.DesignDoc{}
	for rows.Next() {
		var ddocName, vName, mapFn, reduceFn string
		if err = rows.Scan(&ddocName, &vName, &mapFn, &reduceFn); err != nil {
			return
		}
		ddoc, found := ddocs[ddocName]
		if !found {
			ddoc.Language = "javascript"
			ddoc.Views = sgbucket.ViewMap{}
		}
		ddoc.Views[vName] = sgbucket.ViewDef{Map: mapFn, Reduce: reduceFn}
		ddocs[ddocName] = ddoc
	}
	return ddocs, err
}

func (c *Collection) GetDDoc(designDoc string) (ddoc sgbucket.DesignDoc, err error) {
	ddocs, err := c.GetDDocs()
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
	return c.inTransaction(func(txn *sql.Tx) error {
		if existing, err := c.GetDDoc(designDoc); err == nil {
			if reflect.DeepEqual(ddoc, &existing) {
				return nil // unchanged
			}
		}
		_, err := txn.Exec(`DELETE FROM views WHERE collection=$1 AND designDoc=$2`,
			c.id, designDoc)
		if err != nil {
			return err
		}
		for name, view := range ddoc.Views {
			_, err := txn.Exec(`INSERT INTO views (collection,designDoc,name,mapFn,reduceFn)
							VALUES($1, $2, $3, $4, $5)`,
				c.id, designDoc, name, view.Map, view.Reduce)
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
}

func (c *Collection) DeleteDDoc(designDoc string) error {
	return c.inTransaction(func(txn *sql.Tx) error {
		result, err := txn.Exec(`DELETE FROM views WHERE collection=$1 AND designDoc=$2`,
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
}

var (
	// Enforce interface conformance:
	_ sgbucket.ViewStore = &Collection{}
)
