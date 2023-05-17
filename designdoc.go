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

func (c *Collection) GetDDoc(docname string) (ddoc sgbucket.DesignDoc, err error) {
	ddocs, err := c.GetDDocs()
	if err != nil {
		return
	}
	ddoc, found := ddocs[docname]
	if !found {
		err = sgbucket.MissingError{Key: docname}
	}
	return
}

func (c *Collection) PutDDoc(docname string, ddoc *sgbucket.DesignDoc) error {
	return c.inTransaction(func(txn *sql.Tx) error {
		if existing, err := c.GetDDoc(docname); err == nil {
			if reflect.DeepEqual(ddoc, &existing) {
				return nil // unchanged
			}
		}
		_, err := txn.Exec(`DELETE FROM views WHERE collection=$1 AND designDoc=$2`,
			c.id, docname)
		if err != nil {
			return err
		}
		for name, view := range ddoc.Views {
			_, err := txn.Exec(`INSERT INTO views (collection,designDoc,name,mapFn,reduceFn)
							VALUES($1, $2, $3, $4, $5)`,
				c.id, docname, name, view.Map, view.Reduce)
			if err != nil {
				return err
			}
		}
		// Remove in-memory view objects for the affected views:
		for name, _ := range c.views {
			if name.designDoc == docname {
				delete(c.views, name)
			}
		}
		c.forgetCachedViews(docname)
		return nil
	})
}

func (c *Collection) DeleteDDoc(docname string) error {
	result, err := c.db.Exec(`DELETE FROM views WHERE collection=$1 AND designDoc=$2`,
		c.id, docname)
	if err == nil {
		if n, err2 := result.RowsAffected(); n == 0 && err2 == nil {
			err = sgbucket.MissingError{Key: docname}
		} else {
			c.forgetCachedViews(docname)
		}
	}
	return err
}

// Remove in-memory view objects for a design doc:
func (c *Collection) forgetCachedViews(docname string) {
	for name, _ := range c.views {
		if name.designDoc == docname {
			delete(c.views, name)
		}
	}
}
