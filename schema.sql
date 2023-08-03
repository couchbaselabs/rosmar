-- Copyright 2023-Present Couchbase, Inc.
--
-- Use of this software is governed by the Business Source License included
-- in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
-- in that file, in accordance with the Business Source License, use of this
-- software will be governed by the Apache License, Version 2.0, included in
-- the file licenses/APL2.txt.

CREATE TABLE bucket (
	name 		text not null,
	uuid 		text not null,
	lastCas 	integer not null );

CREATE TABLE collections (
	id 			integer primary key autoincrement,
	scope 		text not null,
	name 		text not null,
	lastCas 	integer default 0,
	UNIQUE (scope, name) );

CREATE TABLE documents (
	id 			integer primary key autoincrement,
	collection 	integer references collections(id) on delete cascade,
	key 		text not null,		/* i.e. documentID */
	cas 		integer not null,
	exp 		integer default 0,
	xattrs 		blob, 				/* xattrs in JSON, else null */
	isJSON 		integer default true,
	value 		blob, 				/* document body, usually JSON; null if deleted */
	UNIQUE (collection, key) );
CREATE INDEX docs_cas ON documents (collection, cas);
CREATE INDEX docs_exp ON documents (collection, exp) WHERE exp > 0;

CREATE TABLE designDocs (
	id 			integer primary key autoincrement,
	collection 	integer references collections(id) on delete cascade,
	name		text not null,
	UNIQUE(collection, name) );

CREATE TABLE views (
	id 			integer primary key autoincrement,
	designDoc 	integer references designDocs(id) on delete cascade,
	name 		text not null,      /* name of view */
	mapFn 		text not null,
	reduceFn 	text,
	lastCas 	integer default 0,  /* highest CAS value that's indexed */
	UNIQUE (designDoc, name) );

/* This table stores view indexes: key/value pairs emitted by map functions. */
CREATE TABLE mapped (
	view		integer references views(id) on delete cascade,
	doc			integer references documents(id) on delete cascade,
	key			text not null collate JSON,
	value		text not null );
CREATE INDEX mapped_doc ON mapped (view, doc);
CREATE INDEX mapped_key ON mapped (view, key);

/* Create the singleton `bucket` row */
INSERT INTO bucket (name, uuid, lastCas) VALUES ($NAME, $UUID, 0);

/* Create the default collection */
INSERT INTO COLLECTIONS (scope, name) VALUES ($SCOPE, $COLL);

/* Bump the user_version to indicate the schema is created */
PRAGMA user_version = 1;
