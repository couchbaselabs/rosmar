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
	key 		text not null,
	cas 		integer not null,
	exp 		integer,
	xattrs 		blob, /*JSON*/
	isJSON 		integer default true,
	value 		blob,
	UNIQUE (collection, key) );
CREATE INDEX docs_cas ON documents (collection, cas);

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
	key			text not null,
	value		text not null );
CREATE INDEX mapped_doc ON mapped (view, doc);

/* Create the singleton `bucket` row */
INSERT INTO bucket (name, uuid, lastCas) VALUES ($1, $2, 0);

/* Create the default collection */
INSERT INTO COLLECTIONS (scope, name) VALUES ($3, $4);
