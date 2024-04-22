#!/bin/sh

echo "process sql: "$1

sort db.sql > sort_db.sql

rm re.db
sqlite3 re.db < db.sql
sqlite3 re.db < $1

sqlite3 re.db ".dump" > changed.sql
sort changed.sql > sort_changed.sql

diff sort_db.sql sort_changed.sql