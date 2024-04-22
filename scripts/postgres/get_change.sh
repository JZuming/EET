#!/bin/sh -v

if [ $# -ne 4 ]; then 
    echo "tested SQL file: "$1
    echo "targeted table: "$2
    echo "test database: "$3
    echo "backup file: "$4
    return
fi

/usr/local/pgsql/bin/psql -d postgres -c "drop database $3"
/usr/local/pgsql/bin/psql -d postgres -c "create database $3"
/usr/local/pgsql/bin/psql -d $3 -f $4
/usr/local/pgsql/bin/psql -d $3 -c "select * from $2" | sort > o_result
/usr/local/pgsql/bin/psql -d $3 -f $1
/usr/local/pgsql/bin/psql -d $3 -c "select * from $2" | sort > q_result
diff q_result o_result | grep ">"
