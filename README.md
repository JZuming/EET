# 🌟EET🌟

## Description

EET is an automatic testing tool for detecting crashes and logic bugs in database systems. It integrates a general and highly-effective test oracle, _**E**quivalent **E**xpression **T**ransformation_, which can operate on arbitrary SQL queries to validate their execution. EET has successfully found many long-latent crashes and logic bugs, which cause database system to produce incorrect results for given SQL queries. EET was implemented on the top of [SQLsmith](https://github.com/anse1/sqlsmith).

The paper based on this tool has been (conditionally) accepted by [OSDI 2024](https://www.usenix.org/conference/osdi24).

## Supported Database Systems and Found Bugs
✅ MySQL (16 bugs listed in [here](./docs/bugs/mysql_bugs.md))

✅ PostgreSQL (9 bugs listed in [here](./docs/bugs/postgres_bugs.md))

✅ SQLite (10 bugs listed in [here](./docs/bugs/sqlite_bugs.md))

✅ ClickHouse (21 bugs listed in [here](./docs/bugs/clickhouse_bugs.md))

✅ TiDB (10 bugs listed in [here](./docs/bugs/tidb_bugs.md))

## Evaluation or Quick Start

We provide scripts to quickly set up the necessary environments in docker and test specific database systems using EET. We recommend you to follow the instructions in the scripts to evaluate EET or familiarize yourself with EET. For artifact evaluation, we recommend to test the specified versions of database systems, where the bugs found by EET had not been fixed yet.

### Artifact Evaluation

For artifact evaluation, we provide scripts to automatically test specified versions (the version evaluated in the paper) of the supported database systems. You can use these scripts to fairly evaluate the effectiveness of EET on finding database bugs. We expect that EET can quickly find bugs in each database systems within one hours.

- [Test MySQL at version 8.0.34](./docs/test/mysql_test.md)
- [Test PostgreSQL at commit 3f1aaaa](./docs/test/postgres_test.md)
- [Test SQLite at commit 20e09ba](./docs/test/sqlite_test.md)
- [Test ClickHouse at commit 30464b9](./docs/test/clickhouse_test.md)
- [Test TiDB at commit f5ca27e]()

Because EET randomly generates SQL queries, the queries generated in reviewers' artifact evaluation could be different from the queries we generated in our evaluation. To help reviewers exactly check the reproducibilty of EET, we provide [bug_features.xlsx]() to validate Table 4 in our paper and [bug_latency.xlsx]() to validate Table 6 in our paper.

### Testing latest versions of database systems

We provide scripts for automatically set up testing for latest versions of the supported database systems. EET has extensively tested these systems, and most of the bugs it found has been fixed in their latest versions.

- [Test MySQL latest version]()
- [Test PostgreSQL latest version]()
- [Test SQLite latest version]()
- [Test ClickHouse latest version]()
- [Test TiDB latest version]()

### Recorded Bug-Triggering Queries

When an EET instance trigger a bug, EET will stop the testing campaign and record the bug-triggering queries.

For each logic bug, EET records three SQL queries:

- *db_setup.sql*: this query is used to set up a database under test (e.g., create tables, insert rows).
- *origin.sql*: a randomly generated query
- *eet.sql*: the query transformed from origin.sql by _Equivalent Expression Transformation_.

In principle, eet.sql and origin.sql should produce the same execution results. EET identifies a logic bug if the equivalence is violated.

An example of a logic bug in PostgreSQL:

**db_setup.sql**
```sql
create table t0 (c2 text);
create table t2 (c10 text);
create table t5 (vkey int4, pkey int4, c27 text, c28 text, c29 text, c30 text);
insert into t0 values ('');
insert into t2 values ('');
insert into t5 values (1, 2, 'a', 'a', 'a', 'a'), (0, 1, '', '', 'a', 'L');
```

**origin.sql**
```sql
--- select 1 row
select * from t5
where (t5.pkey >= t5.vkey) <> (t5.c30 = (
    select
        t5.c29 as c_0
      from
        (t2 as ref_0
          inner join t0 as ref_1
          on (ref_0.c10 = ref_1.c2))
      where t5.c28 = t5.c27
      order by c_0 desc limit 1));
```

**eet.sql**
```sql
--- select 0 row
select * from t5 
where (t5.pkey >= t5.vkey) <> (t5.c30 = (
    select
        t5.c29 as c_0
      from
        (t2 as ref_0
          inner join t0 as ref_1
          on (ref_0.c10 = ref_1.c2))
      where ((case when (((ref_0.c10 like 'z~%')
                and (not (ref_0.c10 like 'z~%')))
                and ((ref_0.c10 like 'z~%') is not null)) 
            then t5.c28 else t5.c28 end)
           = (case when (((ref_1.c2 not like '_%%')
                and (not (ref_1.c2 not like '_%%')))
                and ((ref_1.c2 not like '_%%') is not null)) 
            then t5.c29 else t5.c27 end))
      order by c_0 desc limit 1));
```

For each non-logic bug (e.g., crash, internal error), EET record two SQL queries:

- *db_setup.sql*: this query is used to set up a database under test (e.g., create tables, insert rows).
- *unexpected.sql*: a randomly generated query that trigger a crash or an internal error.

An example of a crash in PostgreSQL:

**db_setup.sql**
```sql
create table t1 (pkey int4, c7 float8, c8 text, c9 float8);
insert into t1 (pkey, c7, c8, c9) values (96000, 0.0, '3n@', -79.14);
```

**unexpected.sql**
```sql
--- trigger a stack overflow
update t1 set c7 = t1.c9 / t1.c7 where 'a' @@ repeat(t1.c8, t1.pkey);
```

## Detailed Usage

We provide more information about EET in case you are interested.

### Compile EET in Debian
```shell
apt-get install -y g++ build-essential autoconf autoconf-archive libboost-regex-dev
git clone https://github.com/JZuming/EET
cd EET
autoreconf -if
./configure
make -j
```

### Test Database Systems
```shell
# You should set up the database server before testing
./eet --mysql-db=testdb --mysql-port=3306 # test mysql
./eet --postgres-db=testdb --postgres-port=5432 # test postgres
./eet --sqlite=test.db # test sqlite
./eet --clickhouse-db=testdb --clickhouse-port=9000 # test clickhouse
./eet --tidb-db=testdb --tidb-port=4000 # test tidb
```

### Supported Options

| Option | Description |
|----------|----------|
| `--mysql-db` | Target MySQL database | 
| `--mysql-port` | MySQL server port number | 
| `--postgres-db` | Target PostgreSQL database |
| `--postgres-port` | PostgreSQL server port number |
| `--sqlite` | Target SQLite database |
| `--clickhouse-db` | Target ClickHouse database |
| `--clickhouse-port` | ClickHouse server port number |
| `--tidb-db` | Target TiDB database |
| `--tidb-port` | TiDB server port number |
| `--ignore-crash` | Ignoring crash bug, EET will focus on finding only logic bugs |
| `--db-test-num` | Number of tests for each randomly generated database (default: 50) |