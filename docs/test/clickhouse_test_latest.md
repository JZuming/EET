## Testing ClickHouse (latest version)
### Set up testing
```shell
mkdir clickhouse_test
git clone https://github.com/JZuming/EET.git
cp EET/scripts/clickhouse/run_test.sh clickhouse_test
cd clickhouse_test
git clone https://github.com/ClickHouse/ClickHouse.git
git submodule update --init
./run_test.sh 2 1 
# run_test.sh: build 2 docker container, and each container has 1 EET test instance.
# the first argument is the number of docker container
# the second argument is the number of EET test instance in each container

# We recommend to run ./run_test.sh x 1. The value of x depends on the computing
# recourse in the machine. x can be (memory_size - 10g)/10g, as each database server
# in the container might consume ~10g memory. For example, if the machine has 16g 
# memory, the x can be 1 (i.e., ./run_test.sh 1 1). If the machine has 128g memory, 
# the x can be 11 or 12 (e.g., ./run_test.sh 12 1).
```

### Check testing results
```shell
# in test directory
./cat_docker.sh 
# it prints the triggered bug in each container and store the bug-triggering queries in bugs/ directory
```

### Stop testing
```shell
# in test directory
./stop_docker.sh
```

