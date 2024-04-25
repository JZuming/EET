## Testing PostgreSQL (latest version)
### Set up testing
```shell
mkdir postgres_test
git clone https://github.com/JZuming/EET.git
cp EET/scripts/postgres/run_test.sh postgres_test
cd postgres_test
git clone https://github.com/postgres/postgres
./run_test.sh 2 1 
# run_test.sh: build 2 docker container, and each container has 1 EET test instance.
# the first argument is the number of docker container
# the second argument is the number of EET test instance in each container
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

