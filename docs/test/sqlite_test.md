## Testing SQLite
### Set up testing
```shell
mkdir sqlite_test
git clone https://github.com/JZuming/EET.git
cp EET/scripts/sqlite/run_test.sh sqlite_test
cd sqlite_test
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
./stop_docker.sh
```
