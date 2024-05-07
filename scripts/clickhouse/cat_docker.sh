#!/bin/bash

n=1
m=1
tool=eet
container=$tool-development-clickhouse
log_file=log
DIR=/root/test

rm bugs/ -rf

docker_num=$(docker ps | grep $container |wc -l)
while [ $m -le $docker_num ]
do
	test_num=$(docker exec -w $DIR -i $container-$m ls -l|grep 'test[1-9][0-9]*'|wc -l)
	while [ $n -le $test_num ]
	do
		echo $container-$m:test$n:
		if test ! -z "$(docker exec -w $DIR -i $container-$m tail -30 test$n/$log_file | grep -i 'bug')"; then
			docker exec -w $DIR -i $container-$m tail -30 test$n/$log_file | grep -i 'bug'
			mkdir -p bugs
			bug_type="crash"
			if test ! -z "$(docker exec -w $DIR -i $container-$m tail -30 test$n/$log_file | grep -i 'logic')"; then
				bug_type="logic"
			fi

			mkdir -p bugs/$container-$m-test$n-$bug_type

			if [ "$bug_type" == "crash" ]; then
				docker cp $container-$m:$DIR/test$n/db_setup.sql bugs/$container-$m-test$n-$bug_type/db_setup.sql
				docker cp $container-$m:$DIR/test$n/unexpected.sql bugs/$container-$m-test$n-$bug_type/unexpected.sql
				docker exec -w $DIR -i $container-$m tail -30 test$n/$log_file | sed -n '/unexpected/,/trigger a memory bug/p' > bugs/$container-$m-test$n-$bug_type/unexpected.err
			else # logic bug
				db_setup=$container-$m:$DIR/test$n/minimized/db_setup.sql

				origin_path=$(docker exec -w $DIR/test$n/minimized -i $container-$m ls -1 | grep 'origin.sql')
				origin=$container-$m:$DIR/test$n/minimized/$origin_path
				origin_out_path=$(docker exec -w $DIR/test$n/minimized -i $container-$m ls -1 | grep 'origin.out')
				origin_out=$container-$m:$DIR/test$n/minimized/$origin_out_path

				eet_path=$(docker exec -w $DIR/test$n/minimized -i $container-$m ls -1 | grep 'qit.sql')
				eet=$container-$m:$DIR/test$n/minimized/$eet_path
				eet_out_path=$(docker exec -w $DIR/test$n/minimized -i $container-$m ls -1 | grep 'qit.out')
				eet_out=$container-$m:$DIR/test$n/minimized/$eet_out_path

				docker cp $db_setup bugs/$container-$m-test$n-$bug_type/db_setup.sql
				docker cp $origin bugs/$container-$m-test$n-$bug_type/origin.sql
				docker cp $eet bugs/$container-$m-test$n-$bug_type/eet.sql
				docker cp $origin_out bugs/$container-$m-test$n-$bug_type/origin.out
				docker cp $eet_out bugs/$container-$m-test$n-$bug_type/eet.out
			fi
			echo ""
		fi
		n=$(( $n + 1))
	done
	m=$(( $m + 1))
	n=1
done
