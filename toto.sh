#!/usr/bin/env bash

set +x

npm-run-all start_dmd start_s3server > npm-out 2> npm-err &
while true
do nc -w 1 localhost 9990
    ret=$?
    echo "ret: $ret"
    if [ "$ret" -eq 0 ]
    then
        break
    fi
    sleep 0.5
done
npm run multiple_backend_test
