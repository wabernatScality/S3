#!/usr/bin/env bash

set +x

npm-run-all --parallel start_mdserver start_dataserver > npm-out 2> npm-err &
while true
do nc -w 1 localhost 9991
    ret=$?
    echo "ret: $ret"
    if [ "$ret" -eq 0 ]
    then
        break
    fi
    sleep 0.5
done
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
CI=true S3BACKEND=mem S3DATA=multiple mocha -t 20000 --no-exit --trace --recursive tests/multipleBackend
