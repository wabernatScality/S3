#!/usr/bin/env bash

set +x

npm-run-all start_dmd start_s3server > npm-out 2> npm-err &
sleep 10
npm run multiple_backend_test
