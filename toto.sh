#!/usr/bin/env bash

set +x

npm run start_dmd &
bash wait_for_local_port.bash 9990 40
npm run multiple_backend_test
