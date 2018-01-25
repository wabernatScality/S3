#!/usr/bin/env bash

npm run start_dmd &
bash wait_for_local_port.bash 9990 40
npm run multiple_backend_test
