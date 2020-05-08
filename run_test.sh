#!/bin/bash
rq worker download_object &
rq worker create_sip &
rq worker submit_sip &
echo "Press anything to shutdown the workers";
read -n 1
killall -s TERM rq
