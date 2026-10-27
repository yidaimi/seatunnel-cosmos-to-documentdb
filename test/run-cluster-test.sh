#!/usr/bin/env bash
# End-to-end test on a running SeaTunnel cluster against local MongoDB containers.
# Usage: run-cluster-test.sh <cosmos_ru|standard> <deletes:true|false>
set -u
MODE=${1:-cosmos_ru}; DELETES=${2:-false}
ST=${ST_HOME:-$HOME/seatunnel-dl/bin/apache-seatunnel-3.0.0}
HERE=$(cd "$(dirname "$0")" && pwd)
SRC="mongodb://127.0.0.1:27117/?directConnection=true"; DST="mongodb://127.0.0.1:27118/"
CMP="var SRC=\"$SRC\",DST=\"$DST\",SRC_DB=\"shop\",DST_DB=\"shop\""

mongosh --quiet "$DST" --eval 'db.getSiblingDB("shop").dropDatabase()' >/dev/null
mongosh --quiet "$SRC" "$HERE/seed.js" >/dev/null 2>&1
sed "s/change_stream.mode = \"cosmos_ru\"/change_stream.mode = \"$MODE\"/" "$HERE/job-initial.conf" > /tmp/job-$MODE.conf

(mongosh --quiet "$SRC" --eval "var SECONDS_TO_RUN=40, DELETES=$DELETES" "$HERE/writer.js" > /tmp/writer-$MODE.log 2>&1 &)
sleep 1
JOB=$("$ST/bin/seatunnel.sh" --config /tmp/job-$MODE.conf --async 2>&1 | grep -oE 'job id: [0-9]+' | head -1 | awk '{print $3}')
echo "[$(date +%T)] submitted job $JOB (mode=$MODE deletes=$DELETES)"
sleep 15
echo "[$(date +%T)] savepoint + stop"; "$ST/bin/seatunnel.sh" -s "$JOB" >/dev/null 2>&1
sleep 8
echo "[$(date +%T)] restore";        "$ST/bin/seatunnel.sh" --config /tmp/job-$MODE.conf -r "$JOB" --async >/dev/null 2>&1
sleep 20
echo "[$(date +%T)] writer: $(tr -d '\n ' < /tmp/writer-$MODE.log)"
mongosh --quiet --nodb --eval "$CMP,WAIT_SECONDS=60" "$HERE/compare.js"
echo "[$(date +%T)] throughput burst"
mongosh --quiet "$SRC" --eval "var N=30000, DST=\"$DST\"" "$HERE/burst.js"
mongosh --quiet --nodb --eval "$CMP,WAIT_SECONDS=30" "$HERE/compare.js" | tail -2
echo "JOB=$JOB"
