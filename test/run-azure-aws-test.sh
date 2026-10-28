#!/usr/bin/env bash
# End-to-end test: Azure DocumentDB (Cosmos DB for MongoDB vCore) -> Amazon DocumentDB,
# full snapshot + change stream incremental, including source deletes and savepoint/restore.
#
# Credentials are taken from the environment, never stored here:
#   SRC_URI   source connection string (mongodb+srv://...)
#   DST_URI   target connection string for mongosh (must include tlsCAFile=...)
#   JOB_CONF  SeaTunnel job config pointing at the same source/target (default: job-azure-docdb.conf)
#   DB        database used for the test (default: st_e2e) -- it is dropped on both sides!
set -u
: "${SRC_URI:?set SRC_URI}" "${DST_URI:?set DST_URI}"
HERE=$(cd "$(dirname "$0")" && pwd)
ST=${ST_HOME:-$HOME/seatunnel-dl/bin/apache-seatunnel-3.0.0}
DB=${DB:-st_e2e}
JOB_CONF=${JOB_CONF:-$HERE/job-azure-docdb.conf}
CMP="var SRC=\"$SRC_URI\",DST=\"$DST_URI\",SRC_DB=\"$DB\",DST_DB=\"$DB\",WAIT_SECONDS=120"
ts() { date +%T; }
compare() { timeout 600 mongosh --quiet --nodb --eval "$CMP" "$HERE/compare.js"; }

echo "== 0. reset: drop $DB on target, re-seed source [$(ts)]"
mongosh --quiet "$DST_URI" --eval "db.getSiblingDB('$DB').dropDatabase().ok" >/dev/null
mongosh --quiet "$SRC_URI" --eval "var DB='$DB'" "$HERE/seed.js" 2>&1 | tr -d '\n '; echo

echo "== 1. full + incremental with concurrent writes (incl. deletes) and savepoint/restore"
rm -f /tmp/writer-azure.log
(mongosh --quiet "$SRC_URI" --eval "var DB='$DB', SECONDS_TO_RUN=40, DELETES=true" "$HERE/writer.js" > /tmp/writer-azure.log 2>&1 &)
sleep 1
JOB=$("$ST/bin/seatunnel.sh" --config "$JOB_CONF" --async 2>&1 | grep -oE 'job id: [0-9]+' | head -1 | awk '{print $3}')
echo "   [$(ts)] submitted job $JOB"
sleep 15
echo "   [$(ts)] savepoint + stop"; "$ST/bin/seatunnel.sh" -s "$JOB" >/dev/null 2>&1
sleep 8
echo "   [$(ts)] restore";          "$ST/bin/seatunnel.sh" --config "$JOB_CONF" -r "$JOB" --async >/dev/null 2>&1
while [ ! -s /tmp/writer-azure.log ]; do sleep 2; done
echo "   [$(ts)] writer finished: $(tr -d '\n ' < /tmp/writer-azure.log)"
compare

echo "== 2. bulk deletes while the job is running [$(ts)]"
mongosh --quiet "$SRC_URI" --eval "
const d = db.getSiblingDB('$DB');
print('   orders no<2000 deleted: ' + d.orders.deleteMany({ no: { \$lt: 2000 } }).deletedCount);
print('   users age<10 deleted:   ' + d.users.deleteMany({ age: { \$lt: 10 } }).deletedCount);
print('   mixed 2.5/s1 deleted:   ' + d.mixed.deleteMany({ _id: { \$in: [2.5, 's1'] } }).deletedCount);"
compare

echo "== 3. deletes / inserts / updates while the job is stopped [$(ts)]"
"$ST/bin/seatunnel.sh" -s "$JOB" >/dev/null 2>&1
echo "   status: $("$ST/bin/seatunnel.sh" -l 2>/dev/null | grep "$JOB" | awk '{print $3}')"
mongosh --quiet "$SRC_URI" --eval "
const d = db.getSiblingDB('$DB');
print('   orders 2000<=no<3000 deleted: ' + d.orders.deleteMany({ no: { \$gte: 2000, \$lt: 3000 } }).deletedCount);
d.orders.insertMany([{ _id: 'while-stopped-1', v: 1 }, { _id: 'while-stopped-2', v: 2 }]);
print('   orders updated: ' + d.orders.updateMany({ no: { \$gte: 3000, \$lt: 3100 } }, { \$set: { stoppedUpdate: true } }).modifiedCount);
print('   deleted again right after insert: ' + d.orders.deleteOne({ _id: 'while-stopped-2' }).deletedCount);"
echo "   [$(ts)] restore"; "$ST/bin/seatunnel.sh" --config "$JOB_CONF" -r "$JOB" --async >/dev/null 2>&1
compare

echo "== 4. incremental throughput: burst insert [$(ts)]"
mongosh --quiet "$SRC_URI" --eval "var DB='$DB', N=10000, DST=\"$DST_URI\"" "$HERE/burst.js"
compare | tail -2
echo "JOB=$JOB"
