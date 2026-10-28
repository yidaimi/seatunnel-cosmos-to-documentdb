#!/usr/bin/env bash
# Azure DocumentDB -> Amazon DocumentDB: full + incremental + updates + deletes, and what happens
# to DDL (indexes, empty / new collections, drop collection).
#
# Credentials come from the environment, never from this file:
#   SRC_URI   source connection string (mongodb+srv://...)
#   DST_URI   target connection string for mongosh (must include tlsCAFile=...)
#   JOB_CONF  SeaTunnel job config for the same source/target (default: job-azure-docdb.conf)
#   DB        test database (default: st_e2e) -- DROPPED on both sides!
set -u
: "${SRC_URI:?set SRC_URI}" "${DST_URI:?set DST_URI}"
HERE=$(cd "$(dirname "$0")" && pwd)
ST=${ST_HOME:-$HOME/seatunnel-dl/bin/apache-seatunnel-3.0.0}
DB=${DB:-st_e2e}
JOB_CONF=${JOB_CONF:-$HERE/job-azure-docdb.conf}
CMP="var SRC=\"$SRC_URI\",DST=\"$DST_URI\",SRC_DB=\"$DB\",DST_DB=\"$DB\",WAIT_SECONDS=60"
ts() { date +%T; }
compare() { timeout 600 mongosh --quiet --nodb --eval "$CMP" "$HERE/compare.js"; }
src() { mongosh --quiet "$SRC_URI" --eval "const d = db.getSiblingDB('$DB'); $1"; }
dst() { mongosh --quiet "$DST_URI" --eval "const d = db.getSiblingDB('$DB'); $1"; }
status() { "$ST/bin/seatunnel.sh" -l 2>/dev/null | grep "$JOB" | awk '{print $3}'; }
# Prints "collection: index names" for every collection of the test database.
SHOW_SCHEMA='d.getCollectionNames().sort().forEach(c => print("   " + c.padEnd(16) + " indexes: " +
  d[c].getIndexes().map(i => i.name + (i.unique ? "(unique)" : "") +
  (i.expireAfterSeconds !== undefined ? "(ttl " + i.expireAfterSeconds + "s)" : "")).join(", ")));'

echo "== 0. reset and prepare source schema [$(ts)]"
dst "d.dropDatabase().ok" >/dev/null
mongosh --quiet "$SRC_URI" --eval "var DB='$DB'" "$HERE/seed.js" 2>&1 | tr -d '\n '; echo
src "
d.orders.createIndex({ no: 1 }, { unique: true, name: 'uniq_no' });
d.orders.createIndex({ created: -1, 'nested.level.deep': 1 }, { name: 'cmp_created_deep' });
d.orders.createIndex({ created: 1 }, { expireAfterSeconds: 315360000, name: 'ttl_created' });
d.users.createIndex({ name: 1 }, { name: 'idx_name' });
d.createCollection('empty_coll');"
echo "   source schema:"; src "$SHOW_SCHEMA"

echo "== 1. full + incremental with concurrent writes (incl. deletes), savepoint/restore"
rm -f /tmp/writer-ddl.log
(mongosh --quiet "$SRC_URI" --eval "var DB='$DB', SECONDS_TO_RUN=30, DELETES=true" "$HERE/writer.js" > /tmp/writer-ddl.log 2>&1 &)
sleep 1
JOB=$("$ST/bin/seatunnel.sh" --config "$JOB_CONF" --async 2>&1 | grep -oE 'job id: [0-9]+' | head -1 | awk '{print $3}')
echo "   [$(ts)] submitted job $JOB"
sleep 10
echo "   [$(ts)] savepoint + stop"; "$ST/bin/seatunnel.sh" -s "$JOB" >/dev/null 2>&1
sleep 6
echo "   [$(ts)] restore";          "$ST/bin/seatunnel.sh" --config "$JOB_CONF" -r "$JOB" --async >/dev/null 2>&1
while [ ! -s /tmp/writer-ddl.log ]; do sleep 2; done
echo "   [$(ts)] writer finished: $(tr -d '\n ' < /tmp/writer-ddl.log)"
compare

echo "== 2. updates of every kind while running [$(ts)]"
src "
print('   \$set + \$inc          : ' + d.orders.updateMany({ no: { \$gte: 5000, \$lt: 5200 } }, { \$set: { status: 'paid', 'nested.level.note': 'n' }, \$inc: { qty: NumberLong('5') } }).modifiedCount);
print('   \$unset               : ' + d.orders.updateMany({ no: { \$gte: 5200, \$lt: 5300 } }, { \$unset: { tags: '', bin: '' } }).modifiedCount);
print('   \$push / \$pull array  : ' + d.orders.updateMany({ no: { \$gte: 5300, \$lt: 5400 } }, { \$push: { tags: 'new' } }).modifiedCount + ' / ' + d.orders.updateMany({ no: { \$gte: 5300, \$lt: 5350 } }, { \$pull: { tags: 'a' } }).modifiedCount);
print('   \$rename field        : ' + d.orders.updateMany({ no: { \$gte: 5400, \$lt: 5500 } }, { \$rename: { amount: 'total' } }).modifiedCount);
print('   type change          : ' + d.orders.updateMany({ no: { \$gte: 5500, \$lt: 5550 } }, [{ \$set: { qty: { \$toString: '\$qty' } } }]).modifiedCount);
print('   replaceOne           : ' + d.users.replaceOne({ _id: 'user-500' }, { _id: 'user-500', name: 'replaced', profile: { v: 2 } }).modifiedCount);
print('   update mixed _id     : ' + d.mixed.updateMany({}, { \$set: { touched: true } }).modifiedCount);"
compare

echo "== 3. deletes while running [$(ts)]"
src "
print('   orders no<1000 : ' + d.orders.deleteMany({ no: { \$lt: 1000 } }).deletedCount);
print('   users age 20-25: ' + d.users.deleteMany({ age: { \$gte: 20, \$lte: 25 } }).deletedCount);
print('   mixed _id 1    : ' + d.mixed.deleteOne({ _id: 1 }).deletedCount);"
compare

echo "== 4. DDL while running [$(ts)]"
src "
d.createCollection('new_after_start'); d.new_after_start.insertMany([{ _id: 1, v: 1 }, { _id: 2, v: 2 }]);
d.orders.createIndex({ qty: 1 }, { name: 'idx_qty_added_later' });
d.users.dropIndex('idx_name');
d.orders.insertOne({ _id: 'after-ddl', note: 'written after the DDL statements' });
print('   created collection new_after_start (+2 docs), created index idx_qty_added_later, dropped idx_name');"
sleep 10
echo "   job status: $(status)"
echo "   'after-ddl' doc on target: $(dst "print(d.orders.countDocuments({ _id: 'after-ddl' }))")"
echo "-- source schema:"; src "$SHOW_SCHEMA"
echo "-- target schema:"; dst "$SHOW_SCHEMA"
echo "-- collection options (source vs target):"
src "['orders','empty_coll'].forEach(c => { const i = d.getCollectionInfos({ name: c })[0]; print('   src ' + c + ': ' + JSON.stringify(i ? i.options : null)); })"
dst "['orders','empty_coll'].forEach(c => { const i = d.getCollectionInfos({ name: c })[0]; print('   dst ' + c + ': ' + JSON.stringify(i ? i.options : 'NOT CREATED')); })"
compare

echo "== 5. drop a collection on the source while running [$(ts)]"
src "print('   dropped mixed: ' + d.mixed.drop())"
for i in $(seq 1 20); do s=$(status); [ "$s" != "RUNNING" ] && break; sleep 2; done
echo "   job status after drop: $(status)"
grep "$JOB" "$ST/logs/seatunnel-engine-server.log" | grep -oE "Change stream of [^ ]+ received '[a-zA-Z]+'[^\"]*" | sort -u | head -2
echo "   target mixed still has: $(dst "print(d.mixed.countDocuments())") docs"
echo "JOB=$JOB"
