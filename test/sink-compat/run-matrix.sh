#!/usr/bin/env bash
# Compatibility matrix: the stock SeaTunnel MongoDB sink (connector-mongodb 3.0.0, Mongo Java
# driver 4.7.1) writing to a real Amazon DocumentDB TLS endpoint. Answers the questions asked
# in apache/seatunnel#10443 for the "Amazon DocumentDB sink" row.
#
# Environment (no credentials are stored in this file):
#   DOCDB_HOST   cluster endpoint, e.g. mycluster.cluster-xxxx.us-west-2.docdb.amazonaws.com
#   DOCDB_USER / DOCDB_PASS
#   CA_PEM       RDS CA bundle (default ~/global-bundle.pem)
#   TRUSTSTORE   PKCS12 truststore built from CA_PEM (password changeit)
#   ST_HOME      SeaTunnel 3.0.0 home
#   TESTS        optional space separated subset, e.g. "T01 T07"
# The test database st_sink_compat is created and DROPPED on the target.
set -u
: "${DOCDB_HOST:?}" "${DOCDB_USER:?}" "${DOCDB_PASS:?}"
ST=${ST_HOME:-$HOME/seatunnel-dl/bin/apache-seatunnel-3.0.0}
CA_PEM=${CA_PEM:-$HOME/global-bundle.pem}
TRUSTSTORE=${TRUSTSTORE:-/tmp/sink-compat/rds-truststore.p12}
W=/tmp/sink-compat; mkdir -p "$W"; chmod 700 "$W"
DB=st_sink_compat
JVM_TRUST="-Djavax.net.ssl.trustStore=$TRUSTSTORE -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=PKCS12"
PASS_ENC=$(python3 -c 'import urllib.parse,os;print(urllib.parse.quote(os.environ["DOCDB_PASS"],safe=""))')
BASE="mongodb://$DOCDB_USER:$PASS_ENC@$DOCDB_HOST:27017"
SHELL_URI="$BASE/?tls=true&tlsCAFile=$CA_PEM&replicaSet=rs0&readPreference=primary&retryWrites=false"
RESULTS=$W/results.tsv; : > "$RESULTS"

shell() { mongosh --quiet "$SHELL_URI" --eval "const d = db.getSiblingDB('$DB'); $1" 2>&1; }

# FakeSource with fixed CDC-style rows: insert 1,2,3 / update 1 / delete 2 / insert 4.
FAKE_CDC='FakeSource {
    schema = { fields { id = bigint, name = string, amount = "decimal(10, 2)", ts = timestamp,
                        flag = boolean, tags = "array<string>", attrs = "map<string, string>" } }
    rows = [
      { kind = INSERT,       fields = [1, "a",  "1.10", "2026-01-01T00:00:00", true,  ["x"],      {k = "v1"}] }
      { kind = INSERT,       fields = [2, "b",  "2.20", "2026-01-02T00:00:00", false, ["y"],      {k = "v2"}] }
      { kind = INSERT,       fields = [3, "c",  "3.30", "2026-01-03T00:00:00", true,  ["z"],      {k = "v3"}] }
      { kind = UPDATE_BEFORE,fields = [1, "a",  "1.10", "2026-01-01T00:00:00", true,  ["x"],      {k = "v1"}] }
      { kind = UPDATE_AFTER, fields = [1, "a2", "9.99", "2026-02-01T00:00:00", false, ["x", "w"], {k = "v9"}] }
      { kind = DELETE,       fields = [2, "b",  "2.20", "2026-01-02T00:00:00", false, ["y"],      {k = "v2"}] }
      { kind = INSERT,       fields = [4, "d",  "4.40", "2026-01-04T00:00:00", true,  [],         {}] }
    ]
  }'
# Insert-only rows: connection / auth tests must not depend on update/delete handling.
FAKE_INS='FakeSource {
    schema = { fields { id = bigint, name = string, amount = "decimal(10, 2)", ts = timestamp,
                        flag = boolean, tags = "array<string>", attrs = "map<string, string>" } }
    rows = [
      { kind = INSERT, fields = [1, "a", "1.10", "2026-01-01T00:00:00", true,  ["x"], {k = "v1"}] }
      { kind = INSERT, fields = [2, "b", "2.20", "2026-01-02T00:00:00", false, ["y"], {k = "v2"}] }
      { kind = INSERT, fields = [3, "c", "3.30", "2026-01-03T00:00:00", true,  ["z"], {k = "v3"}] }
    ]
  }'
fake_bulk() {  # $1 = rows; ids drawn from [1, $2] (duplicates possible -> each row still one write op)
  echo "FakeSource {
    row.num = $1
    split.num = 1
    bigint.min = 1
    bigint.max = $2
    schema = { fields { id = bigint, name = string, amount = \"decimal(10, 2)\", ts = timestamp, flag = boolean } }
  }"
}

# run_job ID "description" "<source block>" "<sink options>" "<uri query>" "<extra JAVA_OPTS>" [mode]
run_job() {
  local id=$1 desc=$2 src=$3 sink=$4 query=$5 jopts=$6 mode=${7:-BATCH}
  if [ -n "${TESTS:-}" ] && ! grep -qw "$id" <<<"$TESTS"; then return; fi
  local conf=$W/$id.conf log=$W/$id.log
  cat > "$conf" <<EOF
env { parallelism = 1, job.mode = "$mode", job.retry.times = 0 }
source { $src }
sink {
  MongoDB {
    uri = "$BASE/$query"
    database = "$DB"
    $sink
  }
}
EOF
  chmod 600 "$conf"
  local t0=$(date +%s.%N)
  JAVA_OPTS="$jopts" timeout 600 "$ST/bin/seatunnel.sh" --config "$conf" -m local > "$log" 2>&1
  local rc=$? t1=$(date +%s.%N)
  local state=$(grep -oE 'end with state [A-Z]+' "$log" | tail -1 | awk '{print $4}')
  local wrote=$(grep -E 'Total Write Count' "$log" | tail -1 | awk '{print $NF}')
  local err=$(grep -oE '(com\.mongodb\.[A-Za-z.]*Exception|javax\.net\.ssl\.[A-Za-z]*Exception|sun\.security\.[A-Za-z.]*Exception)[^\t]{0,230}' "$log" | head -1)
  [ -z "$err" ] && err=$(grep -E '^Caused by|^\s+Caused by' "$log" | tail -1 | sed -E 's/^\s*Caused by: //' | cut -c1-230)
  printf "%s\t%s\t%s\t%s\t%s\t%.1fs\t%s\n" "$id" "$desc" "${state:-rc=$rc}" "${wrote:--}" "$query" \
    "$(echo "$t1 - $t0" | bc)" "${err:--}" | tee -a "$RESULTS"
}

COLL() { echo "collection = \"$1\""; }
OKQ="?tls=true&replicaSet=rs0&retryWrites=false"

echo "== reset target database $DB"; shell 'print("dropped: " + d.dropDatabase().ok)'

# ---- connection: trust construction -------------------------------------------------------
run_job T01 "TLS, CA only via URI tlsCAFile, default JVM trust" "$FAKE_INS" "$(COLL t01)" \
  "?tls=true&tlsCAFile=$CA_PEM&replicaSet=rs0&retryWrites=false" ""
run_job T02 "TLS, URI tlsAllowInvalidCertificates=true (trust-all attempt)" "$FAKE_INS" "$(COLL t02)" \
  "?tls=true&tlsAllowInvalidCertificates=true&replicaSet=rs0&retryWrites=false" ""
run_job T03 "TLS, JVM-wide truststore (-Djavax.net.ssl.trustStore)" "$FAKE_INS" "$(COLL t03)" "$OKQ" "$JVM_TRUST"
# ---- endpoint validation --------------------------------------------------------------------
IP=$(getent hosts "$DOCDB_HOST" | awk '{print $1; exit}')
BASE_IP="mongodb://$DOCDB_USER:$PASS_ENC@$IP:27017"
if [ -z "${TESTS:-}" ] || grep -qw T04 <<<"${TESTS:-}"; then
  BASE_SAVED=$BASE; BASE=$BASE_IP
  run_job T04 "Endpoint by IP, directConnection (hostname check)" "$FAKE_INS" "$(COLL t04)" "?tls=true&directConnection=true&retryWrites=false" "$JVM_TRUST"
  run_job T05 "Endpoint by IP + tlsAllowInvalidHostnames=true" "$FAKE_INS" "$(COLL t05)" "?tls=true&directConnection=true&tlsAllowInvalidHostnames=true&retryWrites=false" "$JVM_TRUST"
  BASE=$BASE_SAVED
fi
run_job T06 "No TLS (tls=false) against TLS-enabled cluster" "$FAKE_INS" "$(COLL t06)" "?tls=false&replicaSet=rs0&retryWrites=false&serverSelectionTimeoutMS=15000" ""
# ---- auth ----------------------------------------------------------------------------------------
if [ -z "${TESTS:-}" ] || grep -qw T07 <<<"${TESTS:-}"; then
  BASE_SAVED=$BASE; BASE="mongodb://$DOCDB_USER:wrong-password@$DOCDB_HOST:27017"
  run_job T07 "Wrong password" "$FAKE_INS" "$(COLL t07)" "$OKQ" "$JVM_TRUST"; BASE=$BASE_SAVED
fi
run_job T08 "authMechanism=SCRAM-SHA-1" "$FAKE_INS" "$(COLL t08)" "$OKQ&authMechanism=SCRAM-SHA-1" "$JVM_TRUST"
run_job T09 "authMechanism=SCRAM-SHA-256" "$FAKE_INS" "$(COLL t09)" "$OKQ&authMechanism=SCRAM-SHA-256" "$JVM_TRUST"
# ---- write semantics -----------------------------------------------------------------------------
run_job T10 "retryWrites not set (driver default true)" "$FAKE_INS" "$(COLL t10)" "?tls=true&replicaSet=rs0" "$JVM_TRUST"
run_job T11 "CDC rows, upsert-enable=true, primary-key=[id]" "$FAKE_CDC" "$(COLL t11)
    upsert-enable = true
    primary-key = [\"id\"]" "$OKQ" "$JVM_TRUST"
run_job T12 "CDC rows, upsert-enable=false (insert/update/delete)" "$FAKE_CDC" "$(COLL t12)
    upsert-enable = false
    primary-key = [\"id\"]" "$OKQ" "$JVM_TRUST"
run_job T13 "transaction=true, upsert, CDC rows" "$FAKE_CDC" "$(COLL t13)
    transaction = true
    upsert-enable = true
    primary-key = [\"id\"]" "$OKQ" "$JVM_TRUST"
# save mode: pre-populate, then DROP_DATA must clear old docs before writing
if [ -z "${TESTS:-}" ] || grep -qw T14 <<<"${TESTS:-}"; then
  shell 'd.t14.insertMany([{ old: 1 }, { old: 2 }]); print("t14 pre-populated: " + d.t14.countDocuments())' | tail -1
fi
run_job T14 "data_save_mode=DROP_DATA on pre-populated collection" "$FAKE_CDC" "$(COLL t14)
    data_save_mode = \"DROP_DATA\"" "$OKQ" "$JVM_TRUST"
if [ -z "${TESTS:-}" ] || grep -qw T15 <<<"${TESTS:-}"; then
  shell 'd.t15.insertOne({ old: 1 }); print("t15 pre-populated: " + d.t15.countDocuments())' | tail -1
fi
run_job T15 "data_save_mode=ERROR_WHEN_DATA_EXISTS on pre-populated collection" "$FAKE_CDC" "$(COLL t15)
    data_save_mode = \"ERROR_WHEN_DATA_EXISTS\"" "$OKQ" "$JVM_TRUST"
# ---- throughput (20k write ops each) -------------------------------------------------------------
run_job P01 "20000 rows, insert (upsert-enable=false)" "$(fake_bulk 20000 1000000000)" "$(COLL p01)
    upsert-enable = false" "$OKQ" "$JVM_TRUST"
if [ -z "${TESTS:-}" ] || grep -qw P02 <<<"${TESTS:-}"; then
  shell 'd.p02.createIndex({ id: 1 }); print("p02 index on id created")' | tail -1
fi
run_job P02 "20000 rows, upsert by id, target index on id" "$(fake_bulk 20000 1000000000)" "$(COLL p02)
    upsert-enable = true
    primary-key = [\"id\"]" "$OKQ" "$JVM_TRUST"
run_job P03 "3000 rows, upsert by id, NO index on id" "$(fake_bulk 3000 1000000000)" "$(COLL p03)
    upsert-enable = true
    primary-key = [\"id\"]" "$OKQ" "$JVM_TRUST"
run_job P04 "30000 rows, transaction=true, upsert (one transaction per 1024-doc bulk)" "$(fake_bulk 30000 1000000000)" "$(COLL p04)
    transaction = true
    upsert-enable = true
    primary-key = [\"id\"]" "$OKQ" "$JVM_TRUST"

echo; echo "== target state for the semantic tests"
shell '
const show = c => print("  " + c + ": " + JSON.stringify(d[c].find({}, { _id: 0 }).sort({ id: 1, old: 1 }).toArray().map(x => x.old !== undefined ? "old" + x.old : x.id + ":" + x.name)));
["t03","t08","t09","t10","t11","t12","t13","t14","t15"].forEach(c => { if (d.getCollectionNames().includes(c)) show(c); else print("  " + c + ": (not created)"); });
const s = d.t11.findOne({ id: 1 }); if (s) print("  t11 id=1 types: " + Object.entries(s).map(([k, v]) => k + "=" + (v === null ? "null" : v.constructor.name)).join(", "));
["p01","p02","p03","p04"].forEach(c => print("  " + c + ": " + (d.getCollectionNames().includes(c) ? d[c].estimatedDocumentCount() + " docs" : "(not created)")));' | tail -20
echo; echo "results: $RESULTS"
