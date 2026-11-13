#!/usr/bin/env bash
# Real-server validation of the Amazon DocumentDB source connector (connector-amazondocumentdb,
# apache/seatunnel#12046, unreleased, built from apache/seatunnel dev) against a live Amazon
# DocumentDB TLS endpoint. Requested in apache/seatunnel#10443 ("real-server validation").
#
# Environment (no credentials are stored in this file):
#   DOCDB_HOST   cluster endpoint, e.g. mycluster.cluster-xxxx.us-west-2.docdb.amazonaws.com
#   DOCDB_USER / DOCDB_PASS
#   CA_PEM       Amazon RDS CA bundle (default ~/global-bundle.pem)
#   ST_HOME      SeaTunnel home built from apache/seatunnel dev
#   TESTS        optional space separated subset, e.g. "S01 S03"
# Test data: database st_source_compat, collection orders (10 documents, see prepare_data).
# No -Djavax.net.ssl.* option is passed to any job: only the connector's tls_ca_file is used.
set -u
: "${DOCDB_HOST:?}" "${DOCDB_USER:?}" "${DOCDB_PASS:?}" "${ST_HOME:?}"
ST=$ST_HOME
CA_PEM=${CA_PEM:-$HOME/global-bundle.pem}
W=/tmp/source-compat; mkdir -p "$W"; chmod 700 "$W"
DB=st_source_compat
PASS_ENC=$(python3 -c 'import urllib.parse,os;print(urllib.parse.quote(os.environ["DOCDB_PASS"],safe=""))')
BASE="mongodb://$DOCDB_USER:$PASS_ENC@$DOCDB_HOST:27017"
RESULTS=$W/results.tsv; : > "$RESULTS"

# A CA that did not sign the cluster certificate.
if [ ! -s "$W/untrusted-ca.pem" ]; then
  openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj "/CN=Untrusted Test CA" \
    -keyout "$W/untrusted-ca.key" -out "$W/untrusted-ca.pem" 2>/dev/null
  rm -f "$W/untrusted-ca.key"
fi

SCHEMA='schema = { fields {
      _id = string, order_no = bigint, status = string, qty = int, price = double,
      amount = "decimal(10, 2)", created_at = timestamp, paid = boolean,
      tags = "array<string>", address = "map<string, string>", note = string } }'

assert_rows() {  # $1 = exact row count
  echo "Assert {
    plugin_input = \"orders\"
    rules {
      row_rules = [ { rule_type = MIN_ROW, rule_value = $1 }, { rule_type = MAX_ROW, rule_value = $1 } ]
      field_rules = [
        { field_name = order_no, field_type = bigint, field_value = [ { rule_type = NOT_NULL } ] }
        { field_name = status,   field_type = string, field_value = [ { rule_type = NOT_NULL } ] }
      ]
    }
  }
  Console { plugin_input = \"orders\" }"
}

# run_job ID "description" "<uri query>" "<extra source options>" "<sink blocks>"
run_job() {
  local id=$1 desc=$2 query=$3 opts=$4 sinks=$5
  if [ -n "${TESTS:-}" ] && ! grep -qw "$id" <<<"$TESTS"; then return; fi
  local conf=$W/$id.conf log=$W/$id.log host=${HOST_OVERRIDE:-$DOCDB_HOST}
  cat > "$conf" <<EOF
env { parallelism = 1, job.mode = "BATCH", job.retry.times = 0 }
source {
  AmazonDocumentDB {
    plugin_output = "orders"
    uri = "mongodb://$DOCDB_USER:$PASS_ENC@$host:27017/$query"
    database = "$DB"
    collection = "orders"
    $opts
    $SCHEMA
  }
}
sink { $sinks }
EOF
  chmod 600 "$conf"
  local t0=$(date +%s.%N)
  timeout 600 "$ST/bin/seatunnel.sh" --config "$conf" -m local > "$log" 2>&1
  local rc=$? t1=$(date +%s.%N)
  local state=$(grep -oE 'end with state [A-Z]+' "$log" | tail -1 | awk '{print $4}')
  local read=$(grep -E 'Total Read Count' "$log" | tail -1 | awk '{print $NF}')
  local err=$(grep -oE '(com\.mongodb\.[A-Za-z.]*Exception|javax\.net\.ssl\.[A-Za-z]*Exception|sun\.security\.[A-Za-z.]*Exception|java\.lang\.IllegalArgumentException)[^\t]{0,230}' "$log" | head -1)
  [ -z "$err" ] && err=$(grep -E '^Caused by|^\s+Caused by' "$log" | tail -1 | sed -E 's/^\s*Caused by: //' | cut -c1-230)
  printf "%s\t%s\t%s\t%s\t%s\t%.1fs\t%s\n" "$id" "$desc" "${state:-rc=$rc}" "${read:--}" "$query" \
    "$(echo "$t1 - $t0" | bc)" "${err:--}" | tee -a "$RESULTS"
}

CA="tls_ca_file = \"$CA_PEM\""
FAST="serverSelectionTimeoutMS=15000"

run_job S01 "Full read, tls_ca_file = RDS bundle, retryWrites not in URI" \
  "?replicaSet=rs0&readPreference=secondaryPreferred" "$CA" "$(assert_rows 10)"
run_job S02 "match.query + match.projection" \
  "?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false" \
  "$CA
    match.query = \"{\\\"status\\\": \\\"OPEN\\\"}\"
    match.projection = \"{\\\"order_no\\\": 1, \\\"status\\\": 1, \\\"amount\\\": 1}\"" "$(assert_rows 4)"
run_job S03 "tls_ca_file = unrelated CA" \
  "?replicaSet=rs0&$FAST" "tls_ca_file = \"$W/untrusted-ca.pem\"" "$(assert_rows 10)"
run_job S04 "tls = true (default) without tls_ca_file" \
  "?replicaSet=rs0" "" "$(assert_rows 10)"
run_job S05 "URI retryWrites=true" \
  "?replicaSet=rs0&retryWrites=true" "$CA" "$(assert_rows 10)"
run_job S06 "tls = false option overrides URI tls=true (cluster requires TLS)" \
  "?tls=true&replicaSet=rs0&$FAST" "tls = false" "$(assert_rows 10)"
IP=$(getent ahostsv4 "$DOCDB_HOST" | awk 'NR==1{print $1}')
if [ -n "$IP" ]; then
  HOST_OVERRIDE=$IP run_job S07 "Endpoint by IP, directConnection (hostname check)" \
    "?directConnection=true&$FAST" "$CA" "$(assert_rows 10)"
  HOST_OVERRIDE=$IP run_job S08 "Endpoint by IP + tlsAllowInvalidHostnames=true" \
    "?directConnection=true&tlsAllowInvalidHostnames=true&$FAST" "$CA" "$(assert_rows 10)"
fi

echo; echo "results: $RESULTS"
