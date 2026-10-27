// Usage: mongosh --quiet --nodb --eval 'var SRC="...",DST="...",SRC_DB="shop",DST_DB="shop"' compare.js
// Optional WAIT_SECONDS: re-check until identical or timeout (to tell lag from loss).
const src = new Mongo(SRC).getDB(SRC_DB), dst = new Mongo(DST).getDB(DST_DB);
const waitMs = (typeof WAIT_SECONDS === "undefined" ? 0 : WAIT_SECONDS) * 1000, t0 = Date.now();
let ok, lines;
do {
ok = true; lines = [];
for (const c of src.getCollectionNames().filter(n => !n.startsWith("system.")).sort()) {
  const a = new Map(), b = new Map();
  src[c].find().forEach(d => a.set(EJSON.stringify(d._id, { relaxed: false }), EJSON.stringify(d, { relaxed: false })));
  dst[c].find().forEach(d => b.set(EJSON.stringify(d._id, { relaxed: false }), EJSON.stringify(d, { relaxed: false })));
  let missing = 0, extra = 0, diff = 0;
  for (const [k, v] of a) { if (!b.has(k)) missing++; else if (b.get(k) !== v) diff++; }
  for (const k of b.keys()) if (!a.has(k)) extra++;
  const good = missing === 0 && extra === 0 && diff === 0;
  ok = ok && good;
  lines.push(`${good ? "OK  " : "FAIL"} ${c}: src=${a.size} dst=${b.size} missing=${missing} extra=${extra} different=${diff}`);
}
if (!ok && Date.now() - t0 < waitMs) sleep(1000);
} while (!ok && Date.now() - t0 < waitMs);
lines.forEach(l => print(l));
print(`waited ${((Date.now() - t0) / 1000).toFixed(1)}s`);
print(ok ? "RESULT: IDENTICAL" : "RESULT: MISMATCH");
