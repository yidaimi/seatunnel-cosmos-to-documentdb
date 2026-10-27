// Inserts N docs into shop.orders as fast as possible, then reports when the target has them.
const s = db.getSiblingDB("shop"), t = new Mongo(DST).getDB("shop");
const before = s.orders.countDocuments(), t0 = Date.now();
for (let b = 0; b < N / 1000; b++) { const a = []; for (let i = 0; i < 1000; i++) a.push({ burst: true, i: b * 1000 + i, at: new Date() }); s.orders.insertMany(a); }
const t1 = Date.now(), target = before + N;
while (t.orders.countDocuments() < target && Date.now() - t1 < 120000) sleep(200);
const t2 = Date.now();
print(`inserted ${N} in ${t1 - t0} ms; target caught up ${t2 - t1} ms later; throughput ~${Math.round(N / ((t2 - t0) / 1000))} docs/s; dst=${t.orders.countDocuments()} expected=${target}`);
