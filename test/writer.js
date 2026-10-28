// Continuous mixed workload. Vars: SECONDS_TO_RUN, DELETES (true/false)
const d = db.getSiblingDB(typeof DB === "undefined" ? "shop" : DB);
const end = Date.now() + SECONDS_TO_RUN * 1000;
let n = 0, ins = 0, upd = 0, rep = 0, del = 0;
while (Date.now() < end) {
  const r = Math.random();
  if (r < 0.3) { d.orders.insertOne({ no: 100000 + n, amount: NumberDecimal("9.99"), created: new Date(), live: true }); ins++; }
  else if (r < 0.6) { d.orders.updateOne({ no: Math.floor(Math.random() * 20000) }, { $inc: { qty: NumberLong("1") }, $set: { touched: new Date() } }); upd++; }
  else if (r < 0.8) { const id = "user-" + Math.floor(Math.random() * 3500); d.users.replaceOne({ _id: id }, { _id: id, name: "replaced-" + n, age: NumberInt(n % 50) }, { upsert: true }); rep++; }
  else if (DELETES && r < 0.9) { const res = d.users.deleteOne({ _id: "user-" + Math.floor(Math.random() * 3500) }); del += res.deletedCount; }
  else { d.mixed.updateOne({ _id: 1 }, { $set: { counter: n } }); upd++; }
  n++;
}
printjson({ ops: n, ins, upd, rep, del });
