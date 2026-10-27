// Small, counted write sets against Cosmos. Var: PHASE = "live" | "stopped"
const c = db.getSiblingDB("cosmos-sourcedb").collection1;
let ops = 0;
if (PHASE === "live") {
  c.insertOne({ _id: "live-1", collection: "probe", phase: "live", amount: NumberDecimal("12.34"), at: new Date() }); ops++;
  c.insertOne({ _id: ObjectId(), collection: "probe", phase: "live", nested: { a: [1, 2, { b: true }] } }); ops++;
  c.insertOne({ _id: NumberLong("42"), collection: "probe", phase: "live", big: NumberLong("9007199254740993") }); ops++;
  c.updateOne({ _id: "s-1" }, { $set: { liveUpdate: 1 }, $inc: { n: 100 } }); ops++;
  c.updateOne({ _id: 7 }, { $unset: { replaced: "" }, $set: { liveUpdate: 2 } }); ops++;
  c.replaceOne({ _id: "live-1" }, { _id: "live-1", collection: "probe", phase: "live", replacedLive: true }); ops++;
} else {
  c.insertOne({ _id: "stopped-1", collection: "probe", phase: "stopped", at: new Date() }); ops++;
  c.insertOne({ _id: "stopped-2", collection: "probe", phase: "stopped", bin: BinData(0, "AQIDBA==") }); ops++;
  c.updateOne({ _id: "s-1" }, { $set: { stoppedUpdate: true } }); ops++;
  c.updateOne({ _id: NumberLong("42") }, { $set: { stoppedUpdate: true } }); ops++;
}
print(PHASE + ": " + ops + " write(s), collection now has " + c.countDocuments() + " docs");
