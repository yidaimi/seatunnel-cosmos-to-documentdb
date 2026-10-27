// One insert on Cosmos, then measures how long until it is visible in DocumentDB. Vars: DST, ID
const t = new Mongo(DST).getDB("cosmos-sourcedb").collection1;
const t0 = Date.now();
db.getSiblingDB("cosmos-sourcedb").collection1.insertOne({ _id: ID, collection: "probe", phase: "latency", at: new Date() });
while (!t.findOne({ _id: ID }) && Date.now() - t0 < 60000) sleep(100);
print("latency " + ID + ": " + (Date.now() - t0) + " ms" + (t.findOne({ _id: ID }) ? "" : " (NOT ARRIVED)"));
