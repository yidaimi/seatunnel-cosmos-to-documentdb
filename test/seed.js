// Seeds the local source with collections covering several BSON types and _id types.
const d = db.getSiblingDB(typeof DB === "undefined" ? "shop" : DB);
d.dropDatabase();
const orders = [];
for (let i = 0; i < 20000; i++) {
  orders.push({ _id: new ObjectId(), no: i, amount: NumberDecimal((i * 1.1).toFixed(2)),
    qty: NumberLong(String(i)), created: new Date(1700000000000 + i * 1000),
    tags: ["a", "b", i % 7], nested: { level: { deep: i % 3 } }, bin: BinData(0, "AQID") });
}
d.orders.insertMany(orders);
const users = [];
for (let i = 0; i < 3000; i++) users.push({ _id: "user-" + i, name: "n" + i, age: NumberInt(i % 90) });
d.users.insertMany(users);
d.mixed.insertMany([{ _id: 1, t: "int" }, { _id: 2.5, t: "double" }, { _id: "s1", t: "string" },
  { _id: new ObjectId(), t: "oid" }, { _id: { k: 1 }, t: "doc" }, { _id: NumberLong("10"), t: "long" }]);
printjson({ orders: d.orders.countDocuments(), users: d.users.countDocuments(), mixed: d.mixed.countDocuments() });
