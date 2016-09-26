const lmdb = require('./build/Release/node-lmdb');
const jsonAdapter = require('./adapter').json;

let env = new lmdb.Env();
env.open({
    path: './testdata',
    maxDbs: 10,
    mapSize: 1024*1024*1024
});

let dbi = env.openDbi({
   name: 'mydb3',
   create: true
});
dbi.adapter = jsonAdapter;

const testObject = {
  a: 1,
  b: 'abcdefghijklmnopqrstuvwxwz',
  c: 324324234,
  d: 'abcdefghijklmnopqrstuvwxwz',
  e: [213, 123, 43, 456, 4656, 43345]
};

let txn = env.beginTxn();
txn.put(dbi, 'test', testObject);
txn.commit();

console.log('Run leak test... (memory use 12K...45K)');
let counter = 0;
setInterval(function () {
    let txn = env.beginTxn({readOnly: true});
    let obj;
    for (var i = 0; i < 1000; i++) {
        obj = txn.get(dbi, 'test');
    }
    txn.abort();
    counter++;
    if (counter % 1000 === 0) console.log(Math.floor(counter / 1000) + 'M reads', obj.c);
}, 1);

if (global.gc) {
    setInterval(function () {
        global.gc();
    }, 15000);
}
