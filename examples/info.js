var lmdb = require('../build/Release/node-lmdb');

var TEST_DIR = "./testdata";
require("mkdirp").sync(TEST_DIR);

var env = new lmdb.Env();
env.open({
    // Path to the environment
    path: TEST_DIR,
    // Maximum number of databases
    maxDbs: 10
});

let info = env.info();
console.log('Map size: ' + Math.round(info.mapSize / 1024 / 1024) + ' MB,',
    'Fill: ' + Math.floor(info.lastPgNo / (info.mapSize / info.pageSize) * 100) + '%,',
    'Readers: ' + info.numReaders + ' of ' + info.maxReaders + ',',
    'DBs: ' + info.entries);

let namesDbi = env.openDbi({
    name: null,
    keyType: 2
});
let txn = env.beginTxn({readOnly: true});
let cursor = new lmdb.Cursor(txn, namesDbi);
let found = cursor.goToFirst();
let dbs = [];

for (; found; found = cursor.goToNext()) {
    cursor.getCurrentNumber((key, data) => {
        let dbi = env.openDbi({
            name: key.toString()
        });
        let stats = dbi.stat(txn);
        dbi.close();
        console.log(key.toString() + ': ' + stats.entryCount + ' records');

    });
}

cursor.close();
txn.abort();
env.close();
