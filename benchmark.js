const b = require('b');
const lmdb = require('./build/Release/node-lmdb');
const jsonAdapter = require('./adapter').json;
const _ = require('lodash');
let schemapack;
try {
    schemapack = require('schemapack');
} catch (e) {}
let schemapackAdapter;
if (schemapack) {
    schemapackAdapter = require('./adapter').schemapack;
}

const REPEATS = 10000;

require("mkdirp").sync("./testdata");

let env = new lmdb.Env();
env.open({
    path: './testdata',
    maxDbs: 10,
    mapSize: 1024 * 1024 * 1024
});

let dbiString = env.openDbi({
   name: 'testString',
   create: true
});

let dbiJson = env.openDbi({
   name: 'testJson',
   create: true
});
dbiJson.adapter = jsonAdapter;

let dbiSchemapack;
if (schemapack) {
    dbiSchemapack = env.openDbi({
       name: 'testSchemapack',
       create: true
    });
    dbiSchemapack.adapter = schemapackAdapter;
}

let data = {};

const testObject = {
  a: 1,
  b: 'abcdefghijklmnopqrstuvwxwz',
  c: 324324234,
  d: 'abcdefghijklmnopqrstuvwxwz',
  e: [213, 123, 43, 456, 4656, 43345]
};
const testString = JSON.stringify(testObject);

let txn;

console.log('##########################################');
console.log('Run benchmarks...');
let batchWriteString = b('Write string');
batchWriteString.add('lmdb', function (i, done) {
    txn.putString(dbiString, '' + i, testString);
    done();
})
.add('lmdb json', function (i, done) {
    txn.put(dbiJson, '' + i, testString);
    done();
}).add('memory', function (i, done) {
    data['' + i] = testString;
    done();
});
if (schemapack) {
    batchWriteString.add('lmdb schemapack', function (i, done) {
        txn.put(dbiSchemapack, '' + i, testString);
        done();
    });
}


let batchWriteObject = b('Write object');
batchWriteObject.add('lmdb json', function (i, done) {
    let key = '' + i + REPEATS;
    let v = _.cloneDeep(testObject);
    txn.put(dbiJson, key, v);
    done();
}).add('memory', function (i, done) {
    data['' + i + REPEATS] = _.cloneDeep(testObject);
    done();
});
if (schemapack) {
    batchWriteObject.add('lmdb schemapack', function (i, done) {
        let key = '' + i + REPEATS;
        let v = _.cloneDeep(testObject);
        txn.put(dbiSchemapack, key, v);
        done();
    });
}

let batchReadString = b('Read string');
batchReadString.add('lmdb batch', function (i, done) {
    var v = txn.getString(dbiString, '' + i);
    if (typeof v !== 'string') {
        console.error('String read error - lmdb batch');
        process.exit(1);
    }
    done();
})
.add('lmdb json batch', function (i, done) {
    var v = txn.get(dbiJson, '' + i);
    if (typeof v !== 'string') {
        console.error('String read error - lmdb json batch');
        process.exit(1);
    }
    done();
}).add('lmdb', function (i, done) {
    let txn = env.beginTxn({readOnly: true});
    let v = txn.getString(dbiString, '' + i);
    if (typeof v !== 'string') {
        txn.abort();
        console.error('String read error - lmdb');
        process.exit(1);
    }
    txn.abort();
    done();
})
.add('lmdb json', function (i, done) {
    let txn = env.beginTxn({readOnly: true});
    let v = txn.get(dbiJson, '' + i);
    if (typeof v !== 'string') {
        txn.abort();
        console.error('String read error - lmdb json');
        process.exit(1);
    }
    txn.abort();
    done();
}).add('memory', function (i, done) {
    var v = data['' + i];
    if (typeof v !== 'string') {
        console.error('String read error - memory');
        process.exit(1);
    }
    done();
});
if (schemapack) {
    batchReadString.add('lmdb schemapack batch', function (i, done) {
        var v = txn.get(dbiSchemapack, '' + i);
        if (typeof v !== 'string') {
            console.error('String read error - lmdb schemapack batch');
            process.exit(1);
        }
        done();
    }).add('lmdb schemapack', function (i, done) {
        let txn = env.beginTxn({readOnly: true});
        let v = txn.get(dbiSchemapack, '' + i);
        if (typeof v !== 'string') {
            txn.abort();
            console.error('String read error - lmdb schemapack');
            process.exit(1);
        }
        txn.abort();
        done();
    });
}

let batchReadObject = b('Read object');
batchReadObject.add('lmdb json batch', function (i, done) {
    let v = txn.get(dbiJson, '' + i + REPEATS);
    if (!v || typeof v !== 'object') {
        console.error('Object read error - lmdb');
        process.exit(1);
    }
    done();
}).add('lmdb json', function (i, done) {
    var txn = env.beginTxn({readOnly: true});
    let v = txn.get(dbiJson, '' + i + REPEATS);
    if (!v || typeof v !== 'object') {
        txn.abort();
        console.error('Object read error - lmdb');
        process.exit(1);
    }
    txn.abort();
    done();
})
.add('memory', function (i, done) {
    var v = data['' + i + REPEATS];
    if (!v || typeof v !== 'object') {
        console.error('Object read error - memory');
        process.exit(1);
    }
    done();
});
if (schemapack) {
    batchReadObject.add('lmdb schemapack batch', function (i, done) {
        let v = txn.get(dbiSchemapack, '' + i + REPEATS);
        if (!v || typeof v !== 'object') {
            console.error('Object read error - lmdb');
            process.exit(1);
        }
        done();
    }).add('lmdb schemapack', function (i, done) {
        var txn = env.beginTxn({readOnly: true});
        let v = txn.get(dbiSchemapack, '' + i + REPEATS);
        if (!v || typeof v !== 'object') {
            txn.abort();
            console.error('Object read error - lmdb');
            process.exit(1);
        }
        txn.abort();
        done();
    });
}


txn = env.beginTxn();

if (schemapack) {
    dbiSchemapack.schema = schemapack.build('string');
}

batchWriteString.run(REPEATS).then(() => {
    txn.commit();

    if (schemapack) {
        dbiSchemapack.schema = schemapack.build({
            a: 'int32',
            b: 'string',
            c: 'int32',
            d: 'string',
            e: ['int32']
        });
    }

    txn = env.beginTxn();
    return batchWriteObject.run(REPEATS);
}).then(() => {
    txn.commit();

    if (schemapack) {
        dbiSchemapack.schema = schemapack.build('string');
    }

    txn = env.beginTxn({readOnly: true});
    return batchReadString.run(REPEATS);
}).then(() => {
    txn.abort();

    if (schemapack) {
        dbiSchemapack.schema = schemapack.build({
            a: 'int32',
            b: 'string',
            c: 'int32',
            d: 'string',
            e: ['int32']
        });
    }

    txn = env.beginTxn({readOnly: true});
    return batchReadObject.run(REPEATS);
}).then(() => {
    txn.abort();
    console.log('##########################################');
    process.exit();
});
