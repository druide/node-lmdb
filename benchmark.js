const Benchmark = require('benchmark');
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

require("mkdirp").sync("./testdata");

let env = new lmdb.Env();
env.open({
    path: './testdata',
    maxDbs: 10,
    mapSize: 1024 * 1024 * 1024
});

let data = {};
let dataObject = {};

const testObject = {
  a: 1,
  b: 'abcdefghijklmnopqrstuvwxwz',
  c: 324324234,
  d: 'abcdefghijklmnopqrstuvwxwz',
  e: [213, 123, 43, 456, 4656, 43345]
};
const testString = JSON.stringify(testObject);

let suits = [];

function nextSuite () {
    let suite = suits.shift();
    if (!suite) {
        console.log('----------------------------------------------------------------');
        console.log('Finished.');
        console.log('################################################################');
        return;
    }
    suite.on('start', function (event) {
        console.log('----------------------------------------------------------------');
        console.log(suite.name);
    })
    .on('complete', function() {
        report(this);
        nextSuite();
    });
    suite.run();
}

function report (suite) {
    let fastest = suite.filter('fastest');
    let fastestMean = fastest[0].stats.mean + fastest[0].stats.moe;
    let all = suite.filter('successful').sort(function (a, b) {
        a = a.stats;
        b = b.stats;
        return (a.mean + a.moe) - (b.mean + b.moe);
    });
    _.each(all, function (bench) {
        let mean = bench.stats.mean + bench.stats.moe;
        if (mean > fastestMean) {
            console.log(String(bench) + ', x' + (mean / fastestMean).toFixed(1) + ' slower');
        } else if (mean < fastestMean) {
            console.log(String(bench) + ', x' + (fastestMean / mean).toFixed(1) + ' faster');
        } else {
            console.log(String(bench) + ', is fastest');
        }
    });
}

let dbi, txn, i;

function inc () {
    i++;
    if (i > 10000) i = 0;
}

console.log('################################################################');
console.log('Run benchmarks...');

let suite = new Benchmark.Suite('Write string');
suite.add({
    name: 'lmdb',
    defer: true,
    delay: 0,
    fn: function (deferred) {
        inc();
        txn.putString(dbi, '' + i, testString);
        deferred.resolve();
    },
    onStart: function () {
        dbi = env.openDbi({
           name: 'testString',
           create: true
        });
        txn = env.beginTxn();
        i = 0;
    },
    onComplete: function () {
        txn.commit();
        dbi.close();
    },
    onError: function (err) {
        console.error(err.message);
    }
})
.add({
    name: 'lmdb json',
    defer: true,
    delay: 0,
    fn: function (deferred) {
        inc();
        txn.put(dbi, '' + i, testString);
        deferred.resolve();
    },
    onStart: function () {
        dbi = env.openDbi({
           name: 'testStringJson',
           create: true
        });
        dbi.adapter = jsonAdapter;
        txn = env.beginTxn();
        i = 0;
    },
    onComplete: function () {
        txn.commit();
        dbi.close();
    },
    onError: function (err) {
        console.error(err.message);
    }
}).add({
    name: 'memory',
    defer: true,
    delay: 0,
    fn: function (deferred) {
        inc();
        data['' + i] = testString;
        deferred.resolve();
    },
    onStart: function () {
        i = 0;
    },
    onError: function (err) {
        console.error(err.message);
    }
});
if (schemapack) {
    suite.add({
        name: 'lmdb schemapack',
        defer: true,
        delay: 0,
        fn: function (deferred) {
            inc();
            txn.put(dbi, '' + i, testString);
            deferred.resolve();
        },
        onStart: function () {
            dbi = env.openDbi({
               name: 'testStringSchemapack',
               create: true
            });
            dbi.adapter = schemapackAdapter;
            dbi.schema = schemapack.build('string');
            txn = env.beginTxn();
            i = 0;
        },
        onComplete: function () {
            txn.commit();
            dbi.close();
        },
        onError: function (err) {
            console.error(err.message);
        }
    });
}
suits.push(suite);

suite = new Benchmark.Suite('Write object');
suite.add({
    name: 'lmdb json',
    defer: true,
    delay: 0,
    fn: function (deferred) {
        inc();
        let v = _.cloneDeep(testObject);
        txn.put(dbi, '' + i, v);
        deferred.resolve();
    },
    onStart: function () {
        dbi = env.openDbi({
           name: 'testObjectJson',
           create: true
        });
        dbi.adapter = jsonAdapter;
        txn = env.beginTxn();
        i = 0;
    },
    onComplete: function () {
        txn.commit();
        dbi.close();
    },
    onError: function (err) {
        console.error(err.message);
    }
}).add({
    name: 'memory',
    defer: true,
    delay: 0,
    fn: function (deferred) {
        inc();
        dataObject['' + i] = _.cloneDeep(testObject);
        deferred.resolve();
    },
    onStart: function () {
        i = 0;
    },
    onError: function (err) {
        console.error(err.message);
    }
});
if (schemapack) {
    suite.add({
        name: 'lmdb schemapack',
        defer: true,
        delay: 0,
        fn: function (deferred) {
            inc();
            let v = _.cloneDeep(testObject);
            txn.put(dbi, '' + i, v);
            deferred.resolve();
        },
        onStart: function () {
            dbi = env.openDbi({
               name: 'testObjectSchemapack',
               create: true
            });
            dbi.adapter = schemapackAdapter;
            dbi.schema = schemapack.build({
                a: 'int32',
                b: 'string',
                c: 'int32',
                d: 'string',
                e: ['int32']
            });
            txn = env.beginTxn();
            i = 0;
        },
        onComplete: function () {
            txn.commit();
            dbi.close();
        },
        onError: function (err) {
            console.error(err.message);
        }
    });
}
suits.push(suite);

suite = new Benchmark.Suite('Read string');
suite.add({
    name: 'lmdb batch',
    defer: true,
    delay: 0,
    fn: function (deferred) {
        inc();
        let v = txn.getString(dbi, '' + i);
        if (typeof v !== 'string') {
            txn.abort();
            console.error('String read error - lmdb batch');
            process.exit(1);
        }
        deferred.resolve();
    },
    onStart: function () {
        dbi = env.openDbi({
           name: 'testString',
           create: true
        });
        txn = env.beginTxn({readOnly: true});
        i = 0;
    },
    onComplete: function () {
        txn.abort();
        dbi.close();
    },
    onError: function (err) {
        console.error(err.message);
    }
})
.add({
    name: 'lmdb json batch',
    defer: true,
    delay: 0,
    fn: function (deferred) {
        inc();
        let v = txn.get(dbi, '' + i);
        if (typeof v !== 'string') {
            txn.abort();
            console.error('String read error - lmdb json batch');
            process.exit(1);
        }
        deferred.resolve();
    },
    onStart: function () {
        dbi = env.openDbi({
           name: 'testStringJson',
           create: true
        });
        dbi.adapter = jsonAdapter;
        txn = env.beginTxn({readOnly: true});
        i = 0;
    },
    onComplete: function () {
        txn.abort();
        dbi.close();
    },
    onError: function (err) {
        console.error(err.message);
    }
}).add({
    name: 'lmdb',
    defer: true,
    delay: 0,
    fn: function (deferred) {
        inc();
        let txn = env.beginTxn({readOnly: true});
        let v = txn.getString(dbi, '' + i);
        if (typeof v !== 'string') {
            txn.abort();
            console.error('String read error - lmdb');
            process.exit(1);
        }
        txn.abort();
        deferred.resolve();
    },
    onStart: function () {
        dbi = env.openDbi({
           name: 'testString',
           create: true
        });
        i = 0;
    },
    onComplete: function () {
        dbi.close();
    },
    onError: function (err) {
        console.error(err.message);
    }
})
.add({
    name: 'lmdb json',
    defer: true,
    delay: 0,
    fn: function (deferred) {
        inc();
        let txn = env.beginTxn({readOnly: true});
        let v = txn.get(dbi, '' + i);
        if (typeof v !== 'string') {
            txn.abort();
            console.error('String read error - lmdb json');
            process.exit(1);
        }
        txn.abort();
        deferred.resolve();
    },
    onStart: function () {
        dbi = env.openDbi({
           name: 'testStringJson',
           create: true
        });
        dbi.adapter = jsonAdapter;
        i = 0;
    },
    onComplete: function () {
        dbi.close();
    },
    onError: function (err) {
        console.error(err.message);
    }
}).add({
    name: 'memory',
    defer: true,
    delay: 0,
    fn: function (deferred) {
        inc();
        let v = data['' + i];
        if (typeof v !== 'string') {
            console.error('String read error - memory');
            process.exit(1);
        }
        deferred.resolve();
    },
    onStart: function () {
        i = 0;
    },
    onError: function (err) {
        console.error(err.message);
    }
});
if (schemapack) {
    suite.add({
        name: 'lmdb schemapack batch',
        defer: true,
        delay: 0,
        fn: function (deferred) {
            inc();
            let v = txn.get(dbi, '' + i);
            if (typeof v !== 'string') {
                txn.abort();
                console.error('String read error - lmdb schemapack batch');
                process.exit(1);
            }
            deferred.resolve();
        },
        onStart: function () {
            dbi = env.openDbi({
               name: 'testStringSchemapack',
               create: true
            });
            dbi.adapter = schemapackAdapter;
            dbi.schema = schemapack.build('string');
            txn = env.beginTxn({readOnly: true});
            i = 0;
        },
        onComplete: function () {
            txn.abort();
            dbi.close();
        },
        onError: function (err) {
            console.error(err.message);
        }
    }).add({
        name: 'lmdb schemapack',
        defer: true,
        delay: 0,
        fn: function (deferred) {
            inc();
            let txn = env.beginTxn({readOnly: true});
            let v = txn.get(dbi, '' + i);
            if (typeof v !== 'string') {
                txn.abort();
                console.error('String read error - lmdb schemapack');
                process.exit(1);
            }
            txn.abort();
            deferred.resolve();
        },
        onStart: function () {
            dbi = env.openDbi({
               name: 'testStringSchemapack',
               create: true
            });
            dbi.adapter = schemapackAdapter;
            dbi.schema = schemapack.build('string');
            i = 0;
        },
        onComplete: function () {
            dbi.close();
        },
        onError: function (err) {
            console.error(err.message);
        }
    });
}
suits.push(suite);

suite = new Benchmark.Suite('Read object');
suite.add({
    name: 'lmdb json batch',
    defer: true,
    delay: 0,
    fn: function (deferred) {
        inc();
        let v = txn.get(dbi, '' + i);
        if (!v || typeof v !== 'object') {
            txn.abort();
            console.error('Object read error - lmdb');
            process.exit(1);
        }
        deferred.resolve();
    },
    onStart: function () {
        dbi = env.openDbi({
           name: 'testObjectJson',
           create: true
        });
        dbi.adapter = jsonAdapter;
        txn = env.beginTxn({readOnly: true});
        i = 0;
    },
    onComplete: function () {
        txn.abort();
        dbi.close();
    },
    onError: function (err) {
        console.error(err.message);
    }
}).add({
    name: 'lmdb json',
    defer: true,
    delay: 0,
    fn: function (deferred) {
        inc();
        let txn = env.beginTxn({readOnly: true});
        let v = txn.get(dbi, '' + i);
        if (!v || typeof v !== 'object') {
            txn.abort();
            console.error('Object read error - lmdb');
            process.exit(1);
        }
        txn.abort();
        deferred.resolve();
    },
    onStart: function () {
        dbi = env.openDbi({
           name: 'testObjectJson',
           create: true
        });
        dbi.adapter = jsonAdapter;
        i = 0;
    },
    onComplete: function () {
        dbi.close();
    },
    onError: function (err) {
        console.error(err.message);
    }
})
.add({
    name: 'memory',
    defer: true,
    delay: 0,
    fn: function (deferred) {
        inc();
        let v = dataObject['' + i];
        if (!v || typeof v !== 'object') {
            console.error('Object read error - memory');
            process.exit(1);
        }
        deferred.resolve();
    },
    onStart: function () {
        i = 0;
    },
    onError: function (err) {
        console.error(err.message);
    }
});
if (schemapack) {
    suite.add({
        name: 'lmdb schemapack batch',
        defer: true,
        delay: 0,
        fn: function (deferred) {
            inc();
            let v = txn.get(dbi, '' + i);
            if (!v || typeof v !== 'object') {
                txn.abort()
                console.error('Object read error - lmdb');
                process.exit(1);
            }
            deferred.resolve();
        },
        onStart: function () {
            dbi = env.openDbi({
               name: 'testObjectSchemapack',
               create: true
            });
            dbi.adapter = schemapackAdapter;
            dbi.schema = schemapack.build({
                a: 'int32',
                b: 'string',
                c: 'int32',
                d: 'string',
                e: ['int32']
            });
            txn = env.beginTxn({readOnly: true});
            i = 0;
        },
        onComplete: function () {
            txn.abort();
            dbi.close();
        },
        onError: function (err) {
            console.error(err.message);
        }
    }).add({
        name: 'lmdb schemapack',
        defer: true,
        delay: 0,
        fn: function (deferred) {
            inc();
            let txn = env.beginTxn({readOnly: true});
            let v = txn.get(dbi, '' + i);
            if (!v || typeof v !== 'object') {
                txn.abort();
                console.error('Object read error - lmdb');
                process.exit(1);
            }
            txn.abort();
            deferred.resolve();
        },
        onStart: function () {
            dbi = env.openDbi({
               name: 'testObjectSchemapack',
               create: true
            });
            dbi.adapter = schemapackAdapter;
            dbi.schema = schemapack.build({
                a: 'int32',
                b: 'string',
                c: 'int32',
                d: 'string',
                e: ['int32']
            });
            i = 0;
        },
        onComplete: function () {
            dbi.close();
        },
        onError: function (err) {
            console.error(err.message);
        }
    });
}
suits.push(suite);

nextSuite();
