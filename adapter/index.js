var lmdb = require('..');

lmdb.Txn.prototype.get = function (dbi, key) {
    if (dbi.adapter) {
        return dbi.adapter.txnGet.call(this, dbi, key);
    }
    return this.getBinary(dbi, key);
};

lmdb.Txn.prototype.put = function (dbi, key, value) {
    if (value === null || typeof value === 'undefined') {
        return this.del(dbi, key);
    }
    if (dbi.adapter) {
        return dbi.adapter.txnPut.call(this, dbi, key, value);
    }
    return this.putBinary(dbi, key, buf);
};

lmdb.Cursor.prototype.get = function (cb) {
    if (dbi.adapter) {
        return dbi.adapter.cursorGet.call(this, cb);
    }
    return this.getCurrentBinary(cb);
};

exports.json = require('./json');
try {
    exports.schemapack = require('./schemapack');
} catch (e) {}
