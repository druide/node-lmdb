const lmdb = require('../build/Release/node-lmdb');

/**
 * dbi must have build `schema` property assigned.
 */

/**
 * Get value
 * @param  {Dbi} dbi
 * @param  {String} key
 * @return {*}
 */
exports.txnGet = function (dbi, key) {
    let v = this.getBinary(dbi, key);
    if (v !== null) {
        v = dbi.schema.decode(v);
    }
    return v;
};

/**
 * Put value
 * @param  {Dbi} dbi
 * @param  {String} key
 * @param  {*} value
 */
exports.txnPut = function (dbi, key, value) {
    if (value === null || typeof value === 'undefined') {
        return this.del(dbi, key);
    }
    var buf = dbi.schema.encode(value);
    return this.putBinary(dbi, key, buf);
};

/**
 * Get cursor current value
 * @param  {Function} cb function (value, key)
 */
exports.cursorGet = function (cb) {
    function fn (v, key) {
        if (v !== null) {
            v = dbi.schema.decode(v);
        }
        return cb(v, key);
    }

    return this.getCurrentBinary(fn);
};

/**
 * Set this adapter as global default.
 */
exports.setDefault = function () {
    lmdb.Txn.prototype.get = exports.txnGet;
    lmdb.Txn.prototype.put = exports.txnPut;
    lmdb.Cursor.prototype.get = exports.cursorPut;
};
