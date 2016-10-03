const lmdb = require('../build/Release/node-lmdb');

/**
 * Types:
 * - 1 binary
 * - 2 string
 * - 3 number
 * - 4 boolean
 * - 5 object
 */

/**
 * Convert Buffer to value
 * @param  {Buffer} v
 * @return {*}
 */
function bin2val (v) {
    if (v !== null) {
        let type = v.readUInt8(0);
        switch (type) {
            case 1:
                v = v.slice(1);
                break;
            case 2:
                v = v.toString('utf8', 1);
                break;
            case 3:
                v = v.readDoubleBE(1);
                break;
            case 4:
                v = !!v.readUInt8(1);
                break;
            case 5:
                v = JSON.parse(v.toString('utf8', 1));
                break;
        }
    }
    return v;
}

/**
 * Get value
 * @param  {Dbi} dbi
 * @param  {String} key
 * @return {*}
 */
exports.txnGet = function (dbi, key) {
    let v = this.getBinary(dbi, key);
    return bin2val(v);
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
    if (typeof value === 'object' && Buffer.isBuffer(value)) {
        let buf = Buffer.allocUnsafe(value.length + 1);
        buf.writeUInt8(1, 0);
        value.copy(buf, 1);
        this.putBinary(dbi, key, buf);
    } else if (typeof value === 'string') {
        let strBuf = Buffer.from(value);
        let buf = Buffer.allocUnsafe(1);
        buf.writeUInt8(2, 0);
        this.putBinary(dbi, key, Buffer.concat([buf, strBuf]));
    } else if (typeof value === 'number') {
        let buf = Buffer.allocUnsafe(8 + 1);
        buf.writeUInt8(3, 0);
        buf.writeDoubleBE(value, 1);
        this.putBinary(dbi, key, buf);
    } else if (typeof value === 'boolean') {
        let buf = Buffer.allocUnsafe(1 + 1);
        buf.writeUInt8(4, 0);
        buf.writeUInt8(value ? 1 : 0, 1);
        this.putBinary(dbi, key, buf);
    } else {
        let str = JSON.stringify(value);
        let strBuf = Buffer.from(str);
        let buf = Buffer.allocUnsafe(1);
        buf.writeUInt8(5, 0);
        this.putBinary(dbi, key, Buffer.concat([buf, strBuf]));
    }
};

/**
 * Get cursor current value
 * @param  {Function} cb function (value, key)
 */
exports.cursorGet = function (cb) {
    function fn (key, v) {
        v = bin2val(v);
        return cb(v, key);
    }

    return this.getCurrentBinary(fn);
};

exports.setDefault = function () {
    lmdb.Txn.prototype.get = exports.txnGet;
    lmdb.Txn.prototype.put = exports.txnPut;
    lmdb.Cursor.prototype.get = exports.cursorGet;
};
exports.setDefault = function () {
    lmdb.Txn.prototype.get = exports.txnGet;
    lmdb.Txn.prototype.put = exports.txnPut;
    lmdb.Cursor.prototype.get = exports.cursorGet;
};
