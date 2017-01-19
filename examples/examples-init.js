var path = require('path')
var TEST_DIR = path.join(__dirname, "..", "data");
require("mkdirp").sync(TEST_DIR);
module.exports = TEST_DIR;
