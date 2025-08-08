const fs = require('fs');
const { LOG_FILE } = require('../config');

function log(line) {
  const s = `[${new Date().toISOString()}] ${line}\n`;
  fs.appendFileSync(LOG_FILE, s);
  console.log(line);
}
module.exports = { log };
