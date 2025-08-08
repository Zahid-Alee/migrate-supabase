function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function hr(ms) { return `${(ms/1000).toFixed(2)}s`; }
module.exports = { sleep, hr };
