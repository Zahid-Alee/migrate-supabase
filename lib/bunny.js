const { BUNNY_API_KEY, BUNNY_STORAGE_ZONE, BUNNY_REGION_BASE } = require('../config');
const { http } = require('./clients');

const BUNNY_BASE = `${BUNNY_REGION_BASE}/${BUNNY_STORAGE_ZONE}`;

async function listDir(path = '/') {
  const url = `${BUNNY_BASE}${path}`;
  const { data } = await http.get(url, { headers: { AccessKey: BUNNY_API_KEY } });
  return data;
}

function absoluteUrl(path) {
  return `${BUNNY_BASE}${path}`;
}

async function download(path, responseType) {
  const url = absoluteUrl(path);
  return http.get(url, { headers: { AccessKey: BUNNY_API_KEY }, responseType });
}

module.exports = { listDir, absoluteUrl, download };
