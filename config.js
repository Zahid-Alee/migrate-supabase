require('dotenv').config();

module.exports = {
  BUNNY_API_KEY: process.env.BUNNY_API_KEY,
  BUNNY_STORAGE_ZONE: process.env.BUNNY_STORAGE_ZONE,
  BUNNY_REGION_BASE: process.env.BUNNY_REGION_BASE || 'https://ny.storage.bunnycdn.com',

  SUPABASE_URL: process.env.SUPABASE_URL,
  SUPABASE_KEY: process.env.SUPABASE_KEY,
  SUPABASE_BUCKET: process.env.SUPABASE_BUCKET || 'mammoth-storage',

  SMALL_FILE_THRESHOLD_BYTES: Number(process.env.SMALL_FILE_THRESHOLD_BYTES || 25 * 1024 * 1024),
  CONCURRENCY: Number(process.env.CONCURRENCY || 5),
  MAX_RETRIES: Number(process.env.MAX_RETRIES || 3),
  BATCH_SIZE: Number(process.env.BATCH_SIZE || 1000),
  TEMP_DIR: process.env.TEMP_DIR || '.tmp',
  LOG_FILE: process.env.LOG_FILE || 'migration.log'
};