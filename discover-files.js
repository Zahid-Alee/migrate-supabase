// discover-files.js
require('dotenv').config();
const os = require('os');
const { v4: uuidv4 } = require('uuid');
const { supabase } = require('./lib/clients');
const { listDir, absoluteUrl } = require('./lib/bunny');
const { log } = require('./lib/log');
const { sleep } = require('./lib/util');

/**
 * CONFIG
 */
const HEARTBEAT_MS = 15_000;
const STALE_MINUTES = 2;                
const WORKER_ID = uuidv4();
const ALLOW_PARALLEL_DISCOVER = false; 

let HEARTBEAT_TIMER = null;
let CURRENT_JOB = null;

/**
 * HELPERS
 */

// Insert-only directory enqueue. Ignores duplicates (unique index on scan_queue.path).
async function insertDir(path, parent_path) {
  const { error } = await supabase
    .from('scan_queue')
    .insert({ path, parent_path, status: 'queued' })
    .select();
  if (error && !(error.code === '23505' || /duplicate|unique/i.test(error.message))) {
    throw error;
  }
}

// Insert-only for bunny_file_map (files & dirs). Ignores duplicates (unique index on path).
async function insertFileOrDir(entry) {
  const { error } = await supabase
    .from('bunny_file_map')
    .insert(entry)
    .select();
  if (error && !(error.code === '23505' || /duplicate|unique/i.test(error.message))) {
    throw error;
  }
}

async function ensureRootQueued() {
  await insertDir('/', null);
}

// Create a new job row (kind=discover) and ensure progress row exists
async function createJob(note = '') {
  const { data, error } = await supabase
    .from('migration_jobs')
    .insert({
      kind: 'discover',
      note,
      worker_id: WORKER_ID,
      host: os.hostname(),
      status: 'running',
      last_heartbeat: new Date().toISOString()
    })
    .select()
    .single();
  if (error) throw error;

  const { error: pErr } = await supabase
    .from('migration_progress')
    .insert({ job_id: data.id });
  if (pErr && !(pErr.code === '23505' || /duplicate|unique/i.test(pErr.message))) {
    throw pErr;
  }

  return data;
}

async function getJob(jobId) {
  const { data, error } = await supabase
    .from('migration_jobs')
    .select('*')
    .eq('id', jobId)
    .single();
  if (error) throw error;
  return data;
}

async function markJob(status) {
  if (!CURRENT_JOB) return;
  const { error } = await supabase
    .from('migration_jobs')
    .update({ status, ended_at: new Date().toISOString() })
    .eq('id', CURRENT_JOB.id);
  if (error) {
    // just log; don't throw on shutdown
    console.warn('markJob error:', error.message);
  }
}

async function heartbeat() {
  if (!CURRENT_JOB) return;
  const { error } = await supabase.rpc('touch_job', { _job_id: CURRENT_JOB.id });
  if (error) {
    console.warn('heartbeat error:', error.message);
  }
}

function startHeartbeat() {
  if (HEARTBEAT_TIMER) clearInterval(HEARTBEAT_TIMER);
  HEARTBEAT_TIMER = setInterval(heartbeat, HEARTBEAT_MS);
}

function stopHeartbeat() {
  if (HEARTBEAT_TIMER) clearInterval(HEARTBEAT_TIMER);
  HEARTBEAT_TIMER = null;
}

async function reapStale() {
  const { error } = await supabase.rpc('reap_stale_jobs', {
    _kind: 'discover',
    minutes_threshold: STALE_MINUTES
  });
  if (error) {
    // not fatal
    console.warn('reapStale error:', error.message);
  }
}

async function findHealthyRunningJob() {
  const { data, error } = await supabase
    .from('migration_jobs')
    .select('*')
    .eq('kind', 'discover')
    .eq('status', 'running')
    .order('created_at', { ascending: false })
    .limit(1);
  if (error) throw error;

  const j = (data || [])[0];
  if (!j) return null;

  const last = j.last_heartbeat || j.created_at;
  const ageMin = (Date.now() - new Date(last).getTime()) / 60000;
  if (ageMin <= STALE_MINUTES) return j;
  return null;
}

async function claimNextDir() {
  const { data, error } = await supabase.rpc('claim_next_dir');
  if (error) throw error;
  return data && data.length ? data[0] : null;
}

async function markDirDone(id, status = 'done') {
  const { error } = await supabase.from('scan_queue').update({ status }).eq('id', id);
  if (error) throw error;
}

async function bumpProgress(jobId, delta) {
  const { error } = await supabase.rpc('increment_progress', {
    _job_id: jobId,
    _total_bytes_delta: delta.total_bytes || 0,
    _total_files_delta: delta.total_files || 0,
    _scanned_dirs_delta: delta.scanned_dirs || 0,
    _migrated_files_delta: 0,
    _failed_files_delta: 0
  });
  if (error) throw error;
}

/**
 * PROCESS SIGNALS
 */
function hookProcessSignals() {
  process.on('SIGINT', async () => {
    log('SIGINT received');
    stopHeartbeat();
    await markJob('stopped');
    process.exit(0);
  });
  process.on('SIGTERM', async () => {
    log('SIGTERM received');
    stopHeartbeat();
    await markJob('stopped');
    process.exit(0);
  });
  process.on('uncaughtException', async (err) => {
    console.error('Uncaught exception:', err);
    stopHeartbeat();
    await markJob('failed');
    process.exit(1);
  });
}

/**
 * MAIN
 */
async function run() {
  hookProcessSignals();
  await reapStale();
  await ensureRootQueued();

  if (!ALLOW_PARALLEL_DISCOVER) {
    const healthy = await findHealthyRunningJob();
    if (healthy) {
      CURRENT_JOB = healthy;
      log(`Reusing running DISCOVER job=${healthy.id}`);
    } else {
      CURRENT_JOB = await createJob('Concurrent-safe discovery');
    }
  } else {
    CURRENT_JOB = await createJob('Concurrent-safe discovery (parallel)');
  }

  startHeartbeat();
  log(`DISCOVER job=${CURRENT_JOB.id} started`);

  while (true) {
    // keep heartbeat fresh during long loops
    await heartbeat();

    // fetch latest job status
    const fresh = await getJob(CURRENT_JOB.id);
    if (fresh.status === 'paused') { log('Paused…'); await sleep(2000); continue; }
    if (['stopped', 'failed', 'completed'].includes(fresh.status)) {
      log(`Job is ${fresh.status}. Exit.`);
      break;
    }

    const claim = await claimNextDir();
    if (!claim) {
      log('No more directories to scan. Marking job completed.');
      await markJob('completed');
      break;
    }

    const { id: dirId, path } = claim;
    try {
      const items = await listDir(path);
      let filesDelta = 0;
      let bytesDelta = 0;

      for (const item of items) {
        const isDir = !!item.IsDirectory;
        const itemPath = `${path}${item.ObjectName}${isDir ? '/' : ''}`;

        const entry = {
          path: itemPath,
          is_dir: isDir,
          parent_path: path,
          size: isDir ? null : item.Length,
          mime_type: isDir ? null : item.ContentType,
          bunny_url: isDir ? null : absoluteUrl(itemPath),
          status: 'pending'
        };

        // Insert-only; ignore duplicates so we don't mess with statuses
        await insertFileOrDir(entry);

        if (isDir) {
          // INSERT ONLY (no upsert) to avoid re-queueing done/claimed dirs
          await insertDir(itemPath, path);
        } else {
          filesDelta += 1;
          bytesDelta += item.Length || 0;
        }
      }

      await bumpProgress(CURRENT_JOB.id, {
        total_files: filesDelta,
        total_bytes: bytesDelta,
        scanned_dirs: 1
      });

      await markDirDone(dirId, 'done');
      log(`Scanned ${path} — files:+${filesDelta}, bytes:+${bytesDelta}`);
    } catch (e) {
      log(`ERROR scanning ${path}: ${e.message}`);
      await markDirDone(dirId, 'failed');
    }
  }

  stopHeartbeat();
}

run().catch(async (e) => {
  console.error('Fatal:', e);
  stopHeartbeat();
  await markJob('failed');
  process.exit(1);
});
