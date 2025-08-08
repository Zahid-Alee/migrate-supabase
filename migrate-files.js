const { supabase } = require('./lib/clients');
const { download } = require('./lib/bunny');
const { log } = require('./lib/log');
const { sleep, hr } = require('./lib/util');
const pLimit = require('p-limit'); // Correctly import p-limit
const { v4: uuidv4 } = require('uuid');
const {
  SUPABASE_BUCKET,
  CONCURRENCY,
  MAX_RETRIES,
  SMALL_FILE_THRESHOLD_BYTES,
  BATCH_SIZE
} = require('./config');

const WORKER_ID = uuidv4();

// Initialize pLimit with the desired concurrency limit
const limit = pLimit(CONCURRENCY);

async function createJob(note = '') {
  try {
    const { data, error } = await supabase
      .from('migration_jobs')
      .insert({ kind: 'migrate', note })
      .select()
      .single();
    if (error) throw error;

    await supabase.from('migration_progress').insert({ job_id: data.id });
    return data;
  } catch (error) {
    console.error('Error creating job:', error.message);
    throw error;
  }
}

async function getJob(jobId) {
  try {
    const { data, error } = await supabase.from('migration_jobs').select('*').eq('id', jobId).single();
    if (error) throw error;
    return data;
  } catch (error) {
    console.error('Error getting job:', error.message);
    throw error;
  }
}

async function claimBatch(batchSize) {
  try {
    const { data, error } = await supabase.rpc('claim_files_to_migrate', {
      batch_size: batchSize,
      worker_id: WORKER_ID
    });
    if (error) throw error;
    return data || [];
  } catch (error) {
    console.error('Error claiming batch:', error.message);
    throw error;
  }
}

async function logResult({ file_id, job_id, status, attempts, bunny_path, supabase_path, time_taken, error_msg }) {
  try {
    const { error } = await supabase.from('migration_logs').insert({
      id: uuidv4(),
      file_id,
      job_id,
      status,
      attempts,
      bunny_path,
      supabase_path,
      time_taken,
      error_msg: error_msg || null
    });
    if (error) throw error;
  } catch (error) {
    console.error('Error logging result:', error.message);
    throw error;
  }
}

async function finalizeFile(id, status) {
  try {
    const { error } = await supabase
      .from('bunny_file_map')
      .update({ status, claimed_at: null, claimed_by: null })
      .eq('id', id);
    if (error) throw error;
  } catch (error) {
    console.error('Error finalizing file:', error.message);
    throw error;
  }
}

async function incProgress(job_id, { migrated = 0, failed = 0 }) {
  try {
    const { error } = await supabase.rpc('increment_progress', {
      _job_id: job_id,
      _total_bytes_delta: 0,
      _total_files_delta: 0,
      _scanned_dirs_delta: 0,
      _migrated_files_delta: migrated,
      _failed_files_delta: failed
    });
    if (error) throw error;
  } catch (error) {
    console.error('Error incrementing progress:', error.message);
    throw error;
  }
}

async function uploadSmall(destPath, mime, bodyBuffer) {
  try {
    return await supabase.storage
      .from(SUPABASE_BUCKET)
      .upload(destPath, bodyBuffer, { upsert: true, contentType: mime || 'application/octet-stream' });
  } catch (error) {
    console.error('Error uploading small file:', error.message);
    throw error;
  }
}

async function uploadStream(destPath, mime, stream) {
  try {
    const { data: signed, error: signErr } = await supabase
      .storage
      .from(SUPABASE_BUCKET)
      .createSignedUploadUrl(destPath);
    if (signErr) throw signErr;

    const { token } = signed;

    const { error } = await supabase
      .storage
      .from(SUPABASE_BUCKET)
      .uploadToSignedUrl(destPath, token, stream, { contentType: mime || 'application/octet-stream' });
    if (error) throw error;

    return { ok: true };
  } catch (error) {
    console.error('Error uploading stream:', error.message);
    throw error;
  }
}

async function migrateOne(job, file) {
  const start = Date.now();
  let attempt = 0;
  const destPath = file.path.startsWith('/') ? file.path.slice(1) : file.path;

  while (attempt < MAX_RETRIES) {
    attempt++;
    try {
      const responseType = file.size > SMALL_FILE_THRESHOLD_BYTES ? 'stream' : 'arraybuffer';
      const resp = await download(file.path, responseType);

      if (responseType === 'arraybuffer') {
        const buf = Buffer.from(resp.data);
        const { error } = await uploadSmall(destPath, file.mime_type, buf);
        if (error) throw error;
      } else {
        const readStream = resp.data; // axios stream
        await uploadStream(destPath, file.mime_type, readStream);
      }

      await logResult({
        file_id: file.id,
        job_id: job.id,
        status: 'success',
        attempts: attempt,
        bunny_path: file.path,
        supabase_path: destPath,
        time_taken: Date.now() - start
      });
      await finalizeFile(file.id, 'migrated');
      await incProgress(job.id, { migrated: 1 });

      log(`OK  ${file.path} (${hr(Date.now() - start)})`);
      return;
    } catch (e) {
      const final = attempt >= MAX_RETRIES;
      if (final) {
        await logResult({
          file_id: file.id,
          job_id: job.id,
          status: 'failed',
          attempts: attempt,
          bunny_path: file.path,
          supabase_path: destPath,
          time_taken: Date.now() - start,
          error_msg: e.message
        });
        await finalizeFile(file.id, 'failed');
        await incProgress(job.id, { failed: 1 });
        log(`FAIL ${file.path} after ${attempt} attempts :: ${e.message}`);
        return;
      }
      const backoff = 1000 * attempt * attempt;
      log(`WARN ${file.path} attempt ${attempt} failed: ${e.message} → retry in ${backoff}ms`);
      await sleep(backoff);
    }
  }
}

async function run() {
  const job = await createJob('Concurrent migration worker');
  log(`MIGRATE job=${job.id} worker=${WORKER_ID} started`);

  // Periodic reclaim of stale in-progress claims
  setInterval(async () => {
    try {
      await supabase.rpc('reclaim_inprogress_to_pending', { minutes_threshold: 30 });
    } catch (e) {
      console.warn('Reclaim failed:', e.message);
    }
  }, 5 * 60 * 1000);

  while (true) {
    const fresh = await getJob(job.id);
    if (fresh.status === 'paused') { log('Paused…'); await sleep(2000); continue; }
    if (['stopped', 'failed', 'completed'].includes(fresh.status)) { log(`Job is ${fresh.status}. Exit.`); break; }

    const batch = await claimBatch(BATCH_SIZE);
    if (batch.length === 0) {
      await sleep(2000);
      const again = await claimBatch(BATCH_SIZE);
      if (again.length === 0) {
        log('No more claimable files. Marking job completed.');
        await supabase.from('migration_jobs').update({ status: 'completed' }).eq('id', job.id);
        break;
      }
      await Promise.all(again.map(f => limit(() => migrateOne(job, f))));
      continue;
    }

    await Promise.all(batch.map(f => limit(() => migrateOne(job, f))));
  }
}

run().catch(e => { console.error(e); process.exit(1); });