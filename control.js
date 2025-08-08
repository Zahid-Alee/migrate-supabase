// control.js
const express = require('express');
const cors = require('cors');
const { supabase } = require('./lib/clients');

const app = express();
app.use(cors());
app.use(express.json());

// Health
app.get('/health', (_req, res) => res.json({ ok: true }));

// --- Job controls ---
app.post('/jobs/:id/status', async (req, res) => {
  const { id } = req.params;
  const { status } = req.body; // running | paused | stopped
  if (!['running', 'paused', 'stopped'].includes(status)) {
    return res.status(400).json({ error: 'Invalid status' });
  }
  const { error } = await supabase
    .from('migration_jobs')
    .update({ status, updated_at: new Date().toISOString() })
    .eq('id', id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ ok: true });
});

// --- Single-file retry ---
app.post('/files/:id/retry', async (req, res) => {
  const { id } = req.params;
  const { error } = await supabase
    .from('bunny_file_map')
    .update({ status: 'pending', claimed_at: null, claimed_by: null })
    .eq('id', id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ ok: true });
});

// --- Progress for a job ---
app.get('/progress/:jobId', async (req, res) => {
  const { jobId } = req.params;
  const { data, error } = await supabase
    .from('migration_progress')
    .select('*')
    .eq('job_id', jobId)
    .single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// --- List jobs (optional filters) ---
app.get('/jobs', async (req, res) => {
  const { kind, status } = req.query;
  const limit = Math.max(1, Math.min(200, Number(req.query.limit || 50)));
  let q = supabase
    .from('migration_jobs')
    .select('*')
    .order('created_at', { ascending: false })
    .limit(limit);
  if (kind) q = q.eq('kind', kind);
  if (status) q = q.eq('status', status);
  const { data, error } = await q;
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// --- Recent logs for a job ---
app.get('/logs', async (req, res) => {
  const { jobId, status } = req.query;
  if (!jobId) return res.status(400).json({ error: 'jobId required' });
  const limit = Math.max(1, Math.min(1000, Number(req.query.limit || 100)));

  let q = supabase
    .from('migration_logs')
    .select('id,status,attempts,bunny_path,supabase_path,time_taken,upload_time,error_msg')
    .eq('job_id', jobId)
    .order('upload_time', { ascending: false })
    .limit(limit);

  if (status) q = q.eq('status', status);

  const { data, error } = await q;
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// --- Files (filter/search) ---
app.get('/files', async (req, res) => {
  const { status, q: search } = req.query;
  const limit = Math.max(1, Math.min(1000, Number(req.query.limit || 100)));

  let query = supabase
    .from('bunny_file_map')
    .select('id,path,size,status,updated_at')
    .eq('is_dir', false)
    .order('updated_at', { ascending: false })
    .limit(limit);

  if (status) query = query.eq('status', status);
  if (search) query = query.ilike('path', `%${search}%`);

  const { data, error } = await query;
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// --- Files currently in progress ---
app.get('/files-inprogress', async (req, res) => {
  const limit = Math.max(1, Math.min(1000, Number(req.query.limit || 100)));
  const { data, error } = await supabase
    .from('bunny_file_map')
    .select('id,path,size,claimed_at,claimed_by')
    .eq('status', 'in_progress')
    .order('claimed_at', { ascending: false })
    .limit(limit);
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// --- Bulk retry ---
app.post('/retry-bulk', async (req, res) => {
  const { status, ids } = req.body || {};
  if (!status && !ids) return res.status(400).json({ error: 'Provide status or ids' });

  let q = supabase.from('bunny_file_map').update({ status: 'pending', claimed_at: null, claimed_by: null });
  if (Array.isArray(ids) && ids.length) q = q.in('id', ids);
  else if (status) q = q.eq('status', status);

  const { error } = await q;
  if (error) return res.status(500).json({ error: error.message });
  res.json({ ok: true });
});

// --- Reclaim stale in-progress claims ---
app.post('/reclaim-inprogress', async (req, res) => {
  const minutes = Math.max(1, Number(req.body?.minutes ?? 30));
  const { error } = await supabase.rpc('reclaim_inprogress_to_pending', { minutes_threshold: minutes });
  if (error) return res.status(500).json({ error: error.message });
  res.json({ ok: true });
});


// POST /reclaim-dirs { minutes: 30 }
app.post('/reclaim-dirs', async (req, res) => {
  const minutes = Math.max(1, Number(req.body?.minutes ?? 30));
  const { error } = await supabase.rpc('reclaim_dirs', { minutes_threshold: minutes });
  if (error) return res.status(500).json({ error: error.message });
  res.json({ ok: true });
});


// Mark stale discover jobs failed (e.g., no heartbeat > 2 min)
// Mark stale discover jobs failed (e.g., no heartbeat > 2 min)
app.post('/jobs/reap-stale', async (req, res) => {
  const { kind = 'discover', minutes = 2 } = req.body || {};
  const { data, error } = await supabase.rpc('reap_stale_jobs', { _kind: kind, minutes_threshold: Number(minutes) });
  if (error) return res.status(500).json({ error: error.message });
  res.json({ reaped: data ?? 0 });
});



// 404 & errors
app.use((_req, res) => res.status(404).json({ error: 'Not found' }));
app.use((err, _req, res, _next) => {
  console.error('API error:', err);
  res.status(500).json({ error: err.message || 'Internal error' });
});

app.listen(4000, () => console.log('Control API running on :4000'));
