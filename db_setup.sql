-- Jobs (discover / migrate) with status control
CREATE TABLE public.migration_jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  kind TEXT NOT NULL CHECK (kind IN ('discover','migrate')),
  status TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running','paused','stopped','completed','failed')),
  note TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Aggregate progress per job
CREATE TABLE public.migration_progress (
  job_id UUID PRIMARY KEY REFERENCES public.migration_jobs(id) ON DELETE CASCADE,
  total_bytes BIGINT NOT NULL DEFAULT 0,
  total_files BIGINT NOT NULL DEFAULT 0,
  scanned_dirs BIGINT NOT NULL DEFAULT 0,
  migrated_files BIGINT NOT NULL DEFAULT 0,
  failed_files BIGINT NOT NULL DEFAULT 0,
  last_update TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Work queue for discovery (resumable)
CREATE TABLE public.scan_queue (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  path TEXT NOT NULL,             -- directory path, trailing slash for folders (e.g. '/foo/bar/')
  parent_path TEXT,
  status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','claimed','done','failed')),
  claimed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- File/dir map from Bunny
CREATE TABLE public.bunny_file_map (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  path TEXT NOT NULL,                     -- full path (files end without '/', dirs end with '/')
  is_dir BOOLEAN NOT NULL,
  parent_path TEXT,
  size BIGINT,
  mime_type TEXT,
  bunny_url TEXT,
  status TEXT NOT NULL DEFAULT 'pending', -- 'pending','scanned','migrated','failed'
  scan_time TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Per-file migration attempts/results
CREATE TABLE public.migration_logs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  file_id UUID REFERENCES public.bunny_file_map(id) ON DELETE CASCADE,
  job_id UUID NOT NULL REFERENCES public.migration_jobs(id) ON DELETE CASCADE,
  status TEXT NOT NULL CHECK (status IN ('pending','success','failed')),
  error_msg TEXT,
  attempts INT NOT NULL DEFAULT 1,
  bunny_path TEXT NOT NULL,
  supabase_path TEXT,
  upload_time TIMESTAMPTZ NOT NULL DEFAULT now(),
  time_taken INT  -- ms
);



----setp 2----------
CREATE UNIQUE INDEX uniq_scan_queue_path        ON public.scan_queue(path);
CREATE UNIQUE INDEX uniq_bunny_file_map_path    ON public.bunny_file_map(path);

CREATE INDEX idx_bfm_status                     ON public.bunny_file_map(status);
CREATE INDEX idx_bfm_is_dir                     ON public.bunny_file_map(is_dir);
CREATE INDEX idx_bfm_status_isdir               ON public.bunny_file_map(status, is_dir);

CREATE INDEX idx_logs_file_id                   ON public.migration_logs(file_id);
CREATE INDEX idx_logs_status                    ON public.migration_logs(status);
CREATE INDEX idx_logs_job_id                    ON public.migration_logs(job_id);

-----step 3-----------

CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_scan_queue_updated_at
BEFORE UPDATE ON public.scan_queue
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

CREATE TRIGGER trg_bunny_file_map_updated_at
BEFORE UPDATE ON public.bunny_file_map
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

CREATE TRIGGER trg_jobs_updated_at
BEFORE UPDATE ON public.migration_jobs
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();




--------step 4-----------

CREATE OR REPLACE FUNCTION public.claim_next_dir()
RETURNS TABLE(id uuid, path text, parent_path text)
LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY
  WITH cte AS (
    SELECT q.id, q.path, q.parent_path
    FROM public.scan_queue AS q
    WHERE q.status = 'queued'
    ORDER BY q.path
    FOR UPDATE SKIP LOCKED
    LIMIT 1
  )
  UPDATE public.scan_queue AS q2
  SET status = 'claimed',
      claimed_at = now()
  FROM cte
  WHERE q2.id = cte.id
  RETURNING cte.id, cte.path, cte.parent_path;
END;
$$;

-------step 5-----------

CREATE OR REPLACE FUNCTION public.claim_files_to_migrate(batch_size INT)
RETURNS TABLE(id uuid, path text, bunny_url text, mime_type text, size bigint) 
LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY
  WITH cte AS (
    SELECT b.id, b.path, b.bunny_url, b.mime_type, b.size
    FROM public.bunny_file_map b
    WHERE b.status = 'pending' AND b.is_dir = FALSE
    ORDER BY b.path
    FOR UPDATE SKIP LOCKED
    LIMIT batch_size
  )
  UPDATE public.bunny_file_map b2
  SET status = 'scanned'
  FROM cte
  WHERE b2.id = cte.id
  RETURNING cte.id, cte.path, cte.bunny_url, cte.mime_type, cte.size;
END;
$$;


----step 6-----------

CREATE OR REPLACE FUNCTION public.increment_progress(
  _job_id uuid,
  _total_bytes_delta bigint,
  _total_files_delta bigint,
  _scanned_dirs_delta bigint,
  _migrated_files_delta bigint,
  _failed_files_delta bigint
)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  UPDATE public.migration_progress
  SET total_bytes    = total_bytes    + COALESCE(_total_bytes_delta, 0),
      total_files    = total_files    + COALESCE(_total_files_delta, 0),
      scanned_dirs   = scanned_dirs   + COALESCE(_scanned_dirs_delta, 0),
      migrated_files = migrated_files + COALESCE(_migrated_files_delta, 0),
      failed_files   = failed_files   + COALESCE(_failed_files_delta, 0),
      last_update    = now()
  WHERE job_id = _job_id;
END;
$$;
