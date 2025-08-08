const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const { SUPABASE_URL, SUPABASE_KEY } = require('../config');

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, { auth: { persistSession: false } });
const http = axios.create({ timeout: 120000, maxContentLength: Infinity, maxBodyLength: Infinity });

module.exports = { supabase, http };
