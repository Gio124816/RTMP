require('dotenv').config();
const NodeMediaServer = require('node-media-server');
const { S3Client, CreateMultipartUploadCommand, UploadPartCommand,
        CompleteMultipartUploadCommand, AbortMultipartUploadCommand,
        DeleteObjectCommand } = require('@aws-sdk/client-s3');
const { spawn } = require('child_process');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');
const { WebSocketServer } = require('ws');
const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');

// ─── Config ──────────────────────────────────────────────────────────────────
const MAIN_API     = process.env.MAIN_API_URL || 'https://lunax-server-production.up.railway.app';
const RTMP_PORT    = parseInt(process.env.RTMP_PORT  || '1935');
const HTTP_PORT    = parseInt(process.env.HTTP_PORT  || '8080');
const S3_BUCKET    = process.env.AWS_S3_BUCKET || 'lunax-media';
const AWS_REGION   = process.env.AWS_REGION    || 'us-east-2';

// ─── Live clip-phrase detection ("Luna clip this") ───────────────────────────
// Watches the live stream's own audio in rolling windows and auto-marks a
// clip whenever a streamer says the trigger phrase — no phone needed at all.
const ASSEMBLYAI_API_KEY = process.env.ASSEMBLYAI_API_KEY;
const CLIP_WINDOW_SECONDS = 12; // length of each rolling audio window
// Known limitation: fixed, non-overlapping windows mean a trigger phrase
// spoken right at a window boundary could get split across two transcripts
// and missed by the substring match below. Acceptable for v1 — upgrade to
// overlapping windows later if streamers report missed clips in practice.
const CLIP_TRIGGER_PHRASES = ['luna clip this', 'clip this', 'luna clip that', 'clip that'];
const CLIP_BUFFER_BEFORE_MS = 45 * 1000; // how far back the clip starts from the detected moment
const CLIP_BUFFER_AFTER_MS  = 8 * 1000;  // small pad after, in case the moment continues briefly

const s3 = new S3Client({
  region: AWS_REGION,
  credentials: {
    accessKeyId:     process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
  requestChecksumCalculation: 'WHEN_REQUIRED',
  responseChecksumValidation: 'WHEN_REQUIRED',
});

// ─── Active stream state ──────────────────────────────────────────────────────
// streamKey → { userId, platforms, s3Key, uploadId, parts, partNumber,
//               clipMarkers, startedAt, ffmpegProcs, wsClients }
const activeStreams = new Map();

// ─── WebSocket server (for real-time status to the iOS app) ──────────────────
const httpServer = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, activeStreams: activeStreams.size }));
    return;
  }
  // Clip marker endpoint called from iOS companion app
  if (req.method === 'POST' && req.url === '/clip') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      try {
        const { streamKey, label } = JSON.parse(body);
        const stream = activeStreams.get(streamKey);
        if (!stream) { res.writeHead(404); res.end('{"error":"stream not found"}'); return; }
        const marker = {
          id: uuidv4(),
          timestamp: Date.now(),
          streamOffset: Date.now() - stream.startedAt, // ms from stream start
          label: label || 'Clip',
        };
        stream.clipMarkers.push(marker);
        console.log(`[Clip] Marked at ${Math.round(marker.streamOffset / 1000)}s for stream ${streamKey}`);
        // Broadcast to connected iOS clients
        broadcastToStream(streamKey, { type: 'clip_marked', marker });
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, marker }));
      } catch (e) {
        res.writeHead(400); res.end('{"error":"invalid body"}');
      }
    });
    return;
  }
  // Stream status endpoint
  if (req.method === 'GET' && req.url.startsWith('/status/')) {
    const streamKey = req.url.replace('/status/', '');
    const stream = activeStreams.get(streamKey);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(stream ? {
      live: true,
      duration: Math.round((Date.now() - stream.startedAt) / 1000),
      clipMarkers: stream.clipMarkers.length,
      pendingClipMarkers: stream.pendingClipMarkers,
      platforms: stream.platforms,
    } : { live: false }));
    return;
  }
  // Confirm an auto-detected clip — moves it from pending into the real,
  // confirmed clipMarkers list the main backend extracts clips from.
  if (req.method === 'POST' && req.url === '/clip/confirm') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      try {
        const { streamKey, markerId, label } = JSON.parse(body);
        const stream = activeStreams.get(streamKey);
        if (!stream) { res.writeHead(404); res.end('{"error":"stream not found"}'); return; }
        const idx = stream.pendingClipMarkers.findIndex(m => m.id === markerId);
        if (idx === -1) { res.writeHead(404); res.end('{"error":"pending marker not found"}'); return; }
        const [marker] = stream.pendingClipMarkers.splice(idx, 1);
        if (label) marker.label = label;
        marker.confirmed = true;
        stream.clipMarkers.push(marker);
        console.log(`[Clip] Auto-detected clip confirmed at ${Math.round(marker.streamOffset / 1000)}s for stream ${streamKey}`);
        broadcastToStream(streamKey, { type: 'clip_marked', marker });
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, marker }));
      } catch (e) {
        res.writeHead(400); res.end('{"error":"invalid body"}');
      }
    });
    return;
  }
  // Discard a false-positive auto-detected clip
  if (req.method === 'POST' && req.url === '/clip/discard') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      try {
        const { streamKey, markerId } = JSON.parse(body);
        const stream = activeStreams.get(streamKey);
        if (!stream) { res.writeHead(404); res.end('{"error":"stream not found"}'); return; }
        stream.pendingClipMarkers = stream.pendingClipMarkers.filter(m => m.id !== markerId);
        broadcastToStream(streamKey, { type: 'clip_discarded', markerId });
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true }));
      } catch (e) {
        res.writeHead(400); res.end('{"error":"invalid body"}');
      }
    });
    return;
  }
  res.writeHead(404);
  res.end('Not found');
});

const wss = new WebSocketServer({ server: httpServer });
wss.on('connection', (ws, req) => {
  const streamKey = new URL(req.url, 'http://localhost').searchParams.get('key');
  if (!streamKey) { ws.close(); return; }
  const stream = activeStreams.get(streamKey);
  if (!stream) { ws.close(); return; }
  stream.wsClients.add(ws);
  ws.on('close', () => stream?.wsClients?.delete(ws));
  // Send current status immediately
  ws.send(JSON.stringify({
    type: 'connected',
    duration: Math.round((Date.now() - stream.startedAt) / 1000),
    clipMarkers: stream.clipMarkers,
  }));
});

httpServer.listen(HTTP_PORT, () => {
  console.log(`✓ Luna X RTMP HTTP/WS server on port ${HTTP_PORT}`);
});

function broadcastToStream(streamKey, msg) {
  const stream = activeStreams.get(streamKey);
  if (!stream) return;
  const json = JSON.stringify(msg);
  for (const ws of stream.wsClients) {
    if (ws.readyState === 1) ws.send(json);
  }
}

// ─── Node Media Server config ─────────────────────────────────────────────────
const nmsConfig = {
  rtmp: {
    port: RTMP_PORT,
    chunk_size: 60000,
    gop_cache: true,
    ping: 30,
    ping_timeout: 60,
  },
  http: {
    port: 8888, // internal NMS HTTP — NOT the one above
    allow_origin: '*',
  },
  logType: 1, // minimal logging
};

const nms = new NodeMediaServer(nmsConfig);

// ─── Stream lifecycle hooks ───────────────────────────────────────────────────

// Called when a streamer connects and starts sending video
nms.on('prePublish', async (id, StreamPath, args) => {
  const streamKey = StreamPath.replace('/live/', '').split('/')[0];
  console.log(`[RTMP] prePublish — streamKey: ${streamKey}`);

  try {
    // 1. Validate stream key against main backend
    const resp = await axios.get(`${MAIN_API}/stream/validate/${streamKey}`, {
      timeout: 5000,
      headers: {
        'x-rtmp-secret': (process.env.RTMP_SHARED_SECRET || '').trim(),
      },
    });
    const { userId, platforms, userName } = resp.data;
    if (!userId) {
      console.warn(`[RTMP] Invalid stream key: ${streamKey}`);
      const session = nms.getSession(id);
      session?.reject();
      return;
    }

    // 2. Create S3 multipart upload for this stream recording
    const s3Key = `streams/${userId}/${uuidv4()}/raw.mp4`;
    const multipart = await s3.send(new CreateMultipartUploadCommand({
      Bucket: S3_BUCKET,
      Key: s3Key,
      ContentType: 'video/mp4',
      Metadata: {
        userId,
        streamKey,
        startedAt: Date.now().toString(),
      },
    }));

    // 3. Store state
    activeStreams.set(streamKey, {
      id,
      userId,
      userName,
      platforms: platforms || [],
      s3Key,
      uploadId: multipart.UploadId,
      parts: [],
      partNumber: 1,
      clipMarkers: [],
      // Auto-detected "Luna clip this" markers awaiting confirmation — kept
      // separate from clipMarkers (manual/Siri/app-triggered) since these
      // can be false positives (e.g. someone saying "let's clip this later"
      // conversationally) and shouldn't be treated as confirmed until a
      // human reviews them.
      pendingClipMarkers: [],
      startedAt: Date.now(),
      ffmpegProcs: new Map(), // platform → ffmpeg process
      wsClients: new Set(),
      totalBytesWritten: 0,
    });

    console.log(`[RTMP] Stream accepted for user ${userId} (${userName}) → ${s3Key}`);

    // 4. Notify main backend stream started
    await axios.post(`${MAIN_API}/stream/started`, {
      streamKey, userId, s3Key, startedAt: Date.now(),
    }, {
      headers: { 'x-rtmp-secret': (process.env.RTMP_SHARED_SECRET || '').trim() }
    }).catch(e => console.warn('[RTMP] Could not notify stream start:', e.message));

    // 5. Start restreaming to platforms
    startRestreaming(streamKey, StreamPath, platforms);

  } catch (e) {
    console.error(`[RTMP] prePublish error for ${streamKey}:`, e.message);
    // If validation fails, reject the stream
    const session = nms.getSession(id);
    session?.reject();
  }
});

// Called when stream data arrives — pipe to S3
nms.on('publish', (id, StreamPath, args) => {
  const streamKey = StreamPath.replace('/live/', '').split('/')[0];
  const stream = activeStreams.get(streamKey);
  if (!stream) return;

  // Start FFmpeg to record to a temp file for S3 multipart upload
  // FFmpeg reads from local RTMP, writes MP4 to stdout in chunks
  startS3Recording(streamKey, StreamPath);

  // Watch the live audio for "Luna clip this" — independent of recording,
  // so a hiccup in one never affects the other.
  stream.audioWatcherActive = true;
  runAudioWindow(streamKey, StreamPath);

  // Broadcast live started
  broadcastToStream(streamKey, {
    type: 'stream_started',
    startedAt: stream.startedAt,
  });
});

// Called when stream ends
nms.on('donePublish', async (id, StreamPath, args) => {
  const streamKey = StreamPath.replace('/live/', '').split('/')[0];
  const stream = activeStreams.get(streamKey);
  if (!stream) return;

  console.log(`[RTMP] Stream ended: ${streamKey} (${stream.clipMarkers.length} clips marked)`);

  // Stop the audio watcher loop — checked before each new window is
  // scheduled, so this just prevents the NEXT window from starting; any
  // in-flight transcription for the current window is left to finish and
  // clean up its own temp file rather than aborted mid-flight.
  stream.audioWatcherActive = false;

  try {
    // Stop all FFmpeg processes
    for (const [platform, proc] of stream.ffmpegProcs) {
      proc.kill('SIGTERM');
      console.log(`[RTMP] Stopped ffmpeg process: ${platform}`);
    }

    // Finalize S3 multipart upload
    if (stream.parts.length > 0) {
      await s3.send(new CompleteMultipartUploadCommand({
        Bucket: S3_BUCKET,
        Key: stream.s3Key,
        UploadId: stream.uploadId,
        MultipartUpload: {
          Parts: stream.parts.sort((a, b) => a.PartNumber - b.PartNumber),
        },
      }));
      console.log(`[RTMP] S3 upload complete: ${stream.s3Key} (${(stream.totalBytesWritten / 1e9).toFixed(2)}GB)`);
    } else {
      // Nothing was uploaded — abort
      await s3.send(new AbortMultipartUploadCommand({
        Bucket: S3_BUCKET, Key: stream.s3Key, UploadId: stream.uploadId,
      })).catch(() => {});
    }

    const duration = Math.round((Date.now() - stream.startedAt) / 1000);

    // Notify main backend — it will extract clips and run AI
    await axios.post(`${MAIN_API}/stream/ended`, {
      streamKey,
      userId: stream.userId,
      s3Key: stream.s3Key,
      duration,
      clipMarkers: stream.clipMarkers,
      startedAt: stream.startedAt,
      endedAt: Date.now(),
    }, {
      headers: { 'x-rtmp-secret': (process.env.RTMP_SHARED_SECRET || '').trim() }
    });

    console.log(`[RTMP] Notified backend: ${duration}s stream, ${stream.clipMarkers.length} markers`);

  } catch (e) {
    console.error(`[RTMP] donePublish error for ${streamKey}:`, e.message);
  } finally {
    broadcastToStream(streamKey, { type: 'stream_ended' });
    activeStreams.delete(streamKey);
  }
});

// ─── S3 Recording (FFmpeg → S3 multipart) ────────────────────────────────────
function startS3Recording(streamKey, rtmpPath) {
  const stream = activeStreams.get(streamKey);
  if (!stream) return;

  // FFmpeg reads from local RTMP relay, outputs MP4 fragments to stdout
  // We collect chunks and upload them to S3 as multipart parts
  const localRtmpUrl = `rtmp://127.0.0.1:${RTMP_PORT}${rtmpPath}`;

  const ffmpeg = spawn('ffmpeg', [
    '-i', localRtmpUrl,
    '-c:v', 'copy',       // no re-encode — just copy the stream as-is
    '-c:a', 'aac',        // normalize audio to AAC
    '-b:a', '128k',
    '-movflags', 'frag_keyframe+empty_moov+default_base_moof', // fragmented MP4 for streaming
    '-f', 'mp4',
    'pipe:1',             // output to stdout
  ], { stdio: ['ignore', 'pipe', 'pipe'] });

  // Collect chunks into a buffer; upload to S3 when we hit 5MB (S3 minimum part size)
  let buffer = Buffer.alloc(0);
  const MIN_PART_SIZE = 5 * 1024 * 1024; // 5MB

  ffmpeg.stdout.on('data', async (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);
    if (buffer.length >= MIN_PART_SIZE) {
      await uploadPart(streamKey, buffer);
      buffer = Buffer.alloc(0);
    }
  });

  ffmpeg.stdout.on('end', async () => {
    // Upload remaining buffer (can be smaller than MIN_PART_SIZE for the last part)
    if (buffer.length > 0) {
      await uploadPart(streamKey, buffer);
    }
  });

  ffmpeg.stderr.on('data', (data) => {
    // FFmpeg logs — only log errors not normal progress
    const msg = data.toString();
    if (msg.includes('Error') || msg.includes('error')) {
      console.error(`[FFmpeg/record] ${msg.trim()}`);
    }
  });

  ffmpeg.on('exit', (code) => {
    if (code !== 0 && code !== null) {
      console.warn(`[FFmpeg/record] exited with code ${code}`);
    }
  });

  stream.ffmpegProcs.set('__record__', ffmpeg);
}

async function uploadPart(streamKey, buffer) {
  const stream = activeStreams.get(streamKey);
  if (!stream) return;
  const partNumber = stream.partNumber++;
  try {
    const result = await s3.send(new UploadPartCommand({
      Bucket: S3_BUCKET,
      Key: stream.s3Key,
      UploadId: stream.uploadId,
      PartNumber: partNumber,
      Body: buffer,
    }));
    stream.parts.push({ PartNumber: partNumber, ETag: result.ETag });
    stream.totalBytesWritten += buffer.length;
    console.log(`[S3] Part ${partNumber} uploaded (${(buffer.length / 1e6).toFixed(1)}MB) total: ${(stream.totalBytesWritten / 1e9).toFixed(2)}GB`);
  } catch (e) {
    console.error(`[S3] Part ${partNumber} upload failed:`, e.message);
  }
}

// ─── Live clip-phrase detection ("Luna clip this") ───────────────────────────
// Captures the live stream's audio in short rolling windows and transcribes
// each one, watching for a trigger phrase. When found, marks a PENDING clip
// spanning backward from that moment (streamers say the phrase after the
// clip-worthy moment already happened) for the streamer to confirm/discard
// from their phone — never auto-saved outright, since live ASR misfires and
// people say things like "let's clip this later" conversationally.
function runAudioWindow(streamKey, rtmpPath) {
  const stream = activeStreams.get(streamKey);
  if (!stream || !stream.audioWatcherActive) return;

  const localRtmpUrl = `rtmp://127.0.0.1:${RTMP_PORT}${rtmpPath}`;
  const tmpFile = path.join(os.tmpdir(), `lunax-clipwatch-${streamKey}-${Date.now()}.wav`);

  // Mono 16kHz WAV — small file size, exactly what AssemblyAI wants, no need
  // to ship stereo/high-sample-rate audio just to detect a spoken phrase.
  const ffmpeg = spawn('ffmpeg', [
    '-i', localRtmpUrl,
    '-t', String(CLIP_WINDOW_SECONDS),
    '-vn',
    '-ac', '1',
    '-ar', '16000',
    '-f', 'wav',
    '-y', tmpFile,
  ], { stdio: ['ignore', 'ignore', 'pipe'] });

  ffmpeg.stderr.on('data', (data) => {
    const msg = data.toString();
    if (msg.includes('Error') || msg.toLowerCase().includes('error')) {
      console.error(`[ClipWatch/${streamKey}] ffmpeg: ${msg.trim().substring(0, 150)}`);
    }
  });

  ffmpeg.on('exit', (code) => {
    if (code === 0 && fs.existsSync(tmpFile)) {
      // Fire and forget — don't block the next window on transcription
      // finishing, or windows would fall further and further behind live.
      processAudioWindow(streamKey, tmpFile).catch(e => {
        console.error(`[ClipWatch/${streamKey}] processing error:`, e.message);
      });
    } else if (fs.existsSync(tmpFile)) {
      fs.unlink(tmpFile, () => {});
    }
    // Schedule the next window immediately, keeping pace with the live
    // stream, as long as the stream (and the watcher) are still active.
    runAudioWindow(streamKey, rtmpPath);
  });

  const stream2 = activeStreams.get(streamKey);
  if (stream2) stream2.ffmpegProcs.set('__clipwatch__', ffmpeg);
}

async function processAudioWindow(streamKey, wavFilePath) {
  const stream = activeStreams.get(streamKey);
  if (!stream) { fs.unlink(wavFilePath, () => {}); return; }

  // This window covers roughly [now - CLIP_WINDOW_SECONDS, now] of stream time.
  const windowEndOffset = Date.now() - stream.startedAt;

  try {
    if (!ASSEMBLYAI_API_KEY) {
      // Fail silently but loudly in logs — clip detection is a bonus
      // feature, it should never take down the actual stream/recording.
      console.warn('[ClipWatch] ASSEMBLYAI_API_KEY not set — live clip detection disabled');
      return;
    }

    const audioBytes = fs.readFileSync(wavFilePath);

    const uploadResp = await axios.post('https://api.assemblyai.com/v2/upload', audioBytes, {
      headers: {
        authorization: ASSEMBLYAI_API_KEY,
        'content-type': 'application/octet-stream',
      },
      maxBodyLength: Infinity,
    });
    const audioUrl = uploadResp.data.upload_url;

    const transcriptResp = await axios.post('https://api.assemblyai.com/v2/transcript', {
      audio_url: audioUrl,
    }, {
      headers: { authorization: ASSEMBLYAI_API_KEY },
    });
    const transcriptId = transcriptResp.data.id;

    // Poll until done — these short windows finish fast (usually a few
    // seconds), but cap attempts so a stuck job can't leak forever.
    let text = null;
    for (let attempt = 0; attempt < 20; attempt++) {
      await new Promise(r => setTimeout(r, 1500));
      const poll = await axios.get(`https://api.assemblyai.com/v2/transcript/${transcriptId}`, {
        headers: { authorization: ASSEMBLYAI_API_KEY },
      });
      if (poll.data.status === 'completed') { text = poll.data.text || ''; break; }
      if (poll.data.status === 'error') {
        console.warn(`[ClipWatch/${streamKey}] transcription error:`, poll.data.error);
        return;
      }
    }
    if (text === null) { console.warn(`[ClipWatch/${streamKey}] transcription timed out`); return; }

    const matchedPhrase = detectClipTrigger(text);
    if (matchedPhrase) {
      const marker = {
        id: uuidv4(),
        timestamp: Date.now(),
        streamOffset: Math.max(0, windowEndOffset - CLIP_BUFFER_BEFORE_MS),
        endOffset: windowEndOffset + CLIP_BUFFER_AFTER_MS,
        label: `Auto-detected: "${matchedPhrase}"`,
        transcript: text,
        source: 'auto',
      };
      const liveStream = activeStreams.get(streamKey);
      if (!liveStream) return;
      liveStream.pendingClipMarkers.push(marker);
      console.log(`[ClipWatch/${streamKey}] Detected "${matchedPhrase}" — pending clip at ${Math.round(marker.streamOffset / 1000)}s`);
      broadcastToStream(streamKey, { type: 'clip_detected', marker });
    }
  } catch (e) {
    console.error(`[ClipWatch/${streamKey}] error:`, e.response?.data || e.message);
  } finally {
    fs.unlink(wavFilePath, () => {});
  }
}

// Simple, forgiving substring match — live ASR punctuation/casing is
// inconsistent, so this normalizes both sides rather than expecting an
// exact match. Good enough for a v1; revisit if false negatives show up.
function detectClipTrigger(transcriptText) {
  const normalized = transcriptText.toLowerCase().replace(/[^a-z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim();
  for (const phrase of CLIP_TRIGGER_PHRASES) {
    if (normalized.includes(phrase)) return phrase;
  }
  return null;
}


// Platforms = [{ name, rtmpUrl, streamKey }]
function startRestreaming(streamKey, rtmpPath, platforms) {
  const stream = activeStreams.get(streamKey);
  if (!stream || !platforms?.length) return;

  const localRtmpUrl = `rtmp://127.0.0.1:${RTMP_PORT}${rtmpPath}`;

  for (const platform of platforms) {
    if (!platform.rtmpUrl || !platform.streamKey) continue;
    const destUrl = `${platform.rtmpUrl}/${platform.streamKey}`;

    const ffmpeg = spawn('ffmpeg', [
      '-i', localRtmpUrl,
      '-c:v', 'copy',
      '-c:a', 'aac',
      '-b:a', '128k',
      '-f', 'flv',
      destUrl,
    ], { stdio: ['ignore', 'ignore', 'pipe'] });

    ffmpeg.stderr.on('data', (data) => {
      const msg = data.toString();
      if (msg.includes('Connection refused') || msg.includes('Failed to')) {
        console.warn(`[Restream/${platform.name}] ${msg.trim().substring(0, 100)}`);
      }
    });

    ffmpeg.on('exit', (code) => {
      console.log(`[Restream/${platform.name}] ended (code ${code})`);
    });

    stream.ffmpegProcs.set(platform.name, ffmpeg);
    console.log(`[Restream] Started → ${platform.name}: ${destUrl.replace(platform.streamKey, '***')}`);
  }
}

// ─── Start ────────────────────────────────────────────────────────────────────
nms.run();
console.log(`✓ Luna X RTMP ingest server listening on port ${RTMP_PORT}`);

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('[RTMP] Shutting down...');
  // Abort any in-progress multipart uploads
  for (const [key, stream] of activeStreams) {
    for (const proc of stream.ffmpegProcs.values()) proc.kill('SIGTERM');
    await s3.send(new AbortMultipartUploadCommand({
      Bucket: S3_BUCKET, Key: stream.s3Key, UploadId: stream.uploadId,
    })).catch(() => {});
  }
  process.exit(0);
});
