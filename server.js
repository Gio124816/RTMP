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

// ─── Config ──────────────────────────────────────────────────────────────────
const MAIN_API     = process.env.MAIN_API_URL || 'https://lunax-server-production.up.railway.app';
const RTMP_PORT    = parseInt(process.env.RTMP_PORT  || '1935');
const HTTP_PORT    = parseInt(process.env.HTTP_PORT  || '8080');
const S3_BUCKET    = process.env.AWS_S3_BUCKET || 'lunax-media';
const AWS_REGION   = process.env.AWS_REGION    || 'us-east-2';

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
      platforms: stream.platforms,
    } : { live: false }));
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
      startedAt: Date.now(),
      ffmpegProcs: new Map(), // platform → ffmpeg process
      wsClients: new Set(),
      totalBytesWritten: 0,
    });

    console.log(`[RTMP] Stream accepted for user ${userId} (${userName}) → ${s3Key}`);

    // 4. Notify main backend stream started
    await axios.post(`${MAIN_API}/stream/started`, {
      streamKey, userId, s3Key, startedAt: Date.now(),
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

  try {
    // Stop all FFmpeg processes
    for (const [platform, proc] of stream.ffmpegProcs) {
      proc.kill('SIGTERM');
      console.log(`[RTMP] Stopped restream to ${platform}`);
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

// ─── Restreaming to platforms ─────────────────────────────────────────────────
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
