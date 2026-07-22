// ════════════════════════════════════════════════════════════════════════
// POST /clip/manual — add to the lunax-rtmp service (server.js or wherever
// /clip/confirm and /clip/discard already live) — this is a DIFFERENT
// Railway service from lunax-server, per your project setup.
// ════════════════════════════════════════════════════════════════════════
//
// Client-side voice trigger (ClipReviewView.swift's "say clip this into
// your phone" toggle, separate from the existing server-side stream-audio
// detection) — creates the same shape of marker the audio-based detection
// creates, broadcasting it over the same clip_detected WebSocket event, so
// it shows up in the app's pendingClips list identically either way — no
// client-side special-casing needed for "this one came from the phone mic."
//
// ASSUMPTIONS I'm making about your existing code, since I don't have the
// actual server.js source for this service — adjust names to match:
//   - `activeStreams` (or similar): an in-memory/store lookup keyed by
//     streamKey, holding at least a `durationMs`/similar and however you
//     track `clipMarkers` for that stream (used by /clip/confirm's own
//     bookkeeping already).
//   - `broadcastToStream(streamKey, payload)`: however you currently push
//     `clip_detected`/`clip_marked`/`clip_discarded` events out over the
//     WebSocket for a given stream — reuse whatever that function is
//     already called in your /clip/confirm or the audio-detection code path.
//   - `uuidv4` already imported (it's used elsewhere in this project's
//     backend already, e.g. auth.js).
//
// If your actual variable/function names differ, this is a straightforward
// find-and-replace against whatever /clip/confirm already uses for the
// same two things.

app.post('/clip/manual', (req, res) => {
  try {
    const { streamKey, offsetMs } = req.body;
    if (!streamKey) return res.status(400).json({ error: 'streamKey required' });

    const stream = activeStreams[streamKey];
    if (!stream) return res.status(404).json({ error: 'No active stream for this key' });

    const endOffset = typeof offsetMs === 'number' ? offsetMs : (stream.durationMs || 0);
    // Default pre-roll: capture the ~20s leading up to the moment "clip
    // this" was actually said, plus a small buffer after — people usually
    // say the phrase slightly after the moment they actually want kept,
    // same assumption the audio-based detection presumably already makes.
    const startOffset = Math.max(0, endOffset - 20000);

    const marker = {
      id: uuidv4(),
      streamOffset: startOffset,
      endOffset: endOffset + 5000,
      label: 'Manually clipped from phone',
      transcript: '',
    };

    stream.clipMarkers = stream.clipMarkers || [];
    stream.clipMarkers.push(marker);

    broadcastToStream(streamKey, { type: 'clip_detected', marker });

    res.json({ ok: true, marker });
  } catch (err) {
    console.error('Manual clip error:', err);
    res.status(500).json({ error: 'Failed to create manual clip' });
  }
});
