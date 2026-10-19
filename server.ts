import express, { Request, Response } from 'express';
import { createServer as createViteServer } from 'vite';
import path from 'path';
import { fileURLToPath } from 'url';
import zlib from 'zlib';
import { Readable } from 'stream';
import parser from 'iptv-playlist-parser';
import { XMLParser } from 'fast-xml-parser';

process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function startServer() {
  const app = express();
  const PORT = Number(process.env.PORT) || 3000;
  const isProd = process.env.NODE_ENV === 'production';

  app.use(express.json({ limit: '50mb' }));
  app.use(express.urlencoded({ extended: true, limit: '50mb' }));

  app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS, HEAD');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Range, Authorization, User-Agent');
    res.header('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Content-Type, Accept-Ranges');
    if (req.method === 'OPTIONS') {
      res.sendStatus(200);
      return;
    }
    next();
  });

  app.get('/api/health', (_req: Request, res: Response) => {
    res.json({ status: 'ok', name: 'IPTVnator Web Server', time: new Date().toISOString() });
  });

  // Stream proxy. This route deliberately does not share any EPG state or
  // buffers, so guide loading cannot close or replace an active stream.
  app.get('/api/proxy', async (req: Request, res: Response) => {
    const targetUrl = req.query.url as string;
    const customUserAgent =
      (req.query.userAgent as string) ||
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';
    const customReferer = req.query.referer as string;

    if (!targetUrl) {
      res.status(400).send('Missing target url query parameter');
      return;
    }

    try {
      const parsedUrl = new URL(targetUrl);
      if (parsedUrl.protocol !== 'http:' && parsedUrl.protocol !== 'https:') {
        res.status(400).send('Only HTTP and HTTPS stream URLs are supported');
        return;
      }

      const abortController = new AbortController();
      res.on('close', () => {
        if (!res.writableEnded) abortController.abort();
      });

      const headers: Record<string, string> = {
        'User-Agent': customUserAgent,
        'Accept': '*/*',
        'Connection': 'keep-alive',
      };
      if (customReferer) headers['Referer'] = customReferer;
      if (req.headers.range) headers['Range'] = req.headers.range;

      const response = await fetch(targetUrl, {
        method: 'GET',
        headers,
        redirect: 'follow',
        signal: abortController.signal,
      });

      if (!response.ok && response.status !== 206) {
        res.status(response.status).send(`Upstream server returned error: ${response.status} ${response.statusText}`);
        return;
      }

      const contentType = response.headers.get('content-type') || '';
      const finalUrl = response.url || targetUrl;
      const urlLower = targetUrl.toLowerCase();
      const finalUrlLower = finalUrl.toLowerCase();
      const isSegment =
        urlLower.includes('.ts') || finalUrlLower.includes('.ts') ||
        urlLower.includes('.m4s') || finalUrlLower.includes('.m4s') ||
        urlLower.includes('.mp4') || finalUrlLower.includes('.mp4');
      const isM3u8 =
        !isSegment &&
        (urlLower.includes('.m3u8') || finalUrlLower.includes('.m3u8') ||
          contentType.includes('application/vnd.apple.mpegurl') ||
          contentType.includes('application/x-mpegurl') ||
          contentType.includes('audio/x-mpegurl'));

      if (isM3u8) {
        const text = await response.text();
        if (text.trim().startsWith('#EXTM3U')) {
          const proxyParams = `userAgent=${encodeURIComponent(customUserAgent)}${customReferer ? `&referer=${encodeURIComponent(customReferer)}` : ''}`;
          const rewrittenLines = text.split('\n').map((line) => {
            const trimmed = line.trim();
            if (!trimmed || trimmed.startsWith('#')) {
              if (trimmed.startsWith('#EXT-X-KEY:') || trimmed.startsWith('#EXT-X-MAP:') || trimmed.startsWith('#EXT-X-MEDIA:')) {
                return line.replace(/URI="([^"]+)"/, (_match, uri) => {
                  try {
                    const resolvedUri = new URL(uri, finalUrl).toString();
                    return `URI="/api/proxy?url=${encodeURIComponent(resolvedUri)}&${proxyParams}"`;
                  } catch {
                    return line;
                  }
                });
              }
              return line;
            }
            try {
              const fullUri = new URL(trimmed, finalUrl).toString();
              return `/api/proxy?url=${encodeURIComponent(fullUri)}&${proxyParams}`;
            } catch {
              return line;
            }
          });
          res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
          res.setHeader('Access-Control-Allow-Origin', '*');
          res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
          res.send(rewrittenLines.join('\n'));
          return;
        }
      }

      if (contentType) res.setHeader('Content-Type', contentType);
      const contentLength = response.headers.get('content-length');
      if (contentLength) res.setHeader('Content-Length', contentLength);
      const contentRange = response.headers.get('content-range');
      if (contentRange) res.setHeader('Content-Range', contentRange);
      const acceptRanges = response.headers.get('accept-ranges');
      if (acceptRanges) res.setHeader('Accept-Ranges', acceptRanges);
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Cache-Control', isSegment ? 'public, max-age=60, immutable' : 'no-store, no-cache, must-revalidate, proxy-revalidate');
      if (!isSegment) {
        res.setHeader('Pragma', 'no-cache');
        res.setHeader('Expires', '0');
      }
      res.status(response.status);

      if (response.body) {
        const stream = Readable.fromWeb(response.body as any);
        stream.on('error', (err) => console.warn('Stream pipe warning:', err?.message));
        stream.pipe(res);
      } else {
        res.end();
      }
    } catch (err: any) {
      if (err?.name === 'AbortError') return;
      console.error('Proxy request failed:', err?.message || err);
      if (!res.headersSent && !res.destroyed) {
        res.status(502).json({ error: 'Proxy request failed', details: err?.message });
      }
    }
  });

  app.post('/api/parse-playlist', async (req: Request, res: Response) => {
    try {
      const { url, content, userAgent } = req.body;
      let rawM3u = content;
      if (url && !content) {
        const response = await fetch(url, {
          headers: {
            'User-Agent': userAgent || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept-Encoding': 'gzip, deflate, br',
          },
          redirect: 'follow',
          signal: AbortSignal.timeout(20000),
        });
        if (!response.ok) {
          res.status(response.status).json({ error: `Failed to fetch playlist: ${response.statusText}` });
          return;
        }
        const buffer = Buffer.from(await response.arrayBuffer());
        rawM3u = buffer.length > 2 && buffer[0] === 0x1f && buffer[1] === 0x8b
          ? zlib.gunzipSync(buffer).toString('utf-8')
          : buffer.toString('utf-8');
      }
      if (!rawM3u || typeof rawM3u !== 'string') {
        res.status(400).json({ error: 'No playlist content or URL provided' });
        return;
      }

      let header: any = {};
      let items: any[] = [];
      try {
        const parsed = parser.parse(rawM3u);
        header = parsed.header;
        items = parsed.items;
      } catch (parserErr) {
        console.warn('iptv-playlist-parser threw error, using fallback regex parser:', parserErr);
        const lines = rawM3u.split(/\r?\n/);
        let curr: any = null;
        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed) continue;
          if (trimmed.startsWith('#EXTINF:')) {
            const comma = trimmed.indexOf(',');
            const info = comma !== -1 ? trimmed.substring(0, comma) : trimmed;
            const name = comma !== -1 ? trimmed.substring(comma + 1).trim() : 'Channel';
            curr = {
              name,
              tvg: {
                id: info.match(/tvg-id="([^"]*)"/i)?.[1] || '',
                name: info.match(/tvg-name="([^"]*)"/i)?.[1] || '',
                logo: info.match(/tvg-logo="([^"]*)"/i)?.[1] || '',
              },
              group: { title: info.match(/group-title="([^"]*)"/i)?.[1] || 'General' },
              radio: /radio="(true|1)"/i.test(info),
            };
          } else if (!trimmed.startsWith('#') && curr && trimmed.length > 5) {
            curr.url = trimmed;
            items.push(curr);
            curr = null;
          }
        }
      }

      const streamHeaders = new Map<string, { httpUserAgent?: string; httpReferrer?: string }>();
      let pendingHeaders: { httpUserAgent?: string; httpReferrer?: string } = {};
      for (const rawLine of rawM3u.split(/\r?\n/)) {
        const line = rawLine.trim();
        if (!line) continue;
        if (line.startsWith('#EXTINF:')) {
          pendingHeaders = {};
          const uaAttr = line.match(/(?:http-user-agent|user-agent)\s*=\s*["']([^"']+)["']/i);
          const refAttr = line.match(/(?:http-referrer|http-referer|referer|referrer)\s*=\s*["']([^"']+)["']/i);
          if (uaAttr) pendingHeaders.httpUserAgent = uaAttr[1].trim();
          if (refAttr) pendingHeaders.httpReferrer = refAttr[1].trim();
        } else if (line.startsWith('#EXTVLCOPT:')) {
          const ua = line.match(/http-user-agent\s*=\s*(.+)$/i);
          const ref = line.match(/http-referrer\s*=\s*(.+)$/i);
          if (ua) pendingHeaders.httpUserAgent = ua[1].trim();
          if (ref) pendingHeaders.httpReferrer = ref[1].trim();
        } else if (!line.startsWith('#') && /^https?:\/\//i.test(line)) {
          if (pendingHeaders.httpUserAgent || pendingHeaders.httpReferrer) streamHeaders.set(line, { ...pendingHeaders });
          pendingHeaders = {};
        }
      }
      for (const item of items) {
        const headers = streamHeaders.get(String(item.url || '').trim());
        if (headers) {
          if (headers.httpUserAgent) item.httpUserAgent = headers.httpUserAgent;
          if (headers.httpReferrer) item.httpReferrer = headers.httpReferrer;
        }
      }
      const m3uHeaderLine = rawM3u.split(/\r?\n/, 1)[0] || '';
      const tvgUrlMatch = m3uHeaderLine.match(/(?:url-tvg|x-tvg-url|tvg-url)\s*=\s*["']([^"']+)["']/i);
      if (tvgUrlMatch) header = { ...header, urlTvg: tvgUrlMatch[1] };
      res.json({ header, items, count: items.length });
    } catch (err: any) {
      console.error('Playlist parse error:', err);
      res.status(500).json({ error: 'Failed to parse playlist', details: err?.message });
    }
  });

  // EPG is intentionally bounded. Large XMLTV bundles were being parsed into
  // the same Node process that serves HLS, causing memory spikes and event-loop
  // stalls. Keep only the window the UI actually displays.
  app.post('/api/parse-epg', async (req: Request, res: Response) => {
    try {
      const { url, content } = req.body;
      if (!url && !content) {
        res.status(400).json({ error: 'No EPG URL or XML data provided' });
        return;
      }

      const xmlParser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '@_' });
      const readXmlText = (value: any): string => {
        if (typeof value === 'string') return value;
        if (Array.isArray(value)) return value.map(readXmlText).filter(Boolean).join(' ');
        return typeof value?.['#text'] === 'string' ? value['#text'] : '';
      };

      const urls: string[] = typeof url === 'string'
        ? url.split(/,\s*(?=https?:\/\/)/i).map((item: string) => item.trim()).filter(Boolean)
        : [];
      const uniqueUrls = Array.from(new Set(urls));
      // Loading eight guides concurrently was the main memory spike. Two is
      // enough for the normal bundled playlist while still allowing a fallback.
      const sources = content
        ? [{ url: 'inline XMLTV', content: String(content) }]
        : uniqueUrls.slice(0, 2).map((sourceUrl) => ({ url: sourceUrl }));
      if (sources.length === 0) {
        res.status(400).json({ error: 'No valid EPG URLs were found' });
        return;
      }

      const channelsById = new Map<string, any>();
      const programmeByKey = new Map<string, any>();
      const failedSources: string[] = [];
      let loadedSources = 0;
      const now = Date.now();
      const windowStart = now - 2 * 60 * 60 * 1000;
      const windowEnd = now + 12 * 60 * 60 * 1000;
      const MAX_PROGRAMMES = 30000;
      const MAX_XML_BYTES = 60 * 1024 * 1024;

      const parseSource = async (source: { url: string; content?: string }) => {
        try {
          let rawXml = source.content;
          if (!rawXml) {
            const response = await fetch(source.url, {
              headers: { 'User-Agent': 'Ma9icTV/1.0 XMLTV', 'Accept-Encoding': 'gzip, deflate' },
              redirect: 'follow',
              signal: AbortSignal.timeout(20000),
            });
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const buffer = Buffer.from(await response.arrayBuffer());
            if (buffer.length > MAX_XML_BYTES) throw new Error(`EPG source is too large (${Math.round(buffer.length / 1024 / 1024)} MB); skipped to protect the live stream`);
            rawXml = buffer.length > 2 && buffer[0] === 0x1f && buffer[1] === 0x8b
              ? zlib.gunzipSync(buffer).toString('utf-8')
              : buffer.toString('utf-8');
          }
          if (!rawXml || Buffer.byteLength(rawXml, 'utf8') > MAX_XML_BYTES) {
            throw new Error('EPG XML is too large; skipped to protect the live stream');
          }

          // Yield before the CPU-heavy XML parse so pending HLS work can run.
          await new Promise<void>((resolve) => setImmediate(resolve));
          const parsedData = xmlParser.parse(rawXml);
          const tvData = parsedData.tv || {};
          const rawChannels = Array.isArray(tvData.channel) ? tvData.channel : (tvData.channel ? [tvData.channel] : []);
          const rawProgrammes = Array.isArray(tvData.programme) ? tvData.programme : (tvData.programme ? [tvData.programme] : []);

          for (const channel of rawChannels) {
            const id = String(channel['@_id'] || '').trim();
            if (!id) continue;
            const displayNames = Array.isArray(channel['display-name']) ? channel['display-name'] : [channel['display-name']];
            const names = displayNames.map(readXmlText).filter(Boolean);
            const existing = channelsById.get(id);
            channelsById.set(id, {
              id,
              name: names[0] || id,
              names: Array.from(new Set([...(existing?.names || []), ...names])),
              icon: channel.icon?.['@_src'] || existing?.icon || '',
            });
          }

          for (const programme of rawProgrammes) {
            if (programmeByKey.size >= MAX_PROGRAMMES) break;
            const channel = String(programme['@_channel'] || '').trim();
            const start = String(programme['@_start'] || '').trim();
            const stop = String(programme['@_stop'] || '').trim();
            const title = readXmlText(programme.title).trim();
            if (!channel || !start || !stop || !title) continue;
            const startMs = Date.parse(start.replace(/^(\d{8})(\d{6})(?:\s*([+-]\d{4}))?$/, '$1T$2$3'));
            const stopMs = Date.parse(stop.replace(/^(\d{8})(\d{6})(?:\s*([+-]\d{4}))?$/, '$1T$2$3'));
            // XMLTV timestamps are not guaranteed to be ISO, so retain records
            // when Date.parse cannot understand them; the client validates them.
            if (Number.isFinite(startMs) && Number.isFinite(stopMs) && (stopMs < windowStart || startMs > windowEnd)) continue;
            const key = `${channel}|${start}|${stop}|${title}`;
            if (!programmeByKey.has(key)) {
              programmeByKey.set(key, {
                channel,
                start,
                stop,
                title,
                desc: readXmlText(programme.desc),
                category: readXmlText(programme.category),
              });
            }
          }
          loadedSources += 1;
          // Drop the giant parsed object before processing the next source.
          // eslint-disable-next-line @typescript-eslint/no-unused-vars
          void parsedData;
        } catch (error: any) {
          failedSources.push(`${source.url}: ${error?.message || 'fetch/parse failed'}`);
        }
      };

      // Parse one guide at a time. This is intentionally conservative: the
      // stream must remain usable even when a provider publishes a huge EPG.
      for (const source of sources) {
        await parseSource(source);
        await new Promise<void>((resolve) => setImmediate(resolve));
      }

      const channels = Array.from(channelsById.values());
      const programmes = Array.from(programmeByKey.values());
      if (loadedSources === 0 || programmes.length === 0) {
        res.status(502).json({
          error: 'No valid XMLTV programme data could be loaded',
          sourceCount: sources.length,
          failedSourceCount: failedSources.length,
          failedSources: failedSources.slice(0, 10),
        });
        return;
      }

      res.json({
        channels,
        programmes,
        channelCount: channels.length,
        programmeCount: programmes.length,
        sourceCount: sources.length,
        loadedSourceCount: loadedSources,
        failedSourceCount: failedSources.length,
        failedSources: failedSources.slice(0, 10),
      });
    } catch (err: any) {
      console.error('EPG parse error:', err);
      if (!res.headersSent) res.status(500).json({ error: 'Failed to parse XMLTV EPG', details: err?.message });
    }
  });

  app.post('/api/xtream', async (req: Request, res: Response) => {
    try {
      const { serverUrl, username, password, action, params } = req.body;
      if (!serverUrl || !username || !password) {
        res.status(400).json({ error: 'Missing Xtream serverUrl, username, or password' });
        return;
      }
      const cleanServerUrl = serverUrl.replace(/\/+$/, '');
      let targetUrl = `${cleanServerUrl}/player_api.php?username=${encodeURIComponent(username)}&password=${encodeURIComponent(password)}`;
      if (action) targetUrl += `&action=${encodeURIComponent(action)}`;
      if (params && typeof params === 'object') {
        for (const [k, v] of Object.entries(params)) {
          if (v !== undefined && v !== null) targetUrl += `&${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`;
        }
      }
      const response = await fetch(targetUrl, { headers: { 'User-Agent': 'IPTVnator/0.25.0', 'Accept': 'application/json' }, redirect: 'follow' });
      if (!response.ok) {
        res.status(response.status).json({ error: `Xtream API returned ${response.statusText}` });
        return;
      }
      res.json(await response.json());
    } catch (err: any) {
      console.error('Xtream proxy error:', err);
      res.status(500).json({ error: 'Xtream request failed', details: err?.message });
    }
  });

  if (!isProd) {
    const vite = await createViteServer({ server: { middlewareMode: true, hmr: process.env.DISABLE_HMR !== 'true' }, appType: 'spa' });
    app.use(vite.middlewares);
  } else {
    const distPath = path.resolve(__dirname, 'dist');
    app.use(express.static(distPath));
    app.get('*', (_req: Request, res: Response) => res.sendFile(path.resolve(distPath, 'index.html')));
  }

  app.listen(PORT, '0.0.0.0', () => console.log(`[IPTVnator] Server listening on http://0.0.0.0:${PORT}`));
}

startServer().catch((err) => {
  console.error('[IPTVnator] Failed to start server:', err);
  process.exit(1);
});
