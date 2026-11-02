CLOUDFLARE HTTP RELAY POOL - NEW-SESSION QUOTA RESERVE

This runs on Cloudflare Workers + one SQLite-backed Durable Object using the Workers Free plan. It is an HTTP-to-WebSocket adapter, not a Minecraft world server. Canva still uses ordinary HTTPS requests and binary streaming. There is no game simulation in the cloud.

WHY THIS OPTION
Cloudflare lists no data-transfer/egress/bandwidth charge for Workers. Its free plan is limited instead by requests and compute. Durable Objects are available on Free when SQLite-backed, as this configuration is.
This is NOT unlimited hosting. Current limits include 100,000 Worker requests/day, 100,000 Durable Object request units/day, and 13,000 GB-seconds of Durable Object duration/day. WebSocket traffic also counts toward Durable Object usage under Cloudflare's billing rules. Limits are shared with other apps on the account. On Free, exhausting a limit causes operations to fail until the daily reset (00:00 UTC). Stay on Workers Free if you want no paid usage.
For scale: 10 HTTP requests/second is 36,000 HTTP requests/hour, before other traffic. A game proxy can use the daily allowance much faster than a normal website. This option avoids a 5-GB monthly bandwidth pool but may not provide more uninterrupted playing time for every workload. Monitor actual usage before choosing it permanently.

SETUP (WINDOWS)
1. Use a free Cloudflare account. Install Node.js 22.20 or newer if needed.
2. Extract this folder. Open a terminal in it.
3. Run: npm install
4. Run: npx wrangler login
   Finish the Cloudflare sign-in page in your browser.
5. Check wrangler.jsonc. ALLOWED_TARGETS defaults to Carrot Craft. To allow another compatible server, add its exact ws:// or wss:// URL to the comma-separated list. Keep an explicit allowlist.
6. Run: npm run deploy
   This creates the Worker and SQLite-backed RelayHub binding. Until the secret is set, it returns a configuration error.
7. Run: npx wrangler secret put RELAY_KEY
   Paste the SAME relay key that is already in your Canva HTML's relay-key meta tag. Do not post the key in chat or put it in wrangler.jsonc.
8. Open the displayed https://math-practice-relay.YOUR-SUBDOMAIN.workers.dev/health URL. It should return ok:true, binaryProtocol:3, and version:2026-11-01.cloudflare-pool1.
9. Send the public workers.dev URL back so the Canva pool list can be configured. Or add that HTTPS URL to the cloudflare-relays JSON-array meta value in BOTH pool-enabled Canva HTML files. Keep relay-url pointing to Render and keep relay-key unchanged. All configured relays use that same key.
10. The /health response includes pool.acceptingNewSessions, httpRequestsObserved, newSessionCutoff and the UTC day. NEW_SESSION_CUTOFF defaults to 60000 and can be configured before deployment. A 503 quota-reserve response applies only to NEW sessions; existing routes continue.


Do not delete the Render relay yet. The supplied 26.3 Canva files still point to Render. To disable Cloudflare, set cloudflare-relays to [] in both pool-enabled HTMLs. Leave Render as relay-url. No new Supabase SQL is needed.
The pool-enabled wrapper always routes LAN/shared-world signaling and data through the common Render backend. Public-server sessions can use Cloudflare. Established sessions stay pinned and cannot migrate.

LOCAL TESTING / WHAT PASSED
- Real Cloudflare workerd runtime via Miniflare, SQLite Durable Object binding.
- Real Chrome with the existing browser binary transport.
- Fragmented binary receive stream; long-poll fallback.
- Unicode and empty packets; typed-array snapshots; 1-MiB binary integrity.
- Simulated lost acknowledgement after the upstream accepted a batch: no duplicate packets.
- Legacy protocol-2 JSON/base64 compatibility.
- Two isolated Chrome contexts: shared-world host code, guest signaling, bidirectional HTTP-only data link, 100 ordered binary packets, final join-success/close handling, host-code cleanup.
- Wrangler dry-run bundle and binding validation.
Not deployed to a Cloudflare account. Public Carrot Craft login and actual Canva behavior through Cloudflare are not yet verified. A public server may reject a Cloudflare connection independently of this relay's correctness.

IMPLEMENTATION LIMITS
One named Durable Object keeps all relay sessions together. Its SQLite binding enables Free-plan deployment, but live sockets and packet queues stay in memory. Deployments, evictions and restarts disconnect sessions; reconnect in game. No packet logs or world saves are stored in SQLite. Only the observed HTTP-request counter is stored there, in one row per UTC day.
There are at most 12 transport sessions, an 8-MiB receive queue per session, a 24-MiB combined receive-queue cap, and a 4-MiB single encoded-event cap. Workers does not expose Node ws.pause/resume; a browser that cannot keep up is disconnected at the memory cap instead. Request uploads retain the 2-MiB cap. Sessions idle for 60 seconds are closed. No permanent idle keepalive or cron job is required.
The Worker hosts only proxy endpoints, not the Minecraft HTML or Math Practice pages. Keep the game host on Vercel and the wrapper in Canva.

SOURCES (CHECKED NOVEMBER 1, 2026)
https://developers.cloudflare.com/workers/platform/pricing/
https://developers.cloudflare.com/durable-objects/platform/pricing/
https://developers.cloudflare.com/durable-objects/platform/limits/
https://developers.cloudflare.com/workers/runtime-apis/websockets/
https://developers.cloudflare.com/durable-objects/examples/readable-stream/

ADMISSION COUNTER LIMITS
The counter covers requests forwarded to this Durable Object, including health and configured CORS preflights. It is not Cloudflare account-wide usage telemetry. Unauthorized requests rejected at the edge, other Workers on the account, and WebSocket-message request units are not included. The 60,000 cutoff is a precaution with a reserve, not a guarantee that existing players can never hit Cloudflare's hard limit. Hard-cap exhaustion can still disconnect games. New sessions return 503 with retryAfter until the next UTC day; the browser then uses Render and caches that cooldown.
The SQLite counter survives runtime restarts. A configuration deployment disconnects games, so set NEW_SESSION_CUTOFF before playing. Upstream WebSocket startup is completed before a new-server session is accepted, letting the browser fall back if Cloudflare cannot reach the public server.

POOL VALIDATION
Local Cloudflare runtime + real Chrome verified: first session selects Cloudflare; existing session remains usable after the admission cutoff; the next session selects Render; LAN signaling selects Render; request count survives runtime restart. Updated binary integrity/retry tests passed as well.
