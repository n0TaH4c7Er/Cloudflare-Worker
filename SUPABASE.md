SUPABASE SAVE RELAY

This service already exposes /save-proxy for this project's Supabase auth and private
Storage requests. Configure RELAY_KEY to match the client's relay key. SUPABASE_URL
and SUPABASE_PUBLISHABLE_KEY can override the supplied project defaults. Never use a
Supabase service-role/secret key here. The caller supplies its own user bearer token;
Supabase validates it and enforces storage ownership policies.

Canva client priority: computer HTTP relay, configured Cloudflare workers, Render.
Existing multiplayer sessions stay pinned to the backend that accepted them. LAN
signaling and data stay on a single helper; individual live sessions do not migrate.
Only the computer helper runs Singleplayer simulation and owns active world files.

Cloudflare and Render can forward Supabase save requests, but they do not silently
become the computer's world simulator or gain access to its private disk. Restoring
the helper-owned player identity after a fresh Canva upload requires that helper.

/health advertises saveProxy:true, saveProxyProtocol:1 and singleplayerSimulation:false.
Browser save chunks are limited to 5 MiB (auth requests 64 KiB), with up to four active
save-proxy operations. Provider account limits still apply. A timeout during signup
or refresh must not be blindly retried on another relay because it may have already
created an identity or rotated its refresh token.
