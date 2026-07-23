# P3.4 — Live Saturday Dashboard: Go/No-Go Spike

**Date:** 2026-07-21
**Branch:** `claude/p34-realtime-spike`
**Status:** spike complete — no production code in this branch
**Scope:** Research only. Answers: build a live Saturday dashboard on CFBD's Tier-3 GraphQL/Hasura subscriptions, poll REST instead, or defer?

---

## Context

The user is CFBD Patreon Tier 3 ($10/mo, 75,000 REST calls/month). The warehouse's daily `cfb-database` loads consume ~22K calls/month worst case, leaving ~53K/month of headroom. The frontend (`cfb-app`) is Next.js 16 App Router, all server components, deployed to Vercel, reading historical data from Supabase — it has **no existing CFBD client**; every CFBD call would be new. The CFBD API key must never reach the browser. Today is 2026-07-21; the season opens late August, roughly five weeks out.

The ask: should a live Saturday dashboard (score ticker + live win probability) be built now on CFBD's Tier-3 GraphQL/Hasura subscriptions, on REST polling, or deferred?

## Findings

All sources below were fetched directly (collegefootballdata.com, blog.collegefootballdata.com → redirects to radsportsanalytics.com/blog, graphqldocs.collegefootballdata.com, api.collegefootballdata.com's OpenAPI spec, Vercel docs). **No proxy blocks were encountered** — every host resolved and returned real content, so the fallback "user-runnable probe" mode wasn't strictly needed, but one is included below for the single question public docs don't answer (quota sharing).

### 1. Tier 3 still includes GraphQL, as of July 2026 (verified)

`collegefootballdata.com/api-tiers` (fetched live) gives the current tier ladder:

| Tier | Price | Calls/mo | New feature vs. prior tier |
|---|---|---|---|
| Free / Academic | $0 | 1,000 / 3,000 | Base endpoints |
| Tier 1 | $1 | 5,000 | Opponent-adjusted metrics, weather, **Live Scoreboard** |
| Tier 2 | $5 | 30,000 | **Live Play-by-Play** |
| **Tier 3** | **$10** | **75,000** | **GraphQL API** (subscriptions) |
| Tier 4–6 | $15–$30 | 125K–500K | Same features as Tier 3, just more calls |

GraphQL access is still the Tier-3 headline feature, unchanged from its original positioning. The user's $10/mo tier is the correct (minimum) tier for GraphQL. Source: [API Access Tiers](https://collegefootballdata.com/api-tiers).

**Important nuance the question didn't anticipate:** Live Scoreboard (Tier 1) and Live Play-by-Play (Tier 2) are *not* GraphQL-gated — they're plain REST features available two tiers below where GraphQL unlocks. The user's Tier 3 subscription includes all of it.

### 2. GraphQL subscriptions are real, but schema coverage for live plays is the known gap (verified)

- Endpoint: `https://graphql.collegefootballdata.com/v1/graphql` (queries), `wss://graphql.collegefootballdata.com/v1/graphql` (subscriptions) — Hasura-powered, confirmed via [graphqldocs.collegefootballdata.com](https://graphqldocs.collegefootballdata.com/) and the CFBD blog.
- Auth: same REST API key, passed as `Authorization: Bearer <key>` in the websocket connection headers — same mechanism, no separate credential. Source: [Subscribing to Data Events with the CFBD GraphQL API](https://radsportsanalytics.com/blog/subscribing-to-data-events-with-the-cfbd-graphql-api/).
- Mechanism: any query becomes a subscription by renaming the operation; Hasura re-pushes the result set whenever the underlying rows change (event-based, not client-polled). The CFBD-authored example explicitly recommends exponential-backoff reconnect logic because "WebSocket connections can be very brittle."
- Root query fields present today (from the live GraphQL docs site): `game`, `gameAggregate`, `gameLines`, `gamePlayerStat`, `gameMedia`, `gameWeather`, `gameTeam`, `scoreboard`, `adjustedTeamMetrics`, `adjustedPlayerMetrics`, `predictedPoints`, `ratings`, `recruit`, `pollRank`, and more.
- **Gap:** the CFBD-authored intro post ("Building Dynamic Queries…") states plainly that as of that writing, "drive and play data is **not** currently included but will be added over time." The current schema dump shows no dedicated live play-by-play or drive query/subscription field, and no in-game win-probability-per-play field distinct from the `scoreboard` type's own field (see #3). Whether this has been backfilled since the original post isn't independently datable from the docs site; treat "full play-by-play over GraphQL" as unverified/likely still absent.

### 3. REST `/scoreboard` already contains everything a live dashboard needs — the load-bearing finding (verified)

Pulled the live OpenAPI spec (`api.collegefootballdata.com`) for `GET /scoreboard`. It requires only `week` (season/team/conference/groups optional — so **one call returns every live game for a week**, not one call per game). The response schema (`ScoreboardGame`) includes, per game:

- `status`, `period`, `clock`, `possession`, `situation`, `lastPlay`
- `homeTeam.points`, `awayTeam.points`, `homeTeam.lineScores`, `awayTeam.lineScores`
- **`homeTeam.winProbability`, `awayTeam.winProbability`** — live in-game win probability, already on this endpoint
- `betting.homeMoneyline`, `betting.awayMoneyline`, `betting.spread`, `betting.overUnder`
- `weather.*`, `venue.*`

This single, already-available, Tier-1-gated REST endpoint covers both stated requirements (score ticker + live WP) with **zero GraphQL, zero websockets, and one call per poll regardless of how many games are live.** This materially changes the shape of the decision: the GraphQL/subscriptions investment would be buying infrastructure complexity for data the REST API already serves for free polling.

### 4. GraphQL-vs-REST quota interaction — NOT verifiable from public docs (inferred only)

No page (tiers, key, blog, GraphQL docs) states whether GraphQL/subscription usage is metered against the same 75K/month bucket as REST, metered separately, or unmetered. The only signal is that both surfaces share one API key and one auth scheme, which is suggestive but not a confirmation of shared billing. **This is flagged unverified.** It's moot for the recommended option (A, pure REST) but would need to be confirmed with a live key before ever building on B. Probe script in the Appendix.

### 5. Vercel's serverless model is a genuine architectural mismatch for a persistent GraphQL-subscription relay (verified against current Vercel docs, July 2026)

Fetched Vercel's live "Configuring Maximum Duration" doc. Current limits with Fluid Compute (default-on):

| Plan | Default/standard max | Extended max (beta) |
|---|---|---|
| Hobby | 300s (5 min) | not available |
| Pro | 800s (~13 min) | 1800s (30 min) |
| Enterprise | 800s | 1800s |

Even on the most generous Pro/Enterprise extended-beta setting, a function holding open a websocket to `graphql.collegefootballdata.com` must terminate and reconnect at least every 30 minutes — across a ~12-hour Saturday, that's 24+ reconnect cycles, each needing the CFBD-recommended backoff/retry handling. And a Vercel function relaying a subscription still doesn't solve fan-out to browser clients: Vercel functions don't hold long-lived inbound browser websocket connections either, so pushing updates to N simultaneous dashboard viewers would need a second piece of infrastructure (Supabase Realtime channel, Pusher, Ably, or similar) sitting between the relay and the browser. Vercel's own docs point workloads needing long-running/persistent execution at **Vercel Workflows** or external always-on compute, not at plain serverless functions. Source: [Configuring Maximum Duration for Vercel Functions](https://vercel.com/docs/functions/configuring-functions/duration).

## Quota math (Option A — REST polling)

`/scoreboard` is week-scoped, so poll cadence — not game count — drives usage.

| Cadence | Calls/Saturday (12h window) | Calls/month (4 Saturdays) | Calls/month (5 Saturdays) |
|---|---|---|---|
| 60s | 720 | 2,880 | 3,600 |
| 30s | 1,440 | 5,760 | 7,200 |
| 15s | 2,880 | 11,520 | 14,400 |

Against ~53K/month headroom (75K − 22K daily-load baseline), even the most aggressive realistic cadence (15s) uses ~27% of the remaining budget. 30–60s (plenty responsive for a dashboard humans are glancing at, not a betting terminal) uses 5–14%. **Quota is not a constraint for Option A under any sane polling interval.** The real constraint to design for instead is: don't let concurrent *browser* viewers each trigger their own CFBD call — poll CFBD once server-side (route handler + short-lived cache/`revalidate` tag, or a Vercel Cron hitting an endpoint that writes to a tiny cache), and serve all browser clients from that cache. That keeps the multiplier at "1 poll interval," not "1 poll interval × concurrent users."

## Architecture options

| | A — REST poll | B — GraphQL subscription relay | C — Defer |
|---|---|---|---|
| **Mechanism** | Route handler polls `/scoreboard` server-side on a timer/cron; browser clients poll or long-poll the route handler, which serves from a short cache | Server-side Node process holds a websocket to `wss://graphql.collegefootballdata.com`, relays events to browsers via a second pub/sub layer | Build nothing this cycle |
| **Data coverage** | Score, clock, period, possession, live WP, moneyline/spread — all confirmed present on `/scoreboard` today | Live play-by-play/drive coverage unconfirmed (documented gap); would still need `/scoreboard`-equivalent data, which GraphQL's own `scoreboard` field can provide, but that's the same payload as A with far more moving parts | N/A |
| **Key exposure** | Server-side only; trivial to keep off the browser | Server-side only, but the relay is a standing process, which is a bigger secret-management surface than a stateless route handler | N/A |
| **Vercel fit** | Native — stateless HTTP polling is exactly what route handlers + Vercel Cron do well | Poor fit — 30-min function ceiling forces reconnect cycles; still needs an external fan-out service to reach browsers; effectively requires infra outside the current Vercel+Supabase stack (e.g., an always-on relay on Fly.io/Render) | N/A |
| **Quota risk** | Negligible (see table above) | Unknown — quota-sharing with REST is unverified (Finding 4) | N/A |
| **Build complexity** | Low — 1 route handler, 1–2 client components, existing stack only | High — new always-on service, new pub/sub dependency, reconnect/backoff logic, new secret, new deploy target | None |
| **Timing fit** | Buildable any time; nothing to test live until games start late August | Same, plus more integration surface to shake out before week 1 | Pushes any live-dashboard capability past this season's start |

## Recommendation: GO on Option A (REST polling), reject Option B for this stack, don't flatly defer to C

**Build the REST-polling score/WP ticker, sequenced as a small pre-season sprint landing before Week 0 (late August) — not urgent this week, but not "no build this cycle" either.**

Rationale:
1. The REST `/scoreboard` endpoint the user already has access to (since Tier 1, well below their Tier 3) returns everything the two stated requirements need — score, clock/period/possession, and live win probability — in one call per poll, independent of how many games are live.
2. GraphQL/Hasura subscriptions would trade that simplicity for a websocket relay that doesn't fit Vercel's serverless execution model (30-minute hard ceiling even on extended-beta Pro/Enterprise) and still requires a second fan-out service to reach browsers — real new infrastructure bought for a live-play-by-play capability CFBD's own docs describe as not yet fully built out on GraphQL, when the REST endpoint already delivers the two things actually asked for.
3. Quota math rules out "can't afford it" as a reason to defer (worst realistic case ~14% of headroom), and there's ~5 weeks of runway before the season starts, so this is buildable calmly now rather than being either urgent or indefinitely postponed.

If a future requirement genuinely needs sub-second live play-by-play (not just score/clock/WP), re-open this spike then, with the quota-sharing question (Finding 4) resolved first via the probe below.

## Estimated build size if GO

Small — roughly a single-session PR:
- 1 Next.js route handler (`app/api/live/scoreboard/route.ts`) that calls CFBD `/scoreboard` server-side, holds the API key, and caches the response for the poll interval (`fetch(..., { next: { revalidate: 30 } })` or a Vercel Cron job writing to a tiny cache/KV).
- 1–2 client components: a score ticker strip and a live-WP bar/number, both reading from the app's own route handler (never CFBD directly).
- A quota guard: skip/backoff polling automatically when no games are scheduled that week (avoid burning calls Mon–Fri or bye weeks).
- No warehouse/Supabase schema changes required — this is ephemeral display data, not something that needs to land in `cfb-database`'s marts.

Rough size: ~150–300 LOC, 1 PR, well within a single sprint slot.

## Non-goals (this spike and the recommended build)

- No GraphQL/Hasura subscription integration this cycle.
- No live play-by-play/drive feed (only score, clock/period/possession, and win probability).
- No persistent relay service, no new hosting target outside Vercel + Supabase.
- No historical replay or push notifications.
- No expansion beyond Oklahoma/whatever teams the dashboard already scopes to — this doesn't imply a national live-scores product.
- No change to `cfb-database` daily-load pipelines or Supabase schema.

## What was verified vs. inferred

**Verified (fetched live, July 2026):**
- Tier ladder and that GraphQL is still Tier 3's headline feature (`api-tiers`).
- GraphQL/subscription endpoint URLs, Hasura backend, shared-API-key auth (GraphQL docs site + CFBD blog via radsportsanalytics.com redirect).
- `/scoreboard` REST schema including embedded live win probability (live OpenAPI spec).
- Live Scoreboard = Tier 1, Live Play-by-Play = Tier 2 (both below GraphQL's Tier 3 gate).
- Current Vercel function duration ceilings (Vercel docs, dated 2026-07-01).

**Inferred / unverified (flagged in-line, not load-bearing for the recommendation):**
- Whether GraphQL/subscription calls are metered against the same 75K/month REST budget, separately, or not at all (Finding 4).
- Whether GraphQL's play/drive coverage gap (documented in the original 2024-era CFBD blog post) has since been closed — graphqldocs.collegefootballdata.com's current field list still shows no dedicated live play/drive query.

## Appendix: user-runnable probes

No proxy block was hit while researching this spike, but the quota-sharing question above has no public documentation answer — it needs a live API key, which this sandbox doesn't have. Run these once, right before or during Week 0, to close the gap before ever reconsidering Option B:

```bash
# 1. REST baseline call — check response headers for any quota/rate-limit hints
curl -sS -D - -o /dev/null \
  -H "Authorization: Bearer $CFBD_API_KEY" \
  "https://api.collegefootballdata.com/scoreboard?week=1&year=2026"
```

```python
# 2. GraphQL introspection — confirm current root query/subscription fields,
#    specifically checking for play/drive-level live data.
# pip install gql[websockets]
import asyncio
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

API_KEY = "..."  # same key as REST


async def introspect():
    transport = AIOHTTPTransport(
        url="https://graphql.collegefootballdata.com/v1/graphql",
        headers={"Authorization": f"Bearer {API_KEY}"},
    )
    async with Client(transport=transport, fetch_schema_from_transport=True) as session:
        schema = session.client.schema
        query_fields = sorted(schema.query_type.fields.keys())
        sub_fields = (
            sorted(schema.subscription_type.fields.keys()) if schema.subscription_type else []
        )
        print("Query fields:", query_fields)
        print("Subscription fields:", sub_fields)
        print(
            "Has play/drive live field:",
            any("play" in f.lower() or "drive" in f.lower() for f in query_fields),
        )


asyncio.run(introspect())
```

```python
# 3. One subscription attempt — confirm it actually pushes events, and note
#    reconnect behavior under the "brittle" warning CFBD's own docs give.
# pip install gql[websockets]
import asyncio
from gql import Client, gql
from gql.transport.websockets import WebsocketsTransport

API_KEY = "..."


async def subscribe():
    transport = WebsocketsTransport(
        url="wss://graphql.collegefootballdata.com/v1/graphql",
        headers={"Authorization": f"Bearer {API_KEY}"},
    )
    query = gql("""
        subscription LiveScoreboard {
          scoreboard(where: {status: {_eq: "in_progress"}}) {
            homeTeam { name points winProbability }
            awayTeam { name points winProbability }
            period
            clock
          }
        }
    """)
    async with Client(transport=transport) as session:
        async for result in session.subscribe(query):
            print(result)


asyncio.run(subscribe())
```

```bash
# 4. After running #1-3 during an active game window, check the API dashboard
# (collegefootballdata.com account page) for updated call-count usage to see
# whether the GraphQL calls in #2/#3 moved the same counter as #1's REST call.
```
