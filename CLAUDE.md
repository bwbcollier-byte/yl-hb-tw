# CLAUDE.md — `yl-hb-tw` (Twitter / X enrichment via Airtable)

Conventions shared across the `yl-hb-*` fleet live in
[`SCRAPER-CLAUDE-TEMPLATE.md`](../SCRAPER-CLAUDE-TEMPLATE.md). **This repo
diverges from the template** — no Supabase, Airtable-only.

## What this repo does

Pulls Twitter/X handles from Airtable, queries the
`twitter-api45.p.rapidapi.com` RapidAPI host for follower counts, bios,
verified status, and bulk-PATCHes results back into Airtable.

## Stack

**Custom variant — TypeScript + Airtable.** No Supabase. Single TS file
in `src/`, single workflow.

## Repo layout

```
src/
  social-enrich-twitter.ts         # entry point
.github/workflows/
  twitter-enrichment.yml
package.json
tsconfig.json
```

## Auth

> Convention divergence: no `SUPABASE_*` env vars.

```
AIRTABLE_API_KEY        # required
RAPIDAPI_KEY            # required
RAPIDAPI_HOST           # default twitter-api45.p.rapidapi.com
```

## Workflow lifecycle convention

> Convention divergence: no `log_workflow_run`. Dashboard won't see runs.
> If observability is wanted, add the service-role auth and standard
> blocks per template.

## Tables this repo touches

Airtable only — no Supabase tables.

## Running locally

```bash
npm install
cp .env.example .env.local           # if present; otherwise create
# Set: AIRTABLE_API_KEY, RAPIDAPI_KEY (and optionally RAPIDAPI_HOST)
npm start                            # ts-node src/social-enrich-twitter.ts
```

## Per-repo gotchas

- **Single RapidAPI key.** Unlike `yl-hb-ig` (which rotates 11 keys),
  this repo uses one. Throughput is correspondingly lower; throttle
  upstream Airtable reads to avoid 429s.
- **`twitter-api45.p.rapidapi.com` is the host today.** RapidAPI hosts
  go stale; if requests start 404'ing across the board, check whether
  the host has been deprecated and update `RAPIDAPI_HOST`.
- **Airtable handles can be bare (`@user`), URLs, or full strings.**
  The script normalises via `extractHandle()` — keep that helper resilient
  when adding new input shapes.
- **Bulk-PATCH 10 is the Airtable max.** Don't switch to single-record
  updates.

## Conventions Claude should follow when editing this repo

- **Don't introduce a Supabase client** unless this is intentionally
  migrating off Airtable.
- **Don't replace the RapidAPI provider with twitter.com scraping** —
  Twitter's web aggressively blocks unauthenticated bots; the RapidAPI
  fronts handle the auth dance.

## Related repos

- `yl-hb-ig`, `yl-hb-sc`, `yl-hb-sk` — sibling Airtable-only enrichers.
- The remaining `yl-hb-*` repos write to Supabase.
