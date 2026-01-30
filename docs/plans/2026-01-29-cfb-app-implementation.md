# CFB App Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build an interactive CFB analytics portal with team search, metrics dashboard, and drive patterns visualization.

**Architecture:** Next.js 14 App Router with server components fetching from Supabase Postgres. D3.js renders custom football field visualizations. Deploy to Vercel.

**Tech Stack:** Next.js 14, React 18, TypeScript, D3.js, Tailwind CSS, Supabase JS, Radix UI

---

## Phase 1: Project Setup

### Task 1: Create Next.js Project

**Files:**
- Create: `/Users/robstover/Development/personal/cfb-app/` (new project)

**Step 1: Create the project**

```bash
cd /Users/robstover/Development/personal
npx create-next-app@latest cfb-app --typescript --tailwind --eslint --app --src-dir --import-alias "@/*"
```

When prompted:
- Would you like to use Turbopack? → No
- Would you like to customize the default import alias? → No

**Step 2: Verify project created**

```bash
cd cfb-app && ls -la
```
Expected: See `src/`, `package.json`, `tailwind.config.ts`, etc.

**Step 3: Initialize git and commit**

```bash
git init
git add -A
git commit -m "Initial commit from create-next-app

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

### Task 2: Install Dependencies

**Files:**
- Modify: `package.json`

**Step 1: Install Supabase client**

```bash
npm install @supabase/supabase-js @supabase/ssr
```

**Step 2: Install D3 for visualizations**

```bash
npm install d3 @types/d3
```

**Step 3: Install Radix UI for accessible components**

```bash
npm install @radix-ui/react-select @radix-ui/react-toggle
```

**Step 4: Verify dependencies**

```bash
npm ls @supabase/supabase-js d3 @radix-ui/react-select
```
Expected: Shows installed versions

**Step 5: Commit**

```bash
git add package.json package-lock.json
git commit -m "feat: add supabase, d3, and radix dependencies

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

### Task 3: Configure Supabase Client

**Files:**
- Create: `src/lib/supabase/client.ts`
- Create: `src/lib/supabase/server.ts`
- Create: `.env.local`

**Step 1: Create environment file**

Create `.env.local`:

```env
NEXT_PUBLIC_SUPABASE_URL=https://your-project.supabase.co
NEXT_PUBLIC_SUPABASE_ANON_KEY=your-anon-key
```

Get these values from your Supabase dashboard → Settings → API.

**Step 2: Create lib directory**

```bash
mkdir -p src/lib/supabase
```

**Step 3: Create browser client**

Create `src/lib/supabase/client.ts`:

```typescript
import { createBrowserClient } from '@supabase/ssr'

export function createClient() {
  return createBrowserClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
  )
}
```

**Step 4: Create server client**

Create `src/lib/supabase/server.ts`:

```typescript
import { createServerClient } from '@supabase/ssr'
import { cookies } from 'next/headers'

export async function createClient() {
  const cookieStore = await cookies()

  return createServerClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!,
    {
      cookies: {
        getAll() {
          return cookieStore.getAll()
        },
        setAll(cookiesToSet) {
          try {
            cookiesToSet.forEach(({ name, value, options }) =>
              cookieStore.set(name, value, options)
            )
          } catch {
            // Called from Server Component
          }
        },
      },
    }
  )
}
```

**Step 5: Add .env.local to .gitignore**

Verify `.gitignore` contains:
```
.env*.local
```

**Step 6: Test connection**

Create a temporary test in `src/app/page.tsx`:

```typescript
import { createClient } from '@/lib/supabase/server'

export default async function Home() {
  const supabase = await createClient()
  const { data, error } = await supabase.from('teams').select('school').limit(5)

  return (
    <main className="p-8">
      <h1 className="text-2xl font-bold">CFB App</h1>
      <pre>{JSON.stringify(data, null, 2)}</pre>
      {error && <p className="text-red-500">{error.message}</p>}
    </main>
  )
}
```

**Step 7: Run dev server and verify**

```bash
npm run dev
```

Open http://localhost:3000 — should see team names from your database.

**Step 8: Commit**

```bash
git add src/lib/supabase/ .gitignore
git commit -m "feat: configure supabase client for browser and server

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

### Task 4: Create Database Types

**Files:**
- Create: `src/lib/types/database.ts`

**Step 1: Generate types from Supabase**

Option A (if Supabase CLI installed):
```bash
npx supabase gen types typescript --project-id your-project-id > src/lib/types/database.ts
```

Option B (manual for now):

Create `src/lib/types/database.ts`:

```typescript
export interface Team {
  id: number
  school: string
  mascot: string | null
  abbreviation: string | null
  conference: string | null
  color: string | null
  alt_color: string | null
  logo: string | null
  alt_logo: string | null
}

export interface TeamSeasonEpa {
  season: number
  team: string
  games: number
  total_plays: number
  total_epa: number
  epa_per_play: number
  success_rate: number
  explosiveness: number
  off_epa_rank: number
  def_epa_rank: number
}

export interface TeamStyleProfile {
  season: number
  team: string
  run_rate: number
  pass_rate: number
  epa_rushing: number
  epa_passing: number
  plays_per_game: number
  tempo_category: 'up_tempo' | 'balanced' | 'slow'
  offensive_identity: 'run_heavy' | 'balanced' | 'pass_heavy'
  def_epa_vs_run: number
  def_epa_vs_pass: number
}

export interface TeamSeasonTrajectory {
  season: number
  team: string
  epa_per_play: number
  success_rate: number
  off_epa_rank: number
  def_epa_rank: number
  win_pct: number | null
  wins: number | null
  games: number | null
  recruiting_rank: number | null
  era_code: string | null
  era_name: string | null
  prev_epa: number | null
  epa_delta: number | null
}

export interface DrivePattern {
  start_yard: number
  end_yard: number
  outcome: 'touchdown' | 'field_goal' | 'punt' | 'turnover' | 'end_of_half' | 'downs'
  count: number
  avg_plays: number
  avg_yards: number
}
```

**Step 2: Commit**

```bash
mkdir -p src/lib/types
git add src/lib/types/database.ts
git commit -m "feat: add database types for team analytics

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Phase 2: Database Function

### Task 5: Create get_drive_patterns Function

**Files:**
- Create: `sql/get_drive_patterns.sql` (in cfb-database repo)

**Step 1: Write the function**

In the cfb-database repo, create `src/schemas/functions/get_drive_patterns.sql`:

```sql
-- Aggregate drive data for visualization
CREATE OR REPLACE FUNCTION get_drive_patterns(
  p_team TEXT,
  p_season INT
)
RETURNS TABLE (
  start_yard INT,
  end_yard INT,
  outcome TEXT,
  count BIGINT,
  avg_plays NUMERIC,
  avg_yards NUMERIC
) AS $$
BEGIN
  RETURN QUERY
  WITH drive_outcomes AS (
    SELECT
      d.start_yardline AS start_yard,
      d.end_yardline AS end_yard,
      CASE
        WHEN d.drive_result IN ('TD', 'TOUCHDOWN') THEN 'touchdown'
        WHEN d.drive_result IN ('FG', 'FIELD GOAL', 'FG GOOD') THEN 'field_goal'
        WHEN d.drive_result IN ('PUNT') THEN 'punt'
        WHEN d.drive_result IN ('INT', 'INTERCEPTION', 'FUMBLE', 'FUMBLE LOST', 'INT TD', 'FUMBLE TD') THEN 'turnover'
        WHEN d.drive_result IN ('END OF HALF', 'END OF GAME', 'END OF 4TH QUARTER') THEN 'end_of_half'
        WHEN d.drive_result IN ('DOWNS', 'TURNOVER ON DOWNS') THEN 'downs'
        ELSE 'other'
      END AS outcome,
      d.plays,
      d.yards
    FROM core.drives d
    JOIN core.games g ON d.game_id = g.id
    WHERE d.offense = p_team
      AND g.season = p_season
      AND d.start_yardline IS NOT NULL
      AND d.end_yardline IS NOT NULL
  )
  SELECT
    -- Bucket start yards into 10-yard zones
    (FLOOR(do.start_yard / 10) * 10)::INT AS start_yard,
    -- Bucket end yards into 10-yard zones
    (FLOOR(do.end_yard / 10) * 10)::INT AS end_yard,
    do.outcome,
    COUNT(*)::BIGINT AS count,
    ROUND(AVG(do.plays), 1) AS avg_plays,
    ROUND(AVG(do.yards), 1) AS avg_yards
  FROM drive_outcomes do
  WHERE do.outcome != 'other'
  GROUP BY 1, 2, do.outcome
  HAVING COUNT(*) >= 2  -- At least 2 drives for this pattern
  ORDER BY do.outcome, start_yard, end_yard;
END;
$$ LANGUAGE plpgsql STABLE;
```

**Step 2: Run the migration**

```bash
cd /Users/robstover/Development/personal/cfb-database
psql $DATABASE_URL -f src/schemas/functions/get_drive_patterns.sql
```

**Step 3: Test the function**

```bash
psql $DATABASE_URL -c "SELECT * FROM get_drive_patterns('Alabama', 2024) LIMIT 10;"
```
Expected: Rows with start_yard, end_yard, outcome, count, avg_plays, avg_yards

**Step 4: Commit in cfb-database repo**

```bash
git add src/schemas/functions/get_drive_patterns.sql
git commit -m "feat: add get_drive_patterns function for visualization

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Phase 3: Team List Page

### Task 6: Create Team List Page

**Files:**
- Modify: `src/app/page.tsx`
- Create: `src/components/TeamCard.tsx`

**Step 1: Create TeamCard component**

Create `src/components/TeamCard.tsx`:

```typescript
import Link from 'next/link'
import { Team } from '@/lib/types/database'

interface TeamCardProps {
  team: Team
}

export function TeamCard({ team }: TeamCardProps) {
  const slug = team.school.toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '')

  return (
    <Link
      href={`/teams/${slug}`}
      className="block p-4 border rounded-lg hover:border-blue-500 hover:shadow-md transition-all"
      style={{ borderLeftColor: team.color || '#6b7280', borderLeftWidth: '4px' }}
    >
      <div className="flex items-center gap-3">
        {team.logo && (
          <img
            src={team.logo}
            alt={`${team.school} logo`}
            className="w-10 h-10 object-contain"
          />
        )}
        <div>
          <h2 className="font-semibold text-lg">{team.school}</h2>
          <p className="text-sm text-gray-500">{team.conference || 'Independent'}</p>
        </div>
      </div>
    </Link>
  )
}
```

**Step 2: Update home page**

Replace `src/app/page.tsx`:

```typescript
import { createClient } from '@/lib/supabase/server'
import { TeamCard } from '@/components/TeamCard'
import { Team } from '@/lib/types/database'

export default async function Home() {
  const supabase = await createClient()
  const { data: teams, error } = await supabase
    .from('teams')
    .select('*')
    .not('conference', 'is', null)
    .order('school')

  if (error) {
    return <div className="p-8 text-red-500">Error loading teams: {error.message}</div>
  }

  // Group by conference
  const byConference = (teams as Team[]).reduce((acc, team) => {
    const conf = team.conference || 'Independent'
    if (!acc[conf]) acc[conf] = []
    acc[conf].push(team)
    return acc
  }, {} as Record<string, Team[]>)

  return (
    <main className="max-w-6xl mx-auto p-8">
      <h1 className="text-3xl font-bold mb-2">CFB Team 360</h1>
      <p className="text-gray-600 mb-8">Select a team to view analytics</p>

      {Object.entries(byConference).sort().map(([conference, confTeams]) => (
        <section key={conference} className="mb-8">
          <h2 className="text-xl font-semibold mb-4 text-gray-700">{conference}</h2>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
            {confTeams.map(team => (
              <TeamCard key={team.id} team={team} />
            ))}
          </div>
        </section>
      ))}
    </main>
  )
}
```

**Step 3: Run and verify**

```bash
npm run dev
```
Open http://localhost:3000 — should see teams grouped by conference

**Step 4: Commit**

```bash
mkdir -p src/components
git add src/app/page.tsx src/components/TeamCard.tsx
git commit -m "feat: add team list page grouped by conference

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

### Task 7: Add Team Search

**Files:**
- Create: `src/components/TeamSearch.tsx`
- Modify: `src/app/page.tsx`

**Step 1: Create search component**

Create `src/components/TeamSearch.tsx`:

```typescript
'use client'

import { useState } from 'react'

interface TeamSearchProps {
  onSearch: (query: string) => void
}

export function TeamSearch({ onSearch }: TeamSearchProps) {
  const [query, setQuery] = useState('')

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value
    setQuery(value)
    onSearch(value)
  }

  return (
    <div className="mb-8">
      <label htmlFor="team-search" className="sr-only">Search teams</label>
      <input
        id="team-search"
        type="search"
        placeholder="Search teams..."
        value={query}
        onChange={handleChange}
        className="w-full max-w-md px-4 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
      />
    </div>
  )
}
```

**Step 2: Create client wrapper for filtering**

Create `src/components/TeamList.tsx`:

```typescript
'use client'

import { useState, useMemo } from 'react'
import { TeamCard } from './TeamCard'
import { TeamSearch } from './TeamSearch'
import { Team } from '@/lib/types/database'

interface TeamListProps {
  teams: Team[]
}

export function TeamList({ teams }: TeamListProps) {
  const [searchQuery, setSearchQuery] = useState('')

  const filteredTeams = useMemo(() => {
    if (!searchQuery.trim()) return teams
    const query = searchQuery.toLowerCase()
    return teams.filter(team =>
      team.school.toLowerCase().includes(query) ||
      team.conference?.toLowerCase().includes(query) ||
      team.mascot?.toLowerCase().includes(query)
    )
  }, [teams, searchQuery])

  const byConference = filteredTeams.reduce((acc, team) => {
    const conf = team.conference || 'Independent'
    if (!acc[conf]) acc[conf] = []
    acc[conf].push(team)
    return acc
  }, {} as Record<string, Team[]>)

  return (
    <>
      <TeamSearch onSearch={setSearchQuery} />

      {Object.keys(byConference).length === 0 ? (
        <p className="text-gray-500">No teams found matching "{searchQuery}"</p>
      ) : (
        Object.entries(byConference).sort().map(([conference, confTeams]) => (
          <section key={conference} className="mb-8">
            <h2 className="text-xl font-semibold mb-4 text-gray-700">{conference}</h2>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
              {confTeams.map(team => (
                <TeamCard key={team.id} team={team} />
              ))}
            </div>
          </section>
        ))
      )}
    </>
  )
}
```

**Step 3: Update page to use TeamList**

Update `src/app/page.tsx`:

```typescript
import { createClient } from '@/lib/supabase/server'
import { TeamList } from '@/components/TeamList'
import { Team } from '@/lib/types/database'

export default async function Home() {
  const supabase = await createClient()
  const { data: teams, error } = await supabase
    .from('teams')
    .select('*')
    .not('conference', 'is', null)
    .order('school')

  if (error) {
    return <div className="p-8 text-red-500">Error loading teams: {error.message}</div>
  }

  return (
    <main className="max-w-6xl mx-auto p-8">
      <h1 className="text-3xl font-bold mb-2">CFB Team 360</h1>
      <p className="text-gray-600 mb-8">Select a team to view analytics</p>

      <TeamList teams={teams as Team[]} />
    </main>
  )
}
```

**Step 4: Test search**

```bash
npm run dev
```
Type in search box — should filter teams in real-time

**Step 5: Commit**

```bash
git add src/components/TeamSearch.tsx src/components/TeamList.tsx src/app/page.tsx
git commit -m "feat: add client-side team search with filtering

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Phase 4: Team Dashboard

### Task 8: Create Team Page Route

**Files:**
- Create: `src/app/teams/[slug]/page.tsx`
- Create: `src/lib/utils.ts`

**Step 1: Create utils for slug handling**

Create `src/lib/utils.ts`:

```typescript
export function teamNameToSlug(name: string): string {
  return name.toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '')
}

export function slugToTeamName(slug: string): string {
  // This is approximate — we'll look up the actual team by slug
  return slug.split('-').map(word =>
    word.charAt(0).toUpperCase() + word.slice(1)
  ).join(' ')
}

export function formatPercent(value: number): string {
  return `${(value * 100).toFixed(1)}%`
}

export function formatRank(rank: number): string {
  if (rank === 1) return '1st'
  if (rank === 2) return '2nd'
  if (rank === 3) return '3rd'
  return `${rank}th`
}
```

**Step 2: Create team page**

Create `src/app/teams/[slug]/page.tsx`:

```typescript
import { createClient } from '@/lib/supabase/server'
import { notFound } from 'next/navigation'
import { Team, TeamSeasonEpa, TeamStyleProfile, TeamSeasonTrajectory, DrivePattern } from '@/lib/types/database'

interface TeamPageProps {
  params: Promise<{ slug: string }>
}

async function getTeamBySlug(supabase: any, slug: string): Promise<Team | null> {
  const { data: teams } = await supabase.from('teams').select('*')

  return teams?.find((team: Team) => {
    const teamSlug = team.school.toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '')
    return teamSlug === slug
  }) || null
}

export default async function TeamPage({ params }: TeamPageProps) {
  const { slug } = await params
  const supabase = await createClient()

  const team = await getTeamBySlug(supabase, slug)

  if (!team) {
    notFound()
  }

  const currentSeason = 2024  // TODO: make dynamic

  // Fetch all data in parallel
  const [metricsResult, styleResult, trajectoryResult, drivesResult] = await Promise.all([
    supabase
      .from('team_season_epa')
      .select('*')
      .eq('team', team.school)
      .eq('season', currentSeason)
      .single(),
    supabase
      .from('team_style_profile')
      .select('*')
      .eq('team', team.school)
      .eq('season', currentSeason)
      .single(),
    supabase
      .from('team_season_trajectory')
      .select('*')
      .eq('team', team.school)
      .order('season', { ascending: true }),
    supabase.rpc('get_drive_patterns', {
      p_team: team.school,
      p_season: currentSeason
    })
  ])

  const metrics = metricsResult.data as TeamSeasonEpa | null
  const style = styleResult.data as TeamStyleProfile | null
  const trajectory = trajectoryResult.data as TeamSeasonTrajectory[] | null
  const drives = drivesResult.data as DrivePattern[] | null

  return (
    <main className="max-w-6xl mx-auto p-8">
      {/* Header */}
      <header className="flex items-center gap-4 mb-8">
        {team.logo && (
          <img
            src={team.logo}
            alt={`${team.school} logo`}
            className="w-16 h-16 object-contain"
          />
        )}
        <div>
          <h1 className="text-3xl font-bold">{team.school}</h1>
          <p className="text-gray-600">{team.conference || 'Independent'} • {currentSeason} Season</p>
        </div>
      </header>

      {/* Placeholder sections */}
      <section className="mb-8 p-6 border rounded-lg bg-gray-50">
        <h2 className="text-xl font-semibold mb-4">Drive Patterns</h2>
        <p className="text-gray-500">Visualization coming next...</p>
        <pre className="mt-4 text-xs bg-white p-4 rounded overflow-auto max-h-40">
          {JSON.stringify(drives?.slice(0, 5), null, 2)}
        </pre>
      </section>

      <section className="mb-8 p-6 border rounded-lg">
        <h2 className="text-xl font-semibold mb-4">Metrics</h2>
        {metrics ? (
          <pre className="text-sm">{JSON.stringify(metrics, null, 2)}</pre>
        ) : (
          <p className="text-gray-500">No metrics available</p>
        )}
      </section>

      <section className="mb-8 p-6 border rounded-lg">
        <h2 className="text-xl font-semibold mb-4">Style Profile</h2>
        {style ? (
          <pre className="text-sm">{JSON.stringify(style, null, 2)}</pre>
        ) : (
          <p className="text-gray-500">No style data available</p>
        )}
      </section>

      <section className="mb-8 p-6 border rounded-lg">
        <h2 className="text-xl font-semibold mb-4">Historical Trajectory</h2>
        {trajectory ? (
          <pre className="text-sm max-h-60 overflow-auto">{JSON.stringify(trajectory, null, 2)}</pre>
        ) : (
          <p className="text-gray-500">No trajectory data available</p>
        )}
      </section>
    </main>
  )
}
```

**Step 3: Test navigation**

```bash
npm run dev
```
Click on a team card — should navigate to `/teams/alabama` etc. and show raw data

**Step 4: Commit**

```bash
mkdir -p src/app/teams/\[slug\]
git add src/app/teams/\[slug\]/page.tsx src/lib/utils.ts
git commit -m "feat: add team page route with data fetching

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

### Task 9: Create Metrics Cards Component

**Files:**
- Create: `src/components/team/MetricsCards.tsx`
- Modify: `src/app/teams/[slug]/page.tsx`

**Step 1: Create MetricsCards**

Create `src/components/team/MetricsCards.tsx`:

```typescript
import { TeamSeasonEpa } from '@/lib/types/database'
import { formatRank } from '@/lib/utils'

interface MetricsCardsProps {
  metrics: TeamSeasonEpa
}

interface MetricCardProps {
  label: string
  value: string
  rank?: number
  trend?: 'up' | 'down' | 'neutral'
  trendLabel?: string
}

function MetricCard({ label, value, rank, trend, trendLabel }: MetricCardProps) {
  return (
    <div className="p-4 border rounded-lg bg-white">
      <p className="text-sm text-gray-500 mb-1">{label}</p>
      <p className="text-2xl font-bold">{value}</p>
      <div className="flex items-center gap-2 mt-1">
        {rank && (
          <span className="text-sm text-gray-600">{formatRank(rank)} nationally</span>
        )}
        {trend && trendLabel && (
          <span className={`text-sm ${
            trend === 'up' ? 'text-green-600' :
            trend === 'down' ? 'text-red-600' : 'text-gray-500'
          }`}>
            {trend === 'up' ? '↑' : trend === 'down' ? '↓' : '→'} {trendLabel}
          </span>
        )}
      </div>
    </div>
  )
}

export function MetricsCards({ metrics }: MetricsCardsProps) {
  return (
    <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
      <MetricCard
        label="EPA per Play"
        value={metrics.epa_per_play.toFixed(3)}
        rank={metrics.off_epa_rank}
      />
      <MetricCard
        label="Success Rate"
        value={`${(metrics.success_rate * 100).toFixed(1)}%`}
      />
      <MetricCard
        label="Explosiveness"
        value={metrics.explosiveness.toFixed(3)}
      />
      <MetricCard
        label="Games Played"
        value={metrics.games.toString()}
      />
    </div>
  )
}
```

**Step 2: Update team page to use component**

In `src/app/teams/[slug]/page.tsx`, add import and replace metrics section:

```typescript
import { MetricsCards } from '@/components/team/MetricsCards'

// ... in the return, replace metrics section:
<section className="mb-8">
  <h2 className="text-xl font-semibold mb-4">Performance Metrics</h2>
  {metrics ? (
    <MetricsCards metrics={metrics} />
  ) : (
    <p className="text-gray-500">No metrics available for this season</p>
  )}
</section>
```

**Step 3: Verify**

```bash
npm run dev
```
Team page should show styled metric cards

**Step 4: Commit**

```bash
mkdir -p src/components/team
git add src/components/team/MetricsCards.tsx src/app/teams/\[slug\]/page.tsx
git commit -m "feat: add metrics cards component with rankings

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

### Task 10: Create Style Profile Component

**Files:**
- Create: `src/components/team/StyleProfile.tsx`
- Modify: `src/app/teams/[slug]/page.tsx`

**Step 1: Create StyleProfile**

Create `src/components/team/StyleProfile.tsx`:

```typescript
import { TeamStyleProfile as StyleData } from '@/lib/types/database'

interface StyleProfileProps {
  style: StyleData
}

function IdentityBadge({ identity }: { identity: string }) {
  const colors = {
    run_heavy: 'bg-amber-100 text-amber-800',
    balanced: 'bg-blue-100 text-blue-800',
    pass_heavy: 'bg-purple-100 text-purple-800',
  }

  const labels = {
    run_heavy: 'Run Heavy',
    balanced: 'Balanced',
    pass_heavy: 'Pass Heavy',
  }

  return (
    <span className={`px-3 py-1 rounded-full text-sm font-medium ${colors[identity as keyof typeof colors] || 'bg-gray-100'}`}>
      {labels[identity as keyof typeof labels] || identity}
    </span>
  )
}

function TempoBadge({ tempo }: { tempo: string }) {
  const colors = {
    up_tempo: 'bg-green-100 text-green-800',
    balanced: 'bg-gray-100 text-gray-800',
    slow: 'bg-orange-100 text-orange-800',
  }

  const labels = {
    up_tempo: 'Up Tempo',
    balanced: 'Balanced Tempo',
    slow: 'Slow Tempo',
  }

  return (
    <span className={`px-3 py-1 rounded-full text-sm font-medium ${colors[tempo as keyof typeof colors] || 'bg-gray-100'}`}>
      {labels[tempo as keyof typeof labels] || tempo}
    </span>
  )
}

export function StyleProfile({ style }: StyleProfileProps) {
  const runPercent = (style.run_rate * 100).toFixed(0)
  const passPercent = (style.pass_rate * 100).toFixed(0)

  return (
    <div className="p-6 border rounded-lg">
      <div className="flex items-center gap-3 mb-6">
        <IdentityBadge identity={style.offensive_identity} />
        <TempoBadge tempo={style.tempo_category} />
        <span className="text-sm text-gray-500">
          {style.plays_per_game.toFixed(1)} plays/game
        </span>
      </div>

      {/* Run/Pass Split Bar */}
      <div className="mb-6">
        <div className="flex justify-between text-sm mb-1">
          <span>Run {runPercent}%</span>
          <span>Pass {passPercent}%</span>
        </div>
        <div className="h-4 rounded-full overflow-hidden flex">
          <div
            className="bg-amber-500"
            style={{ width: `${runPercent}%` }}
            role="img"
            aria-label={`Run rate: ${runPercent}%`}
          />
          <div
            className="bg-purple-500"
            style={{ width: `${passPercent}%` }}
            role="img"
            aria-label={`Pass rate: ${passPercent}%`}
          />
        </div>
      </div>

      {/* EPA by Type */}
      <div className="grid grid-cols-2 gap-4">
        <div>
          <p className="text-sm text-gray-500">Rushing EPA</p>
          <p className="text-xl font-semibold">{style.epa_rushing?.toFixed(3) || 'N/A'}</p>
        </div>
        <div>
          <p className="text-sm text-gray-500">Passing EPA</p>
          <p className="text-xl font-semibold">{style.epa_passing?.toFixed(3) || 'N/A'}</p>
        </div>
        <div>
          <p className="text-sm text-gray-500">Def vs Run</p>
          <p className="text-xl font-semibold">{style.def_epa_vs_run?.toFixed(3) || 'N/A'}</p>
        </div>
        <div>
          <p className="text-sm text-gray-500">Def vs Pass</p>
          <p className="text-xl font-semibold">{style.def_epa_vs_pass?.toFixed(3) || 'N/A'}</p>
        </div>
      </div>
    </div>
  )
}
```

**Step 2: Update team page**

```typescript
import { StyleProfile } from '@/components/team/StyleProfile'

// Replace style section:
<section className="mb-8">
  <h2 className="text-xl font-semibold mb-4">Style Profile</h2>
  {style ? (
    <StyleProfile style={style} />
  ) : (
    <p className="text-gray-500">No style data available</p>
  )}
</section>
```

**Step 3: Commit**

```bash
git add src/components/team/StyleProfile.tsx src/app/teams/\[slug\]/page.tsx
git commit -m "feat: add style profile component with run/pass split

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Phase 5: Drive Patterns Visualization

### Task 11: Create Football Field SVG

**Files:**
- Create: `src/components/visualizations/FootballField.tsx`

**Step 1: Create the field component**

Create `src/components/visualizations/FootballField.tsx`:

```typescript
'use client'

interface FootballFieldProps {
  width?: number
  height?: number
  children?: React.ReactNode
}

export function FootballField({ width = 1000, height = 400, children }: FootballFieldProps) {
  // Field dimensions: 100 yards + 2 end zones (10 yards each) = 120 total
  const endZoneWidth = (width / 120) * 10
  const fieldWidth = width - (2 * endZoneWidth)
  const yardWidth = fieldWidth / 100

  return (
    <svg
      viewBox={`0 0 ${width} ${height}`}
      className="w-full h-auto"
      aria-label="Football field visualization showing drive patterns"
      role="img"
    >
      {/* Background */}
      <rect x={0} y={0} width={width} height={height} fill="#2d5a27" />

      {/* Left End Zone */}
      <rect
        x={0}
        y={0}
        width={endZoneWidth}
        height={height}
        fill="#1e3d1a"
        stroke="#fff"
        strokeWidth={2}
      />
      <text
        x={endZoneWidth / 2}
        y={height / 2}
        fill="#fff"
        fontSize={24}
        textAnchor="middle"
        dominantBaseline="middle"
        transform={`rotate(-90, ${endZoneWidth / 2}, ${height / 2})`}
        opacity={0.5}
      >
        END ZONE
      </text>

      {/* Right End Zone */}
      <rect
        x={width - endZoneWidth}
        y={0}
        width={endZoneWidth}
        height={height}
        fill="#1e3d1a"
        stroke="#fff"
        strokeWidth={2}
      />
      <text
        x={width - endZoneWidth / 2}
        y={height / 2}
        fill="#fff"
        fontSize={24}
        textAnchor="middle"
        dominantBaseline="middle"
        transform={`rotate(90, ${width - endZoneWidth / 2}, ${height / 2})`}
        opacity={0.5}
      >
        END ZONE
      </text>

      {/* Yard Lines */}
      {Array.from({ length: 21 }).map((_, i) => {
        const yard = i * 5
        const x = endZoneWidth + (yard * yardWidth)
        const isMajor = yard % 10 === 0

        return (
          <g key={yard}>
            <line
              x1={x}
              y1={0}
              x2={x}
              y2={height}
              stroke="#fff"
              strokeWidth={isMajor ? 2 : 1}
              opacity={isMajor ? 0.8 : 0.4}
            />
            {isMajor && yard > 0 && yard < 100 && (
              <text
                x={x}
                y={height - 10}
                fill="#fff"
                fontSize={14}
                textAnchor="middle"
                opacity={0.6}
              >
                {yard <= 50 ? yard : 100 - yard}
              </text>
            )}
          </g>
        )
      })}

      {/* Hash Marks */}
      {Array.from({ length: 100 }).map((_, yard) => {
        const x = endZoneWidth + (yard * yardWidth)
        return (
          <g key={`hash-${yard}`}>
            <line x1={x} y1={height * 0.35} x2={x + yardWidth * 0.5} y2={height * 0.35} stroke="#fff" strokeWidth={1} opacity={0.3} />
            <line x1={x} y1={height * 0.65} x2={x + yardWidth * 0.5} y2={height * 0.65} stroke="#fff" strokeWidth={1} opacity={0.3} />
          </g>
        )
      })}

      {/* 50 Yard Line Emphasis */}
      <line
        x1={endZoneWidth + 50 * yardWidth}
        y1={0}
        x2={endZoneWidth + 50 * yardWidth}
        y2={height}
        stroke="#fff"
        strokeWidth={3}
        opacity={0.9}
      />

      {/* Children (arcs, overlays, etc.) */}
      <g transform={`translate(${endZoneWidth}, 0)`}>
        {children}
      </g>
    </svg>
  )
}

// Helper to convert yard line to x position within the field area
export function yardToX(yard: number, fieldWidth: number): number {
  return (yard / 100) * fieldWidth
}
```

**Step 2: Test the field**

Add to team page temporarily:
```typescript
import { FootballField } from '@/components/visualizations/FootballField'

// In the drives section:
<FootballField />
```

**Step 3: Commit**

```bash
mkdir -p src/components/visualizations
git add src/components/visualizations/FootballField.tsx
git commit -m "feat: add football field SVG component

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

### Task 12: Create Drive Arcs Visualization

**Files:**
- Create: `src/components/visualizations/DrivePatterns.tsx`
- Modify: `src/app/teams/[slug]/page.tsx`

**Step 1: Create DrivePatterns component**

Create `src/components/visualizations/DrivePatterns.tsx`:

```typescript
'use client'

import { useRef, useEffect, useState } from 'react'
import * as d3 from 'd3'
import { FootballField, yardToX } from './FootballField'
import { DrivePattern } from '@/lib/types/database'

interface DrivePatternsProps {
  drives: DrivePattern[]
  teamName: string
}

const OUTCOME_STYLES = {
  touchdown: { color: '#22c55e', dash: 'none', label: 'Touchdown' },
  field_goal: { color: '#3b82f6', dash: '8,4', label: 'Field Goal' },
  punt: { color: '#6b7280', dash: '4,4', label: 'Punt' },
  turnover: { color: '#ef4444', dash: 'none', label: 'Turnover' },
  downs: { color: '#f59e0b', dash: '2,2', label: 'Turnover on Downs' },
  end_of_half: { color: '#8b5cf6', dash: '6,2', label: 'End of Half' },
} as const

export function DrivePatterns({ drives, teamName }: DrivePatternsProps) {
  const [selectedOutcome, setSelectedOutcome] = useState<string | null>(null)
  const [tooltip, setTooltip] = useState<{ x: number; y: number; data: DrivePattern } | null>(null)

  const fieldWidth = 1000 - (1000 / 120) * 20  // Subtract end zones
  const fieldHeight = 400

  // Generate arc path
  function getArcPath(drive: DrivePattern): string {
    const startX = yardToX(drive.start_yard, fieldWidth)
    const endX = yardToX(drive.end_yard, fieldWidth)
    const midX = (startX + endX) / 2

    // Arc height based on drive length, scaled by count
    const driveLength = Math.abs(drive.end_yard - drive.start_yard)
    const baseHeight = Math.min(driveLength * 2, fieldHeight * 0.4)
    const arcHeight = baseHeight * (1 + Math.log10(drive.count) * 0.2)

    const midY = fieldHeight / 2
    const controlY = midY - arcHeight

    return `M ${startX} ${midY} Q ${midX} ${controlY} ${endX} ${midY}`
  }

  const outcomes = [...new Set(drives.map(d => d.outcome))]

  return (
    <div className="relative">
      {/* Legend */}
      <div className="flex flex-wrap gap-4 mb-4" role="list" aria-label="Drive outcome legend">
        {outcomes.map(outcome => {
          const style = OUTCOME_STYLES[outcome as keyof typeof OUTCOME_STYLES] || { color: '#999', dash: 'none', label: outcome }
          const isSelected = selectedOutcome === null || selectedOutcome === outcome

          return (
            <button
              key={outcome}
              onClick={() => setSelectedOutcome(selectedOutcome === outcome ? null : outcome)}
              className={`flex items-center gap-2 px-3 py-1 rounded border transition-opacity ${
                isSelected ? 'opacity-100' : 'opacity-40'
              }`}
              aria-pressed={selectedOutcome === outcome}
            >
              <svg width={24} height={12}>
                <line
                  x1={0} y1={6} x2={24} y2={6}
                  stroke={style.color}
                  strokeWidth={3}
                  strokeDasharray={style.dash}
                />
              </svg>
              <span className="text-sm">{style.label}</span>
            </button>
          )
        })}
      </div>

      {/* Field with Arcs */}
      <FootballField width={1000} height={400}>
        {drives.map((drive, i) => {
          const style = OUTCOME_STYLES[drive.outcome as keyof typeof OUTCOME_STYLES] || { color: '#999', dash: 'none' }
          const isVisible = selectedOutcome === null || selectedOutcome === drive.outcome

          return (
            <path
              key={i}
              d={getArcPath(drive)}
              fill="none"
              stroke={style.color}
              strokeWidth={Math.max(2, Math.min(drive.count / 2, 8))}
              strokeDasharray={style.dash}
              opacity={isVisible ? 0.7 : 0.1}
              className="transition-opacity cursor-pointer hover:opacity-100"
              onMouseEnter={(e) => {
                const rect = e.currentTarget.getBoundingClientRect()
                setTooltip({ x: rect.x + rect.width / 2, y: rect.y, data: drive })
              }}
              onMouseLeave={() => setTooltip(null)}
              onFocus={(e) => {
                const rect = e.currentTarget.getBoundingClientRect()
                setTooltip({ x: rect.x + rect.width / 2, y: rect.y, data: drive })
              }}
              onBlur={() => setTooltip(null)}
              tabIndex={0}
              role="button"
              aria-label={`${drive.count} drives from ${drive.start_yard} to ${drive.end_yard} yard line, ${drive.outcome}`}
            />
          )
        })}
      </FootballField>

      {/* Tooltip */}
      {tooltip && (
        <div
          className="absolute bg-black text-white text-sm px-3 py-2 rounded shadow-lg pointer-events-none z-10"
          style={{
            left: tooltip.x,
            top: tooltip.y - 60,
            transform: 'translateX(-50%)'
          }}
        >
          <p className="font-semibold capitalize">{tooltip.data.outcome.replace('_', ' ')}</p>
          <p>{tooltip.data.count} drives</p>
          <p>{tooltip.data.start_yard} → {tooltip.data.end_yard} yard line</p>
          <p>Avg: {tooltip.data.avg_plays} plays, {tooltip.data.avg_yards} yards</p>
        </div>
      )}

      {/* Data Table Toggle */}
      <details className="mt-4">
        <summary className="cursor-pointer text-sm text-blue-600 hover:underline">
          View as table (screen reader accessible)
        </summary>
        <table className="mt-2 w-full text-sm border-collapse">
          <thead>
            <tr className="border-b">
              <th className="text-left p-2">Outcome</th>
              <th className="text-left p-2">Start</th>
              <th className="text-left p-2">End</th>
              <th className="text-left p-2">Count</th>
              <th className="text-left p-2">Avg Plays</th>
              <th className="text-left p-2">Avg Yards</th>
            </tr>
          </thead>
          <tbody>
            {drives.map((drive, i) => (
              <tr key={i} className="border-b">
                <td className="p-2 capitalize">{drive.outcome.replace('_', ' ')}</td>
                <td className="p-2">{drive.start_yard}</td>
                <td className="p-2">{drive.end_yard}</td>
                <td className="p-2">{drive.count}</td>
                <td className="p-2">{drive.avg_plays}</td>
                <td className="p-2">{drive.avg_yards}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </details>
    </div>
  )
}
```

**Step 2: Update team page**

```typescript
import { DrivePatterns } from '@/components/visualizations/DrivePatterns'

// Replace drive patterns section:
<section className="mb-8">
  <h2 className="text-xl font-semibold mb-4">Drive Patterns</h2>
  {drives && drives.length > 0 ? (
    <DrivePatterns drives={drives} teamName={team.school} />
  ) : (
    <p className="text-gray-500">No drive data available</p>
  )}
</section>
```

**Step 3: Test the visualization**

```bash
npm run dev
```
Navigate to a team page — should see the football field with colored arcs

**Step 4: Commit**

```bash
git add src/components/visualizations/DrivePatterns.tsx src/app/teams/\[slug\]/page.tsx
git commit -m "feat: add drive patterns visualization with D3

- Football field SVG with yard lines
- Colored arcs for each drive outcome
- Interactive legend filtering
- Hover tooltips with stats
- Accessible data table fallback

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Phase 6: Polish & Deploy

### Task 13: Add Layout and Navigation

**Files:**
- Modify: `src/app/layout.tsx`
- Create: `src/components/Header.tsx`

**Step 1: Create Header**

Create `src/components/Header.tsx`:

```typescript
import Link from 'next/link'

export function Header() {
  return (
    <header className="border-b bg-white">
      <div className="max-w-6xl mx-auto px-8 py-4 flex items-center justify-between">
        <Link href="/" className="text-xl font-bold text-gray-900 hover:text-blue-600">
          CFB Team 360
        </Link>
        <nav>
          <Link href="/" className="text-gray-600 hover:text-gray-900">
            All Teams
          </Link>
        </nav>
      </div>
    </header>
  )
}
```

**Step 2: Update layout**

Update `src/app/layout.tsx`:

```typescript
import type { Metadata } from 'next'
import { Inter } from 'next/font/google'
import './globals.css'
import { Header } from '@/components/Header'

const inter = Inter({ subsets: ['latin'] })

export const metadata: Metadata = {
  title: 'CFB Team 360 | College Football Analytics',
  description: 'Interactive analytics portal for college football teams',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body className={`${inter.className} bg-gray-50 min-h-screen`}>
        <Header />
        {children}
      </body>
    </html>
  )
}
```

**Step 3: Commit**

```bash
git add src/components/Header.tsx src/app/layout.tsx
git commit -m "feat: add header with navigation

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

### Task 14: Deploy to Vercel

**Files:**
- None (deployment setup)

**Step 1: Push to GitHub**

```bash
# Create GitHub repo first via gh CLI or GitHub.com
gh repo create cfb-app --private --source=. --push
```

Or manually:
```bash
git remote add origin git@github.com:YOUR_USERNAME/cfb-app.git
git push -u origin main
```

**Step 2: Connect to Vercel**

1. Go to https://vercel.com/new
2. Import your `cfb-app` repository
3. Configure environment variables:
   - `NEXT_PUBLIC_SUPABASE_URL`
   - `NEXT_PUBLIC_SUPABASE_ANON_KEY`
4. Deploy

**Step 3: Verify deployment**

Open the Vercel URL — should see the team list and be able to navigate to team pages

**Step 4: Commit any Vercel config**

```bash
git add vercel.json 2>/dev/null || true
git add -A
git commit -m "chore: configure vercel deployment

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>" || echo "Nothing to commit"
```

---

### Task 15: Final Verification

**Step 1: Run full test of flows**

1. Home page loads with team list
2. Search filters teams correctly
3. Click team → navigates to team page
4. Team page shows:
   - Header with logo and name
   - Metrics cards with EPA data
   - Style profile with run/pass split
   - Drive patterns visualization with interactive legend
5. Data table toggle works for accessibility
6. Mobile responsive (check at 375px width)

**Step 2: Run build to verify no errors**

```bash
npm run build
```
Expected: Build succeeds with no errors

**Step 3: Final commit**

```bash
git add -A
git commit -m "chore: MVP complete - Team 360 dashboard

Features:
- Team search and browse by conference
- Team 360 dashboard with EPA metrics
- Style profile with run/pass visualization
- Drive patterns hero visualization
- Accessible design with data table fallbacks
- Deployed to Vercel

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Summary

**Phase 1 (Tasks 1-4):** Project setup, Supabase connection, types
**Phase 2 (Task 5):** Database function for drive patterns
**Phase 3 (Tasks 6-7):** Team list page with search
**Phase 4 (Tasks 8-10):** Team dashboard with metrics and style
**Phase 5 (Tasks 11-12):** Drive patterns visualization
**Phase 6 (Tasks 13-15):** Polish and Vercel deployment

**Total: 15 tasks, MVP complete**
