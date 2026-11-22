import { keepPreviousData, useQuery } from '@tanstack/react-query'
import type { FeatureCollection, Point } from 'geojson'

export type Snapshot<P> = FeatureCollection<Point, P> & { time: number | null }

export type SnapshotResource = 'flights' | 'ships'

const REFRESH_MS = 30_000

async function getJson<T>(url: string): Promise<T> {
  const res = await fetch(url)
  if (!res.ok) throw new Error(`GET ${url} failed: ${res.status}`)
  return res.json()
}

export function emptySnapshot<P>(): Snapshot<P> {
  return { type: 'FeatureCollection', time: null, features: [] }
}

export function useSnapshotTimestamps(resource: SnapshotResource) {
  return useQuery({
    queryKey: [resource, 'timestamps'],
    queryFn: () => getJson<{ timestamps: number[] }>(`/api/${resource}/timestamps`),
    select: (data) => data.timestamps,
    refetchInterval: REFRESH_MS,
  })
}

/** `timestamp === null` means live: the latest snapshot, refreshed regularly. */
export function useSnapshot<P>(resource: SnapshotResource, timestamp: number | null, enabled: boolean) {
  const live = timestamp === null
  return useQuery({
    queryKey: [resource, live ? 'latest' : timestamp],
    queryFn: () => getJson<Snapshot<P>>(live ? `/api/${resource}/latest` : `/api/${resource}/${timestamp}`),
    refetchInterval: live ? REFRESH_MS : false,
    placeholderData: keepPreviousData,
    enabled,
  })
}
