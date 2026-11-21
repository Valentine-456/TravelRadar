import { keepPreviousData, useQuery } from '@tanstack/react-query'
import type { Planes } from '../components/PlanesLayer'

export type FlightsSnapshot = Planes & { time: number | null }

export const emptyFlights: FlightsSnapshot = { type: 'FeatureCollection', time: null, features: [] }

const REFRESH_MS = 30_000

async function getJson<T>(url: string): Promise<T> {
  const res = await fetch(url)
  if (!res.ok) throw new Error(`GET ${url} failed: ${res.status}`)
  return res.json()
}

export function useFlightTimestamps() {
  return useQuery({
    queryKey: ['flights', 'timestamps'],
    queryFn: () => getJson<{ timestamps: number[] }>('/api/flights/timestamps'),
    select: (data) => data.timestamps,
    refetchInterval: REFRESH_MS,
  })
}

/** `timestamp === null` means live: the latest snapshot, refreshed regularly. */
export function useFlights(timestamp: number | null, enabled: boolean) {
  const live = timestamp === null
  return useQuery({
    queryKey: ['flights', live ? 'latest' : timestamp],
    queryFn: () => getJson<FlightsSnapshot>(live ? '/api/flights/latest' : `/api/flights/${timestamp}`),
    refetchInterval: live ? REFRESH_MS : false,
    placeholderData: keepPreviousData,
    enabled,
  })
}
