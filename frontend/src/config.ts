import { useQuery } from '@tanstack/react-query'

export type AppConfig = {
  mapboxToken: string
}

async function fetchConfig(): Promise<AppConfig> {
  try {
    const res = await fetch('/api/config')
    if (res.ok) return await res.json()
  } catch {
    // backend not running, fall back to the dev .env value
  }
  return { mapboxToken: import.meta.env.VITE_MAPBOX_TOKEN ?? '' }
}

export function useConfig() {
  return useQuery({ queryKey: ['config'], queryFn: fetchConfig, staleTime: Infinity })
}
