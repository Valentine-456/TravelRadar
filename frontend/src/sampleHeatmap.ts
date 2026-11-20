import type { FeatureCollection, Point } from 'geojson'

const cities: [number, number, number][] = [
  [2.35, 48.86, 90],
  [-0.13, 51.51, 100],
  [13.4, 52.52, 70],
  [12.5, 41.9, 80],
  [-3.7, 40.42, 65],
  [2.17, 41.39, 75],
  [4.9, 52.37, 55],
  [16.37, 48.21, 45],
  [14.42, 50.09, 50],
  [23.73, 37.98, 40],
  [28.98, 41.01, 60],
  [21.01, 52.23, 35],
  [-9.14, 38.72, 45],
]

function randomAround(lng: number, lat: number, spread: number): [number, number] {
  return [lng + (Math.random() - 0.5) * spread, lat + (Math.random() - 0.5) * spread]
}

export const sampleHeatmap: FeatureCollection<Point, { count: number }> = {
  type: 'FeatureCollection',
  features: cities.flatMap(([lng, lat, weight]) =>
    Array.from({ length: weight }, () => ({
      type: 'Feature' as const,
      geometry: { type: 'Point' as const, coordinates: randomAround(lng, lat, 1.5) },
      properties: { count: Math.random() * 10 },
    })),
  ),
}
