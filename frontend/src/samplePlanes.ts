import type { Planes } from './components/PlanesLayer'

const airlines = ['SAS', 'NAX', 'FIN', 'BRX', 'WIF', 'ICE', 'DLH', 'KLM']

export const samplePlanes: Planes = {
  type: 'FeatureCollection',
  features: Array.from({ length: 40 }, (_, i) => ({
    type: 'Feature' as const,
    geometry: {
      type: 'Point' as const,
      coordinates: [5 + Math.random() * 26, 55 + Math.random() * 15],
    },
    properties: {
      icao24: (0x4a0000 + i).toString(16),
      callsign: `${airlines[i % airlines.length]}${100 + i}`,
      track: Math.random() * 360,
      altitude: Math.round(3000 + Math.random() * 9000),
      velocity: Math.round(180 + Math.random() * 80),
    },
  })),
}
