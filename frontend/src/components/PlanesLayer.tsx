import { Layer, Source } from 'react-map-gl/mapbox'
import type { FeatureCollection, Point } from 'geojson'
import { useMapIcon } from './useMapIcon'

export type PlaneProps = {
  icao24: string
  callsign: string
  track: number
  altitude: number
  velocity: number
}

export type Planes = FeatureCollection<Point, PlaneProps>

export const PLANES_LAYER_ID = 'planes'

const PLANE_SVG = `<svg xmlns="http://www.w3.org/2000/svg" width="48" height="48" viewBox="0 0 24 24"><path fill="#fff" d="M21 16v-2l-8-5V3.5a1.5 1.5 0 0 0-3 0V9l-8 5v2l8-2.5V19l-2 1.5V22l3.5-1 3.5 1v-1.5L13 19v-5.5z"/></svg>`

export function PlanesLayer({ data, visible = true }: { data: Planes; visible?: boolean }) {
  useMapIcon('plane', PLANE_SVG)

  return (
    <Source id="planes" type="geojson" data={data}>
      <Layer
        id={PLANES_LAYER_ID}
        type="symbol"
        layout={{
          visibility: visible ? 'visible' : 'none',
          'icon-image': 'plane',
          'icon-size': ['interpolate', ['linear'], ['zoom'], 3, 0.35, 8, 0.8],
          'icon-rotate': ['get', 'track'],
          'icon-rotation-alignment': 'map',
          'icon-allow-overlap': true,
          'text-field': ['step', ['zoom'], '', 6, ['get', 'callsign']],
          'text-size': 11,
          'text-offset': [0, 1.6],
        }}
        paint={{
          'icon-color': '#ffd43b',
          'text-color': '#ffffff',
          'text-halo-color': '#000000',
          'text-halo-width': 1,
        }}
      />
    </Source>
  )
}
