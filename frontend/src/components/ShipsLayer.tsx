import type { FeatureCollection, Point } from 'geojson'
import { Layer, Source } from 'react-map-gl/mapbox'
import { useMapIcon } from './useMapIcon'

export type ShipProps = {
  mmsi: number
  name: string
  speed: number
  heading: number | null
  navStatus: number | null
}

export type Ships = FeatureCollection<Point, ShipProps>

export const SHIPS_LAYER_ID = 'ships'

const SHIP_SVG = `<svg xmlns="http://www.w3.org/2000/svg" width="48" height="48" viewBox="0 0 24 24"><path fill="#fff" d="M12 2 19 21 12 17 5 21z"/></svg>`

export function ShipsLayer({ data, visible = true }: { data: Ships; visible?: boolean }) {
  useMapIcon('ship', SHIP_SVG)

  return (
    <Source id="ships" type="geojson" data={data}>
      <Layer
        id={SHIPS_LAYER_ID}
        type="symbol"
        layout={{
          visibility: visible ? 'visible' : 'none',
          'icon-image': 'ship',
          'icon-size': ['interpolate', ['linear'], ['zoom'], 3, 0.25, 9, 0.6],
          'icon-rotate': ['coalesce', ['get', 'heading'], 0],
          'icon-rotation-alignment': 'map',
          'icon-allow-overlap': true,
          'text-field': ['step', ['zoom'], '', 8, ['get', 'name']],
          'text-size': 10,
          'text-offset': [0, 1.4],
        }}
        paint={{
          'icon-color': '#38d9a9',
          'text-color': '#ffffff',
          'text-halo-color': '#000000',
          'text-halo-width': 1,
        }}
      />
    </Source>
  )
}
