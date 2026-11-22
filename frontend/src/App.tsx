import { Alert, Box, CircularProgress, Paper, Stack, Typography } from '@mui/material'
import { useState } from 'react'
import Map, { Layer, Popup, Source } from 'react-map-gl/mapbox'
import type { MapMouseEvent } from 'mapbox-gl'
import { ControlsPanel } from './components/ControlsPanel'
import { PLANES_LAYER_ID, PlanesLayer, type PlaneProps } from './components/PlanesLayer'
import { SHIPS_LAYER_ID, ShipsLayer, type ShipProps } from './components/ShipsLayer'
import { StatTile } from './components/StatTile'
import { TimelineSlider } from './components/TimelineSlider'
import { useConfig } from './config'
import { sampleHeatmap } from './sampleHeatmap'
import { emptyFlights, useFlights } from './api/flights'
import { emptyShips, navStatusLabel, useShips } from './api/ships'
import { useLayers } from './store/layers'
import { useTimeline } from './store/timeline'

type Selected = { lng: number; lat: number } & (
  | { kind: 'plane'; props: PlaneProps }
  | { kind: 'ship'; props: ShipProps }
)

function MapView() {
  const { data: config, isLoading } = useConfig()
  const [selected, setSelected] = useState<Selected | null>(null)
  const visible = useLayers((s) => s.visible)
  const selectedTime = useTimeline((s) => s.selected)
  const { data: flights } = useFlights(selectedTime, visible.flights)
  const { data: ships } = useShips(selectedTime, visible.ships)

  const onClick = (e: MapMouseEvent) => {
    const feature = e.features?.[0]
    if (!feature) return setSelected(null)
    setSelected(
      feature.layer?.id === SHIPS_LAYER_ID
        ? { ...e.lngLat, kind: 'ship', props: feature.properties as ShipProps }
        : { ...e.lngLat, kind: 'plane', props: feature.properties as PlaneProps },
    )
  }

  if (isLoading) {
    return (
      <Box sx={{ position: 'absolute', inset: 0, display: 'grid', placeItems: 'center' }}>
        <CircularProgress />
      </Box>
    )
  }

  if (!config?.mapboxToken) {
    return (
      <Box sx={{ position: 'absolute', inset: 0, display: 'grid', placeItems: 'center' }}>
        <Alert severity="warning">Mapbox token missing. Set MAPBOX_TOKEN on the backend or VITE_MAPBOX_TOKEN in .env.local.</Alert>
      </Box>
    )
  }

  return (
    <Map
      mapboxAccessToken={config.mapboxToken}
      initialViewState={{ longitude: 18, latitude: 62, zoom: 4 }}
      mapStyle={config.mapboxStyle}
      style={{ position: 'absolute', inset: 0 }}
      interactiveLayerIds={[PLANES_LAYER_ID, SHIPS_LAYER_ID]}
      onClick={onClick}
    >
      <Source id="searches" type="geojson" data={sampleHeatmap}>
        <Layer
          id="searches-heat"
          type="heatmap"
          layout={{ visibility: visible.heatmap ? 'visible' : 'none' }}
          paint={{
            'heatmap-weight': ['interpolate', ['linear'], ['get', 'count'], 0, 0, 10, 1],
            'heatmap-intensity': ['interpolate', ['linear'], ['zoom'], 0, 1, 9, 3],
            'heatmap-radius': ['interpolate', ['linear'], ['zoom'], 0, 4, 9, 30],
            'heatmap-opacity': 0.8,
          }}
        />
      </Source>

      <ShipsLayer data={ships ?? emptyShips} visible={visible.ships} />
      <PlanesLayer data={flights ?? emptyFlights} visible={visible.flights} />

      {selected && visible[selected.kind === 'plane' ? 'flights' : 'ships'] && (
        <Popup longitude={selected.lng} latitude={selected.lat} onClose={() => setSelected(null)}>
          <Typography variant="subtitle2" sx={{ color: 'black' }}>
            {selected.kind === 'plane' ? selected.props.callsign : selected.props.name}
          </Typography>
          <Typography variant="caption" sx={{ color: 'black', display: 'block' }}>
            {selected.kind === 'plane'
              ? `${selected.props.altitude} m · ${selected.props.velocity} m/s`
              : `${selected.props.speed} kn · ${navStatusLabel(selected.props.navStatus)}`}
          </Typography>
        </Popup>
      )}
    </Map>
  )
}

export default function App() {
  return (
    <Box sx={{ position: 'relative', width: '100vw', height: '100vh', overflow: 'hidden' }}>
      <MapView />

      <Paper elevation={8} sx={{ position: 'absolute', top: 16, left: 16, px: 2.5, py: 1.5 }}>
        <Typography variant="h6" sx={{ fontWeight: 700 }}>
          TravelRadar
        </Typography>
      </Paper>

      <Stack spacing={2} sx={{ position: 'absolute', top: 88, left: 16 }}>
        <StatTile label="Events / sec" value="1,284" delta="+12%" />
        <StatTile label="Active routes" value="342" delta="+4%" />
        <StatTile label="Avg. price" value="€187" delta="-3%" />
      </Stack>

      <Box sx={{ position: 'absolute', top: 16, right: 16 }}>
        <ControlsPanel />
      </Box>

      <Box sx={{ position: 'absolute', bottom: 24, left: '50%', transform: 'translateX(-50%)' }}>
        <TimelineSlider />
      </Box>
    </Box>
  )
}
