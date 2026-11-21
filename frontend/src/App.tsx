import { Alert, Box, CircularProgress, Paper, Stack, Typography } from '@mui/material'
import { useState } from 'react'
import Map, { Layer, Popup, Source } from 'react-map-gl/mapbox'
import type { MapMouseEvent } from 'mapbox-gl'
import { ControlsPanel } from './components/ControlsPanel'
import { PLANES_LAYER_ID, PlanesLayer, type PlaneProps } from './components/PlanesLayer'
import { StatTile } from './components/StatTile'
import { TimelineSlider } from './components/TimelineSlider'
import { useConfig } from './config'
import { sampleHeatmap } from './sampleHeatmap'
import { emptyFlights, useFlights } from './api/flights'
import { useLayers } from './store/layers'
import { useTimeline } from './store/timeline'

type Selected = { lng: number; lat: number; plane: PlaneProps }

function MapView() {
  const { data: config, isLoading } = useConfig()
  const [selected, setSelected] = useState<Selected | null>(null)
  const visible = useLayers((s) => s.visible)
  const selectedTime = useTimeline((s) => s.selected)
  const { data: flights } = useFlights(selectedTime, visible.flights)

  const onClick = (e: MapMouseEvent) => {
    const feature = e.features?.[0]
    setSelected(feature ? { ...e.lngLat, plane: feature.properties as PlaneProps } : null)
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
      interactiveLayerIds={[PLANES_LAYER_ID]}
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

      <PlanesLayer data={flights ?? emptyFlights} visible={visible.flights} />

      {selected && visible.flights && (
        <Popup longitude={selected.lng} latitude={selected.lat} onClose={() => setSelected(null)}>
          <Typography variant="subtitle2" sx={{ color: 'black' }}>
            {selected.plane.callsign}
          </Typography>
          <Typography variant="caption" sx={{ color: 'black', display: 'block' }}>
            {selected.plane.altitude} m · {selected.plane.velocity} m/s
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
