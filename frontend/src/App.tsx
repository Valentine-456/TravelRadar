import { Alert, Box, CircularProgress, Paper, Stack, Typography } from '@mui/material'
import Map, { Layer, Source } from 'react-map-gl/mapbox'
import { ControlsPanel } from './components/ControlsPanel'
import { StatTile } from './components/StatTile'
import { useConfig } from './config'
import { sampleHeatmap } from './sampleHeatmap'

function MapView() {
  const { data: config, isLoading } = useConfig()

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
      initialViewState={{ longitude: 15, latitude: 48, zoom: 3.5 }}
      mapStyle={config.mapboxStyle}
      style={{ position: 'absolute', inset: 0 }}
    >
      <Source id="searches" type="geojson" data={sampleHeatmap}>
        <Layer
          id="searches-heat"
          type="heatmap"
          paint={{
            'heatmap-weight': ['interpolate', ['linear'], ['get', 'count'], 0, 0, 10, 1],
            'heatmap-intensity': ['interpolate', ['linear'], ['zoom'], 0, 1, 9, 3],
            'heatmap-radius': ['interpolate', ['linear'], ['zoom'], 0, 4, 9, 30],
            'heatmap-opacity': 0.8,
          }}
        />
      </Source>
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
    </Box>
  )
}
