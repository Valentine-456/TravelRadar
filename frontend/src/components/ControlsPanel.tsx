import {
  Divider,
  FormControlLabel,
  Paper,
  Slider,
  Stack,
  Switch,
  ToggleButton,
  ToggleButtonGroup,
  Typography,
} from '@mui/material'
import { useState } from 'react'

export function ControlsPanel() {
  const [range, setRange] = useState('24h')
  const [layers, setLayers] = useState({ flights: true, heatmap: false, prices: true })
  const [minPrice, setMinPrice] = useState(50)

  const toggleLayer = (key: keyof typeof layers) =>
    setLayers((prev) => ({ ...prev, [key]: !prev[key] }))

  return (
    <Paper elevation={8} sx={{ p: 2, width: 280 }}>
      <Typography variant="subtitle2" gutterBottom>
        Time range
      </Typography>
      <ToggleButtonGroup
        value={range}
        exclusive
        size="small"
        fullWidth
        onChange={(_, value) => value && setRange(value)}
      >
        <ToggleButton value="1h">1h</ToggleButton>
        <ToggleButton value="24h">24h</ToggleButton>
        <ToggleButton value="7d">7d</ToggleButton>
      </ToggleButtonGroup>

      <Divider sx={{ my: 2 }} />

      <Typography variant="subtitle2">Layers</Typography>
      <Stack>
        {(Object.keys(layers) as (keyof typeof layers)[]).map((key) => (
          <FormControlLabel
            key={key}
            control={<Switch size="small" checked={layers[key]} onChange={() => toggleLayer(key)} />}
            label={key[0].toUpperCase() + key.slice(1)}
          />
        ))}
      </Stack>

      <Divider sx={{ my: 2 }} />

      <Typography variant="subtitle2">Min price: €{minPrice}</Typography>
      <Slider
        value={minPrice}
        min={0}
        max={1000}
        step={10}
        onChange={(_, value) => setMinPrice(value as number)}
      />
    </Paper>
  )
}
