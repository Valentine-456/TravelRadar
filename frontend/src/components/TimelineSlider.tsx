import { Button, Paper, Slider, Stack, Typography } from '@mui/material'
import { useState } from 'react'
import { useFlightTimestamps } from '../api/flights'
import { useTimeline } from '../store/timeline'

function formatTime(timestamp: number | undefined) {
  if (timestamp === undefined) return '–'
  return new Date(timestamp * 1000).toLocaleString([], {
    day: '2-digit',
    month: 'short',
    hour: '2-digit',
    minute: '2-digit',
  })
}

export function TimelineSlider() {
  const { data: timestamps = [] } = useFlightTimestamps()
  const { selected, setSelected } = useTimeline()
  const [dragIndex, setDragIndex] = useState<number | null>(null)

  const last = timestamps.length - 1
  const live = selected === null
  const selectedIndex = live ? last : Math.max(0, timestamps.indexOf(selected))
  const index = dragIndex ?? selectedIndex

  const commit = (i: number) => {
    setDragIndex(null)
    setSelected(i >= last ? null : timestamps[i])
  }

  return (
    <Paper elevation={8} sx={{ px: 3, py: 1.5, width: 'min(720px, calc(100vw - 32px))' }}>
      <Stack direction="row" spacing={2} sx={{ alignItems: 'center' }}>
        <Button
          size="small"
          variant={live ? 'contained' : 'outlined'}
          color={live ? 'error' : 'primary'}
          onClick={() => setSelected(null)}
        >
          Live
        </Button>
        <Slider
          min={0}
          max={Math.max(last, 0)}
          step={1}
          value={Math.max(index, 0)}
          disabled={timestamps.length < 2}
          onChange={(_, value) => setDragIndex(value as number)}
          onChangeCommitted={(_, value) => commit(value as number)}
          valueLabelDisplay="auto"
          valueLabelFormat={(i) => formatTime(timestamps[i])}
        />
        <Typography variant="body2" sx={{ minWidth: 120, fontVariantNumeric: 'tabular-nums' }}>
          {timestamps.length ? formatTime(timestamps[index]) : 'No data yet'}
        </Typography>
      </Stack>
    </Paper>
  )
}
