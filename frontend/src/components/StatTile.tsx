import { Paper, Typography } from '@mui/material'

type Props = {
  label: string
  value: string
  delta?: string
}

export function StatTile({ label, value, delta }: Props) {
  return (
    <Paper elevation={8} sx={{ p: 2, width: 220 }}>
      <Typography variant="caption" color="text.secondary">
        {label}
      </Typography>
      <Typography variant="h5" sx={{ fontWeight: 600 }}>
        {value}
      </Typography>
      {delta && (
        <Typography variant="caption" color={delta.startsWith('-') ? 'error.main' : 'success.main'}>
          {delta}
        </Typography>
      )}
    </Paper>
  )
}
