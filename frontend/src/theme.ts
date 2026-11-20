import { createTheme } from '@mui/material/styles'

export const theme = createTheme({
  palette: {
    mode: 'dark',
    primary: { main: '#4dabf7' },
    background: { default: '#0b0f14', paper: 'rgba(20, 26, 34, 0.85)' },
  },
  shape: { borderRadius: 12 },
  components: {
    MuiPaper: {
      styleOverrides: {
        root: {
          backdropFilter: 'blur(8px)',
          backgroundImage: 'none',
        },
      },
    },
  },
})
