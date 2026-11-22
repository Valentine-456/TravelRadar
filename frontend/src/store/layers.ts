import { create } from 'zustand'

export type LayerKey = 'flights' | 'ships' | 'heatmap'

type LayersState = {
  visible: Record<LayerKey, boolean>
  toggle: (key: LayerKey) => void
}

export const useLayers = create<LayersState>((set) => ({
  visible: { flights: true, ships: true, heatmap: true },
  toggle: (key) => set((s) => ({ visible: { ...s.visible, [key]: !s.visible[key] } })),
}))
