import { create } from 'zustand'

type TimelineState = {
  /** Selected snapshot timestamp, or `null` for live (always the latest). */
  selected: number | null
  setSelected: (timestamp: number | null) => void
}

export const useTimeline = create<TimelineState>((set) => ({
  selected: null,
  setSelected: (selected) => set({ selected }),
}))
