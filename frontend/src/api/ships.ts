import type { ShipProps } from '../components/ShipsLayer'
import { emptySnapshot, useSnapshot, type Snapshot } from './snapshots'

export type ShipsSnapshot = Snapshot<ShipProps>

export const emptyShips: ShipsSnapshot = emptySnapshot()

/** Ships for the slider's timestamp; the backend returns the closest earlier 5-minute snapshot. */
export const useShips = (timestamp: number | null, enabled: boolean) =>
  useSnapshot<ShipProps>('ships', timestamp, enabled)

const NAV_STATUS: Record<number, string> = {
  0: 'Under way',
  1: 'At anchor',
  2: 'Not under command',
  3: 'Restricted manoeuvrability',
  5: 'Moored',
  7: 'Fishing',
  8: 'Sailing',
}

export const navStatusLabel = (status: number | null) =>
  status === null ? 'Unknown' : (NAV_STATUS[status] ?? 'Other')
