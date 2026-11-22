import type { PlaneProps } from '../components/PlanesLayer'
import { emptySnapshot, useSnapshot, useSnapshotTimestamps, type Snapshot } from './snapshots'

export type FlightsSnapshot = Snapshot<PlaneProps>

export const emptyFlights: FlightsSnapshot = emptySnapshot()

export const useFlightTimestamps = () => useSnapshotTimestamps('flights')

export const useFlights = (timestamp: number | null, enabled: boolean) =>
  useSnapshot<PlaneProps>('flights', timestamp, enabled)
