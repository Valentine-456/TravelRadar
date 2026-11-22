import { useEffect } from 'react'
import { useMap } from 'react-map-gl/mapbox'

/** Registers a white SVG as a recolorable (SDF) map icon under `id`. */
export function useMapIcon(id: string, svg: string) {
  const { current: map } = useMap()

  useEffect(() => {
    if (!map) return
    const img = new Image(48, 48)
    img.onload = () => {
      if (!map.hasImage(id)) map.addImage(id, img, { sdf: true })
    }
    img.src = `data:image/svg+xml;charset=utf-8,${encodeURIComponent(svg)}`
  }, [map, id, svg])
}
