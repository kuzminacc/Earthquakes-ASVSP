// OpenStreetMap tiles za Kepler.gl
export const MAP_STYLE_OSM = {
  version: 8,
  sources: {
    osm: {
      type: "raster",
      tiles: ["https://a.tile.openstreetmap.org/{z}/{x}/{y}.png"],
      tileSize: 256
    }
  },
  layers: [
    {
      id: "osm",
      type: "raster",
      source: "osm",
      minzoom: 0,
      maxzoom: 19
    }
  ],
  glyphs: "https://fonts.openmaptiles.org/{fontstack}/{range}.pbf"
};
