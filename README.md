# fetch_overture

Fetch overture maps data

### Usage:

```
npx fetch-overture manhattan_buildings.zstd.parquet --theme=buildings --layer=building --location=Manhattan,NY
```

or by division id (Manhattan)

```
npx fetch-overture manhattan_buildings.zstd.parquet --theme=buildings --layer=building --division_id=0856cf5cbfffffff01a25549e3bf0e4c
```

**GeoJSON** format:

```
npx fetch-overture manhattan_buildings.geojson --theme=buildings --layer=building --division_id=0856cf5cbfffffff01a25549e3bf0e4c
```

### Overture themes and layers:

```
  npx fetch-overture manhattan_buildings.zstd.parquet --theme=<theme> --layer=<layer> --location=Manhattan,NY
```

- **Theme:** "addresses"

  - **Layer:**
    - address

- **Theme:** "base"

  - **Layer:**
    - infrastructure
    - land
    - land_cover
    - land_use
    - water

- **Theme:** "buildings"

  - **Layer:**
    - building
    - building_part

- **Theme:** "divisions"

  - **Layer:**
    - division
    - division_area
    - division_boundary

- **Theme:** "places"

  - **Layer:**
    - place

- **Theme:** "transportation"
  - **Layer:**
    - connector
    - segment
