# fetch_overture

Fetch Overture Maps data (Building footprints, Boundaries, Places...) for regions by text input or by Overture Maps division id. Supports GeoJSON/GeoParquet output.

![Manhattan](manhattan.jpg?raw=true)

As simple as:

```
  npx fetch_overture manhattan_buildings.geojson --theme=buildings --type=building --location=Manhattan,NY
```

### Requirements

Install **Node.js**:

https://nodejs.org

### Usage:

```
npx fetch_overture manhattan_buildings.zstd.parquet --theme=buildings --type=building --location=Manhattan,NY
```

or by division id (Manhattan)

```
npx fetch_overture manhattan_buildings.zstd.parquet --theme=buildings --type=building --division_id=0856cf5cbfffffff01a25549e3bf0e4c
```

**GeoJSON** format:

```
npx fetch_overture manhattan_buildings.geojson --theme=buildings --type=building --division_id=0856cf5cbfffffff01a25549e3bf0e4c
```

### Overture themes and types:

```
  npx fetch_overture manhattan_buildings.zstd.parquet --theme=<theme> --type=<type> --location=Manhattan,NY
```

- **Theme:** "addresses"

  - **type:**
    - address

- **Theme:** "base"

  - **type:**
    - infrastructure
    - land
    - land_cover
    - land_use
    - water

- **Theme:** "buildings"

  - **type:**
    - building
    - building_part

- **Theme:** "divisions"

  - **type:**
    - division
    - division_area
    - division_boundary

- **Theme:** "places"

  - **type:**
    - place

- **Theme:** "transportation"
  - **type:**
    - connector
    - segment
