#! /usr/bin/env node

import duckdb from "duckdb";
import * as turf from "@turf/turf";

const db = new duckdb.Database(":memory:");
const OVERTURE_VERSION = process.env.OVERTURE_VERSION ?? "2024-08-20.0";

const createParquetExtract = (
  divisionId,
  geoJSONString,
  theme,
  type,
  filePath
) =>
  new Promise((resolve, reject) => {
    const bbox = turf.bbox(JSON.parse(geoJSONString));

    const sql = `
      INSTALL spatial;LOAD spatial;SET preserve_insertion_order=false;
      COPY (
        SELECT
        *
        FROM read_parquet('s3://overturemaps-us-west-2/release/${OVERTURE_VERSION}/theme=${theme}/type=${type}/*.parquet')
        WHERE
        bbox.xmin BETWEEN ${bbox[0]} AND ${bbox[2]} AND
        bbox.ymin BETWEEN  ${bbox[1]} AND  ${bbox[3]}
        AND st_intersects(ST_GeomFromWKB(geometry), ST_GeomFromGeoJSON('${geoJSONString}'))

      ) TO '${filePath}' (FORMAT 'parquet', COMPRESSION 'zstd');
    `;

    db.all(sql, (err, results) => {
      if (err) {
        return reject(err);
      }

      return resolve(filePath);
    });
  });

const geocode = (address) => {
  const url = `https://nominatim.openstreetmap.org/search?q=${address}&format=jsonv2&limit=1&polygon_geojson=1`;
  return fetch(url).then((response) => response.json());
};

const getDivision = (name, bbox) =>
  new Promise((resolve, reject) => {
    const sql = `
      INSTALL spatial;LOAD spatial;SET preserve_insertion_order=false;
      SELECT
      id,
      COALESCE(names.common['en'][1], names.primary) as name,
      bbox,
      subtype,
      class,
      ST_ASGeoJSON(ST_GeomFromWKB(geometry)) AS geometry_geojson
      FROM read_parquet('s3://overturemaps-us-west-2/release/2024-07-22.0/theme=divisions/type=division_area/*.parquet')
      WHERE
      bbox.xmin BETWEEN ${bbox[0]} AND ${bbox[2]} AND
      bbox.ymin BETWEEN  ${bbox[1]} AND  ${bbox[3]}
      AND
      st_intersects(
        st_envelope(ST_GeomFromWKB(geometry)),
        ST_MakeEnvelope(${bbox})
      )
      ORDER BY jaro_similarity(COALESCE(names.common['en'][1], names.primary), '${name}') DESC
      LIMIT 1;
    `;

    db.all(sql, (err, results) => {
      if (err) {
        return reject(err);
      }

      return resolve(results[0]);
    });
  });

const findDivision = async (query) => {
  const geocodeResults = await geocode(query);
  if (!geocodeResults.length) {
    return null;
  }
  const geocodeResult = geocodeResults[0];
  const geojson = geocodeResult.geojson;
  const nominatimBoundingBox = geocodeResult.boundingbox;
  const bbox = turf.bbox(geojson);
  const bboxPolygon = turf.bboxPolygon(bbox);
  const center = turf.center(bboxPolygon);
  const doubledPolygon = turf.transformScale(bboxPolygon, 2, {
    origin: center,
  });
  const newBbox = turf.bbox(doubledPolygon);

  const name = geocodeResult.name;
  const division = await getDivision(name, newBbox);
  return division;
};

const getDivisionById = (id) =>
  new Promise((resolve, reject) => {
    const sql = `
      INSTALL spatial;
      LOAD spatial;
      SELECT
      id,
      COALESCE(names.common['en'][1], names.primary) as name,
      bbox,
      subtype,
      class,
      ST_ASGeoJSON(ST_GeomFromWKB(geometry)) AS geometry_geojson
      FROM read_parquet('s3://overturemaps-us-west-2/release/2024-07-22.0/theme=divisions/type=division_area/*.parquet')
      WHERE id = '${id}'
      LIMIT 1;
    `;

    db.all(sql, (err, results) => {
      if (err) {
        return reject(err);
      }

      return resolve(results[0]);
    });
  });

const parseArgs = () => {
  const args = process.argv.slice(3);
  return Object.fromEntries(
    args
      .filter((arg) => arg.startsWith("--"))
      .map((arg) => {
        const [key, value] = arg.slice(2).split("=");
        return [key, value];
      })
  );
};

const filePath = process.argv[2];

const { layer, theme, division_id, location } = parseArgs();

if (!theme) {
  console.error("Missing argument: --theme=<theme>");
  process.exit(1);
}

if (!layer) {
  console.error("Missing argument: --layer=<layer>");
  process.exit(1);
}

if (!division_id && !location) {
  console.error("Missing argument: --division_id=<id> or --location=<address>");
  process.exit(1);
}

console.log(`Creating parquet extract for ${layer}/${theme}`);

const division = location
  ? await findDivision(location)
  : await getDivisionById(division_id);

if (!division) {
  console.error(`Division "${location || division_id}" not found`);
  process.exit(1);
}

console.log(`Found division: ${division.name}`);

console.log(`Creating parquet extract for ${division.name}`);
await createParquetExtract(
  division.id,
  division.geometry_geojson,
  theme,
  layer,
  filePath
);
console.log(`Created parquet extract for ${division.name} at ${filePath}`);
db.close();
