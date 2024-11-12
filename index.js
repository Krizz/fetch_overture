#! /usr/bin/env node

import duckdb from 'duckdb';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const db = new duckdb.Database(':memory:');
const LAST_KNOWN_OVERTURE_VERSION = '2024-10-23.0';

const isGERSId = str => /^[a-f0-9]{32}$/.test(str);

const fetchLatestOvertureVersion = async () => {
  try {
    const url = 'https://docs.overturemaps.org/getting-data/';
    const response = await fetch(url, {
      signal: AbortSignal.timeout(15000)
    });
    const html = await response.text();
    const match = html.match(/<span\s+class="token plain">([^<]*)\/<\/span>/);

    if (match) {
      return match[1];
    } else {
      throw new Error('No overture version found');
    }
  } catch (error) {
    console.error(
      `Error fetching overture version, falling back to last known version:${LAST_KNOWN_OVERTURE_VERSION}`
    );
    return LAST_KNOWN_OVERTURE_VERSION;
  }
};

const OVERTURE_VERSION =
  process.env.OVERTURE_VERSION ?? (await fetchLatestOvertureVersion());
console.log(`Overture version: ${OVERTURE_VERSION}`);

const queryDuckDB = sql =>
  new Promise((resolve, reject) => {
    db.all(sql, (err, results) => {
      if (err) {
        return reject(err);
      }

      return resolve(results);
    });
  });

const describeParquet = parquetPath =>
  queryDuckDB(`DESCRIBE SELECT * FROM '${parquetPath}';`);

const getBBox = async geoJSONString => {
  const sql = `
    INSTALL spatial;LOAD spatial;
    WITH bounds AS (
      SELECT
      ST_Envelope(ST_GeomFromGeoJSON('${geoJSONString}')) AS envelope
    )
    SELECT
    ST_XMin(envelope) AS xmin,
    ST_YMin(envelope) AS ymin,
    ST_XMax(envelope) AS xmax,
    ST_YMax(envelope) AS ymax
    FROM bounds;
  `;

  const results = await queryDuckDB(sql);
  return results[0];
};

export const createParquetExtract = async (
  geoJSONString,
  theme,
  type,
  filePath
) => {
  const bbox = await getBBox(geoJSONString);
  const parquetPath = `s3://overturemaps-us-west-2/release/${OVERTURE_VERSION}/theme=${theme}/type=${type}/*.parquet`;

  const schema = await describeParquet(parquetPath);

  const outputIsGeoJSON = filePath.endsWith('.geojson');
  const settings = outputIsGeoJSON
    ? `(FORMAT GDAL, DRIVER 'GeoJSON', SRS 'EPSG:4326')`
    : `(FORMAT 'parquet', COMPRESSION 'zstd')`;

  const getGeoJSONSelectFromSchema = schema => {
    const geojsonUnsupportedColumns = schema
      .filter(
        column =>
          column.column_type.startsWith('STRUCT') ||
          column.column_type.startsWith('MAP') ||
          column.column_type.startsWith('VARCHAR[')
      )
      .map(column => column.column_name);

    const geojsonExclude = ` EXCLUDE (geometry, ${geojsonUnsupportedColumns.join(
      ', '
    )})`;

    const geojsonSelect = geojsonUnsupportedColumns
      .map(column => `${column}::json AS ${column}`)
      .join(', ');

    const select = `
        * ${geojsonExclude},
        geometry,
        ${geojsonSelect}
      `;

    return select;
  };

  const select = outputIsGeoJSON ? getGeoJSONSelectFromSchema(schema) : '*';

  const sql = `
      INSTALL spatial;LOAD spatial;SET preserve_insertion_order=false;
      COPY (
        SELECT
        ${select}
        FROM read_parquet('${parquetPath}')
        WHERE
        bbox.xmin BETWEEN ${bbox.xmin} AND ${bbox.xmax} AND
        bbox.ymin BETWEEN  ${bbox.ymin} AND  ${bbox.ymax}
        AND st_intersects(geometry, ST_GeomFromGeoJSON('${geoJSONString}'))

      ) TO '${filePath}' ${settings};
    `;

  await queryDuckDB(sql);
  return filePath;
};

const geocode = address => {
  const url = `https://nominatim.openstreetmap.org/search?q=${address}&format=jsonv2&limit=1&polygon_geojson=1`;
  return fetch(url, {
    headers: {
      'accept-language': 'en'
    }
  }).then(response => response.json());
};

export const findDivision = async query => {
  const geocodeResults = await geocode(query);
  if (!geocodeResults.length) {
    return null;
  }

  const geocodeResult = geocodeResults[0];
  const geojson = geocodeResult.geojson;

  return {
    name: geocodeResults[0].display_name,
    geometry_geojson: JSON.stringify(geojson)
  };
};

export const getDivisionById = async id => {
  const sql = `
    INSTALL spatial;
    LOAD spatial;
    SELECT
    id,
    COALESCE(names.common['en'][1], names.primary) as name,
    bbox,
    subtype,
    class,
    ST_ASGeoJSON(geometry) AS geometry_geojson
    FROM read_parquet('s3://overturemaps-us-west-2/release/${OVERTURE_VERSION}/theme=divisions/type=division_area/*.parquet')
    WHERE id = '${id}'
    LIMIT 1;
  `;
  const results = await queryDuckDB(sql);
  return results[0];
};

const parseArgs = () => {
  const args = process.argv.slice(3);
  return Object.fromEntries(
    args
      .filter(arg => arg.startsWith('--'))
      .map(arg => {
        const [key, value] = arg.slice(2).split('=');
        return [key, value];
      })
  );
};

const filePath = process.argv[2];

const runCLI = async () => {
  const args = parseArgs();
  const { theme, division_id, location, layer } = args;
  const type = args.type || layer; // type is an alias for layer

  if (!theme) {
    console.error('Missing argument: --theme=<theme>');
    process.exit(1);
  }

  if (!type) {
    console.error('Missing argument: --type=<type>');
    process.exit(1);
  }

  if (!division_id && !location) {
    console.error(
      'Missing argument: --division_id=<id> or --location=<address>'
    );
    process.exit(1);
  }

  console.log(`Creating extract for ${theme}/${type}`);

  const division = location
    ? await findDivision(location)
    : await getDivisionById(division_id);

  if (!division) {
    console.error(`Division "${location || division_id}" not found`);
    process.exit(1);
  }

  console.log(`Found division: ${division.name}`);
  console.log(`Creating extract for ${division.name}`);
  await createParquetExtract(division.geometry_geojson, theme, type, filePath);
  console.log(`Created extract for ${division.name} at ${filePath}`);
  db.close();
};

const fetchOverture = async ({ location, theme, type, outputFilePath }) => {
  if (!location) {
    throw new Error('Missing location');
  }

  if (!theme) {
    throw new Error('Missing Overture theme (buildings, divisions, etc.)');
  }

  if (!type) {
    throw new Error(
      'Missing Overeture type (building, building_part, division, etc.)'
    );
  }

  if (!outputFilePath) {
    throw new Error('Missing output file path');
  }

  const isGERS = isGERSId(location);
  const division = isGERS
    ? await getDivisionById(location)
    : await findDivision(location);

  if (!division) {
    console.error(`Division "${location}" not found`);
    return null;
  }

  await createParquetExtract(
    division.geometry_geojson,
    theme,
    type,
    outputFilePath
  );

  return {
    name: division.name,
    filePath: outputFilePath,
    division
  };
};

const indexFilePath = resolve(fileURLToPath(import.meta.url));
const isCLI = indexFilePath.includes(process.argv[1]);
const isNpx = process.env._ && process.env._.includes('npx');

if (isCLI || isNpx) {
  runCLI();
}

export default fetchOverture;
