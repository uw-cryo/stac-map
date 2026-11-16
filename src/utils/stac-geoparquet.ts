import { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";
import { data, io } from "@geoarrow/geoarrow-js";
import {
  Binary,
  Data,
  makeData,
  makeVector,
  Table,
  vectorFromArray,
} from "apache-arrow";
import { toaster } from "../components/ui/toaster";
import type { StacItemCollection } from "../types/stac";
import {
  parseMultiPolygonWkb,
  type MultiPolygonData,
} from "./custom-wkb-parser";
import { loadStacWasm } from "./stac-wasm";

const SUPPORTED_GEOMETRY_TYPES = [
  "point",
  "polygon",
  "linestring",
  "multipolygon",
] as const;

async function executeDuckdbQuery({
  connection,
  select,
  href,
  where,
  hivePartitioning,
}: {
  connection: AsyncDuckDBConnection;
  select: string;
  href: string;
  where?: string;
  hivePartitioning: boolean;
}): Promise<Table> {
  let query = `SELECT ${select} FROM read_parquet('${href}', hive_partitioning = ${hivePartitioning})`;
  if (where) {
    query += ` WHERE ${where}`;
  }
  return (await connection.query(query)) as unknown as Table;
}
export type SupportedGeometryType = (typeof SUPPORTED_GEOMETRY_TYPES)[number];

export async function fetchStacGeoparquetValue({
  href,
  connection,
  hivePartitioning,
}: {
  href: string;
  connection: AsyncDuckDBConnection;
  hivePartitioning: boolean;
}): Promise<StacItemCollection> {
  const { count, bbox } = await fetchStacGeoparquetSummary({
    href,
    connection,
    hivePartitioning,
  });
  const datetimeColumns = await getDatetimeColumns({
    href,
    connection,
    hivePartitioning,
  });
  const datetimeExtent = datetimeColumns
    ? await fetchStacGeoparquetDatetimeExtent({
        href,
        connection,
        hivePartitioning,
        datetimeColumns,
      })
    : null;
  return {
    type: "FeatureCollection",
    id: href.split("/").pop(),
    description: `A stac-geoparquet file with ${count} item${count === 1 ? "" : "s"}`,
    bbox,
    features: [],
    datetimeExtent,
    datetimeColumns,
    assets: {
      data: {
        href: href,
        type: "application/vnd.apache.parquet",
      },
    },
  };
}

async function fetchStacGeoparquetSummary({
  href,
  connection,
  hivePartitioning,
}: {
  href: string;
  connection: AsyncDuckDBConnection;
  hivePartitioning: boolean;
}): Promise<{ count: number; bbox: [number, number, number, number] }> {
  const bboxType = await getBboxColumnType({
    href,
    connection,
    hivePartitioning,
  });

  if (bboxType?.startsWith("STRUCT")) {
    const result = await executeDuckdbQuery({
      connection,
      href,
      hivePartitioning,
      select:
        "COUNT(*) as count, MIN(bbox.xmin) as xmin, MIN(bbox.ymin) as ymin, MAX(bbox.xmax) as xmax, MAX(bbox.ymax) as ymax",
    });
    const row = result.toArray().map((row) => row.toJSON())[0];
    return {
      count: row.count,
      bbox: [row.xmin, row.ymin, row.xmax, row.ymax],
    };
  }

  if (bboxType === "DOUBLE[]") {
    toaster.create({
      title: "Non-spec stac-geoparquet",
      description:
        "The 'bbox' column is a list of doubles, but the stac-geoparquet spec requires a struct of {xmin, ymin, xmax, ymax}. Reading bbox values from the list instead.",
      type: "warning",
    });
    const result = await executeDuckdbQuery({
      connection,
      href,
      hivePartitioning,
      select:
        "COUNT(*) as count, MIN(bbox[1]) as xmin, MIN(bbox[2]) as ymin, MAX(bbox[3]) as xmax, MAX(bbox[4]) as ymax",
    });
    const row = result.toArray().map((row) => row.toJSON())[0];
    return {
      count: row.count,
      bbox: [row.xmin, row.ymin, row.xmax, row.ymax],
    };
  }

  if (bboxType) {
    toaster.create({
      title: "Non-spec stac-geoparquet",
      description: `The 'bbox' column has type '${bboxType}', but the stac-geoparquet spec requires a struct of {xmin, ymin, xmax, ymax}. Computing the extent from the geometry column instead.`,
      type: "warning",
    });
  }

  const result = await executeDuckdbQuery({
    connection,
    href,
    hivePartitioning,
    select: "COUNT(*) as count, ST_Extent_Agg(geometry) as extent",
  });
  const row = result.toArray().map((row) => row.toJSON())[0];
  const extent = row.extent;
  return {
    count: row.count,
    bbox: [extent.min_x, extent.min_y, extent.max_x, extent.max_y],
  };
}

async function getBboxColumnType({
  href,
  connection,
  hivePartitioning,
}: {
  href: string;
  connection: AsyncDuckDBConnection;
  hivePartitioning: boolean;
}): Promise<string | null> {
  try {
    const query = `SELECT typeof(bbox) as bbox_type FROM read_parquet('${href}', hive_partitioning = ${hivePartitioning}) LIMIT 1`;
    const result = (await connection.query(query)) as unknown as Table;
    const row = result.toArray().map((row) => row.toJSON())[0];
    return (row?.bbox_type as string | undefined) ?? null;
  } catch {
    return null;
  }
}

/**
 * SQL expressions for the start and end of each row's time span.
 *
 * STAC allows `datetime` to be null when `start_datetime` and `end_datetime`
 * are set, so a file may carry instants, ranges, or a mix. These mirror
 * `getItemDatetimeMs`: `datetime` wins when present, the range otherwise.
 */
export interface StacGeoparquetDatetimeColumns {
  start: string;
  end: string;
}

export function datetimeColumnsFromNames(
  names: Iterable<string>
): StacGeoparquetDatetimeColumns | null {
  const has = new Set(names);
  const hasDatetime = has.has("datetime");
  if (has.has("start_datetime") && has.has("end_datetime")) {
    return hasDatetime
      ? {
          start: "COALESCE(datetime, start_datetime)",
          end: "COALESCE(datetime, end_datetime)",
        }
      : { start: "start_datetime", end: "end_datetime" };
  }
  if (hasDatetime) return { start: "datetime", end: "datetime" };
  return null;
}

/**
 * WHERE clause keeping rows whose time span overlaps `filter`, the same test
 * `itemMatchesFilter` applies to JSON items.
 */
export function datetimeFilterWhere(
  columns: StacGeoparquetDatetimeColumns,
  filter: [number, number]
): string {
  const start = new Date(filter[0]).toISOString();
  const end = new Date(filter[1]).toISOString();
  // TIMESTAMPTZ, not TIMESTAMP: a naive literal is compared to the
  // tz-aware stac-geoparquet columns in the session time zone, which
  // duckdb-wasm takes from the browser, shifting the bounds by its offset
  return `${columns.end} >= TIMESTAMPTZ '${start}' AND ${columns.start} <= TIMESTAMPTZ '${end}'`;
}

async function getDatetimeColumns({
  href,
  connection,
  hivePartitioning,
}: {
  href: string;
  connection: AsyncDuckDBConnection;
  hivePartitioning: boolean;
}): Promise<StacGeoparquetDatetimeColumns | null> {
  try {
    const query = `DESCRIBE SELECT * FROM read_parquet('${href}', hive_partitioning = ${hivePartitioning})`;
    const result = (await connection.query(query)) as unknown as Table;
    const names = result
      .toArray()
      .map((row) => row.toJSON().column_name as string);
    return datetimeColumnsFromNames(names);
  } catch {
    return null;
  }
}

async function fetchStacGeoparquetDatetimeExtent({
  href,
  connection,
  hivePartitioning,
  datetimeColumns,
}: {
  href: string;
  connection: AsyncDuckDBConnection;
  hivePartitioning: boolean;
  datetimeColumns: StacGeoparquetDatetimeColumns;
}): Promise<[number, number] | null> {
  try {
    const result = await executeDuckdbQuery({
      connection,
      href,
      hivePartitioning,
      select: `MIN(${datetimeColumns.start}) as dt_min, MAX(${datetimeColumns.end}) as dt_max`,
    });
    const row = result.toArray().map((row) => row.toJSON())[0];
    const min = row?.dt_min ? new Date(row.dt_min).getTime() : NaN;
    const max = row?.dt_max ? new Date(row.dt_max).getTime() : NaN;
    if (Number.isNaN(min) || Number.isNaN(max)) return null;
    return [min, max];
  } catch {
    return null;
  }
}

export async function fetchStacGeoparquetTable({
  href,
  connection,
  hivePartitioning,
  where,
}: {
  href: string;
  connection: AsyncDuckDBConnection;
  hivePartitioning: boolean;
  where?: string;
}) {
  const result = await executeDuckdbQuery({
    connection,
    href,
    hivePartitioning,
    where,
    select:
      "ST_AsWKB(geometry) AS geometry, ST_GeometryType(geometry) AS geometry_type, id",
  });
  // A filter can match nothing; that is an empty map, not an unsupported
  // geometry type (the type is read from the first row)
  if (result.numRows === 0) {
    return { table: undefined, geometryType: undefined };
  }
  const geometry: Uint8Array[] = result.getChildAt(0)?.toArray();
  const geometryType = result.getChildAt(1)?.toArray()[0]?.toLowerCase() as
    string | undefined;
  if (
    !geometryType ||
    !SUPPORTED_GEOMETRY_TYPES.includes(geometryType as SupportedGeometryType)
  ) {
    throw new Error(
      `Unsupported geometry type: ${geometryType}. Supported types: ${SUPPORTED_GEOMETRY_TYPES.join(", ")}`
    );
  }

  const wkb = new Uint8Array(geometry?.flatMap((array) => [...array]));
  const valueOffsets = new Int32Array(geometry.length + 1);
  for (let i = 0, len = geometry.length; i < len; i++) {
    const current = valueOffsets[i];
    valueOffsets[i + 1] = current + geometry[i].length;
  }
  const wkbData: Data<Binary> = makeData({
    type: new Binary(),
    data: wkb,
    valueOffsets,
  });
  let table: Table | undefined = undefined;
  if (geometryType === "polygon") {
    const polygons = io.parseWkb(
      wkbData,
      io.WKBType.Polygon,
      2
    ) as data.PolygonData;
    table = new Table({
      geometry: makeVector(polygons),
      id: vectorFromArray(result.getChild("id")?.toArray()),
    });
    table.schema.fields[0].metadata.set(
      "ARROW:extension:name",
      "geoarrow.polygon"
    );
  } else if (geometryType === "multipolygon") {
    const multipolygons = parseMultiPolygonWkb(
      wkbData,
      "MultiPolygon",
      2
    ) as MultiPolygonData;
    table = new Table({
      geometry: makeVector(multipolygons),
      id: vectorFromArray(result.getChild("id")?.toArray()),
    });
    table.schema.fields[0].metadata.set(
      "ARROW:extension:name",
      "geoarrow.multipolygon"
    );
  } else if (geometryType === "point") {
    const points = io.parseWkb(wkbData, io.WKBType.Point, 2) as data.PointData;
    table = new Table({
      geometry: makeVector(points),
      id: vectorFromArray(result.getChild("id")?.toArray()),
    });
    table.schema.fields[0].metadata.set(
      "ARROW:extension:name",
      "geoarrow.point"
    );
  } else if (geometryType === "linestring") {
    const linestrings = io.parseWkb(
      wkbData,
      io.WKBType.LineString,
      2
    ) as data.LineStringData;
    table = new Table({
      geometry: makeVector(linestrings),
      id: vectorFromArray(result.getChild("id")?.toArray()),
    });
    table.schema.fields[0].metadata.set(
      "ARROW:extension:name",
      "geoarrow.linestring"
    );
  }
  return {
    table,
    geometryType: geometryType as SupportedGeometryType | undefined,
  };
}

export async function fetchStacGeoparquetItem({
  id,
  href,
  connection,
  hivePartitioning,
}: {
  id: string;
  href: string;
  connection: AsyncDuckDBConnection;
  hivePartitioning: boolean;
}) {
  const result = await executeDuckdbQuery({
    connection,
    href,
    hivePartitioning,
    select: "* REPLACE ST_AsGeoJSON(geometry) as geometry",
    where: `id = '${id}'`,
  });
  const stacWasm = await loadStacWasm();
  const item = stacWasm.arrowToStacJson(result)[0];
  item.geometry = JSON.parse(item.geometry);
  return item;
}
