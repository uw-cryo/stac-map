import { describe, expect, it } from "vitest";
import {
  datetimeColumnsFromNames,
  datetimeFilterWhere,
} from "../../src/utils/stac-geoparquet";

describe("datetimeColumnsFromNames", () => {
  it("uses datetime alone when there is no range", () => {
    expect(datetimeColumnsFromNames(["id", "datetime", "geometry"])).toEqual({
      start: "datetime",
      end: "datetime",
    });
  });

  it("falls back to the range when datetime may be null", () => {
    expect(
      datetimeColumnsFromNames(["datetime", "start_datetime", "end_datetime"])
    ).toEqual({
      start: "COALESCE(datetime, start_datetime)",
      end: "COALESCE(datetime, end_datetime)",
    });
  });

  it("uses the range when there is no datetime column", () => {
    expect(
      datetimeColumnsFromNames(["start_datetime", "end_datetime"])
    ).toEqual({ start: "start_datetime", end: "end_datetime" });
  });

  it("ignores a half range", () => {
    expect(datetimeColumnsFromNames(["datetime", "start_datetime"])).toEqual({
      start: "datetime",
      end: "datetime",
    });
    expect(datetimeColumnsFromNames(["start_datetime"])).toBeNull();
  });

  it("returns null with no datetime columns", () => {
    expect(datetimeColumnsFromNames(["id", "geometry"])).toBeNull();
  });
});

describe("datetimeFilterWhere", () => {
  const filter: [number, number] = [
    Date.UTC(2020, 0, 1),
    Date.UTC(2020, 11, 31),
  ];

  it("keeps rows whose span overlaps the filter", () => {
    expect(
      datetimeFilterWhere(
        { start: "start_datetime", end: "end_datetime" },
        filter
      )
    ).toBe(
      "end_datetime >= TIMESTAMPTZ '2020-01-01T00:00:00.000Z' AND start_datetime <= TIMESTAMPTZ '2020-12-31T00:00:00.000Z'"
    );
  });

  it("is equivalent to BETWEEN for instants", () => {
    expect(
      datetimeFilterWhere({ start: "datetime", end: "datetime" }, filter)
    ).toBe(
      "datetime >= TIMESTAMPTZ '2020-01-01T00:00:00.000Z' AND datetime <= TIMESTAMPTZ '2020-12-31T00:00:00.000Z'"
    );
  });
});
