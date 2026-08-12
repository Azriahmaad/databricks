import math

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# _CAT = "uc"
# _SCH = "famous"
# POSITIONS_TABLE = f"{_CAT}.{_SCH}.tc_positions"
# DEVICES_TABLE   = f"{_CAT}.{_SCH}.tc_devices"
# GEOFENCES_TABLE = f"{_CAT}.{_SCH}.tc_geofences"
# LANES_TABLE     = f"{_CAT}.{_SCH}.famous_jalur_202606242329"

_CAT = "uc"
_SCH = "famous_16_feb"
POSITIONS_TABLE = f"{_CAT}.{_SCH}.famous_tc_positions_16_feb"
DEVICES_TABLE   = f"{_CAT}.{_SCH}.tc_devices"
GEOFENCES_TABLE = f"{_CAT}.{_SCH}.tc_geofences"
LANES_TABLE     = f"uc.famous.famous_jalur_202606242329"

def _haversine_py(lat1, lon1, lat2, lon2):
    R = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi    = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2.0 * R * math.asin(math.sqrt(min(a, 1.0)))


def _parse_linestring_wkt(wkt):
    """Lane WKT: LINESTRING in lon lat order → [(lon, lat), ...]."""
    s, e = wkt.find("("), wkt.rfind(")")
    if s == -1 or e == -1:
        return []
    return [
        tuple(float(v) for v in p.strip().split()[:2])
        for p in wkt[s+1:e].split(",")
    ]


def _seg_dist_m(plat, plon, alat, alon, blat, blon):
    dax, day = blon - alon, blat - alat
    apx, apy = plon - alon, plat - alat
    ab2 = dax*dax + day*day
    if ab2 == 0.0:
        return _haversine_py(plat, plon, alat, alon)
    t = max(0.0, min(1.0, (apx*dax + apy*day) / ab2))
    return _haversine_py(plat, plon, alat + t*day, alon + t*dax)


@F.udf(returnType=DoubleType())
def _point_to_linestring_dist_m(plat, plon, wkt):
    """Distance in metres from GPS point to WKT LINESTRING lane (lon lat order)."""
    if plat is None or plon is None or wkt is None:
        return None
    coords = _parse_linestring_wkt(wkt)
    if not coords:
        return None
    if len(coords) == 1:
        return _haversine_py(plat, plon, coords[0][1], coords[0][0])
    return min(
        _seg_dist_m(plat, plon, coords[i][1], coords[i][0],
                    coords[i+1][1], coords[i+1][0])
        for i in range(len(coords) - 1)
    )


@F.udf(returnType=DoubleType())
def _haversine_m(lat1, lon1, lat2, lon2):
    if any(v is None for v in (lat1, lon1, lat2, lon2)):
        return None
    return _haversine_py(lat1, lon1, lat2, lon2)

def _interval_bucket(ts_col):
    h = F.hour(ts_col)
    return (
        F.when(h.between(0, 5), F.lit("00:00-05:59"))
        .when(h.between(6, 11), F.lit("06:00-11:59"))
        .when(h.between(12, 17), F.lit("12:00-17:59"))
        .otherwise(F.lit("18:00-23:59"))
    )


def _hari_indonesia(ts_col):
    dow = F.dayofweek(ts_col)
    return (
        F.when(dow == 1, F.lit("Minggu"))
        .when(dow == 2, F.lit("Senin"))
        .when(dow == 3, F.lit("Selasa"))
        .when(dow == 4, F.lit("Rabu"))
        .when(dow == 5, F.lit("Kamis"))
        .when(dow == 6, F.lit("Jumat"))
        .otherwise(F.lit("Sabtu"))
    )


def _iso_week_label(ts_col):
    return F.concat(
        F.year(ts_col).cast("string"),
        F.lit("-W"),
        F.lpad(F.weekofyear(ts_col).cast("string"), 2, "0"),
    )


def _read_devices():
    return spark.read.table(DEVICES_TABLE).select(
        F.col("id").cast("long").alias("deviceid_lookup"),
        F.col("name").alias("truk_name"),
    )


def _read_lanes():
    return spark.read.table(LANES_TABLE).select(
        F.col("id").cast("string").alias("lane_id"),
        F.col("nama_jalur").alias("lane_name"),
        F.col("arah").cast("string").alias("lane_direction"),
        F.col("koordinat_jalur"),
    )


def _read_geofences():
    return spark.read.table(GEOFENCES_TABLE).select(
        F.col("id").cast("long").alias("geofence_lookup_id"),
        F.col("name").alias("geofence_name"),
    )


def _sort_key_primary(course_col, lat_col, lon_col):
    return (
        F.when(course_col == 0, lat_col)
        .when(course_col == 180, -lat_col)
        .when(course_col == 90, lon_col)
        .when(course_col == 270, -lon_col)
        .when(course_col.between(0, 90), lat_col)
        .when(course_col.between(90, 180), -lat_col)
        .when(course_col.between(180, 270), -lat_col)
        .when(course_col.between(270, 360), lat_col)
    )


def _sort_key_secondary(course_col, lon_col):
    return (
        F.when(course_col.between(0, 90), lon_col)
        .when(course_col.between(90, 180), lon_col)
        .when(course_col.between(180, 270), -lon_col)
        .when(course_col.between(270, 360), -lon_col)
    )

# SILVER TABLE: silver_famous_jarakaman


@dlt.table(
    name="silver_famous_jarakaman",
    comment="Silver output shaped to match famous_jarakaman_16_feb as closely as possible.",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dlt.expect("paired_trucks_present", "truk IS NOT NULL AND truk_depan IS NOT NULL")
@dlt.expect("non_negative_jarak", "jarak IS NULL OR jarak >= 0")
def silver_famous_jarakaman():
    devices = _read_devices()
    lanes = _read_lanes()
    geofences = _read_geofences()

    positions = (
        spark.readStream.table(POSITIONS_TABLE)
        .select(
            "id",
            "deviceid",
            "devicetime",
            "latitude",
            "longitude",
            "speed",
            "course",
            "geofenceids",
            "valid",
        )
        .withColumn("id", F.col("id").cast("long"))
        .withColumn("deviceid", F.col("deviceid").cast("long"))
        .withColumn("devicetime", F.to_timestamp("devicetime"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("speed", F.col("speed").cast("double"))
        .withColumn("course", F.col("course").cast("double"))
        .withWatermark("devicetime", "30 seconds")
        .filter(F.col("valid") == F.lit(1))
        .filter(F.col("latitude").between(-90.0, 90.0))
        .filter(F.col("longitude").between(-180.0, 180.0))
    )

    positions_with_devices = (
        positions
        .join(devices, positions.deviceid == devices.deviceid_lookup, "left")
        .drop("deviceid_lookup")
        .withColumn("truk", F.coalesce(F.col("truk_name"), F.col("deviceid").cast("string")))
        .drop("truk_name")
        .withColumn(
            "grup_arah",
            F.when(
                ((F.col("course") >= 270.0) & (F.col("course") <= 360.0))
                | ((F.col("course") >= 0.0) & (F.col("course") <= 90.0)),
                F.lit("utara"),
            ).when(
                (F.col("course") >= 90.0) & (F.col("course") <= 270.0),
                F.lit("selatan"),
            ),
        )
    )

    lanes_keyed = lanes.withColumn("_jk", F.lit(1))
    pos_keyed = positions_with_devices.withColumn("_jk", F.lit(1))

    positions_with_lane = (
        pos_keyed
        .join(lanes_keyed, "_jk", "inner")
        .drop("_jk")
        .withColumn(
            "lane_dist_m",
            _point_to_linestring_dist_m(
                F.col("latitude"),
                F.col("longitude"),
                F.col("koordinat_jalur"),
            ),
        )
        .filter(F.col("lane_dist_m").isNotNull() & (F.col("lane_dist_m") <= 4.4))
        .groupBy(
            "id",
            "deviceid",
            "truk",
            "devicetime",
            "latitude",
            "longitude",
            "speed",
            "course",
            "grup_arah",
            "geofenceids",
        )
        .agg(
            F.min_by(F.col("lane_id"), F.col("lane_name")).alias("lane_id"),
            F.min_by(F.col("lane_name"), F.col("lane_name")).alias("jalur"),
            F.min_by(F.col("lane_direction"), F.col("lane_name")).alias("jalur_arah"),
        )
    )

    ordered_pairs = (
        positions_with_lane
        .withColumn("event_window", F.window("devicetime", "5 seconds"))
        .withColumn("sort_key_1", _sort_key_primary(F.col("course"), F.col("latitude"), F.col("longitude")))
        .withColumn("sort_key_2", _sort_key_secondary(F.col("course"), F.col("longitude")))
        .groupBy("event_window", "jalur_arah")
        .agg(
            F.sort_array(
                F.collect_list(
                    F.struct(
                        F.col("sort_key_1").alias("sort_key_1"),
                        F.col("sort_key_2").alias("sort_key_2"),
                        F.col("truk").alias("truk"),
                        F.col("id").alias("position_id"),
                        F.col("speed").alias("speed"),
                        F.col("latitude").alias("lat"),
                        F.col("longitude").alias("lon"),
                        F.col("devicetime").alias("devicetime"),
                        F.col("grup_arah").alias("grup_arah"),
                        F.col("geofenceids").alias("geofenceids"),
                        F.col("jalur").alias("jalur"),
                    )
                )
            ).alias("ordered_trucks")
        )
        .filter(F.size("ordered_trucks") > 1)
        .withColumn("pair_index", F.explode(F.sequence(F.lit(1), F.size("ordered_trucks") - 1)))
        .withColumn("behind", F.element_at("ordered_trucks", F.col("pair_index")))
        .withColumn("ahead", F.element_at("ordered_trucks", F.col("pair_index") + 1))
    )

    base_pairs = (
        ordered_pairs.select(
            F.col("behind.truk").alias("truk"),
            F.col("ahead.truk").alias("truk_depan"),
            F.when(F.col("behind.jalur") != F.col("ahead.jalur"), F.lit("Beda Jalur"))
            .otherwise(F.lit("Sejalur"))
            .alias("sejalur"),
            F.col("behind.jalur").alias("jalur"),
            F.col("ahead.jalur").alias("jalur2"),
            F.round(
                _haversine_m(
                    F.col("behind.lat"),
                    F.col("behind.lon"),
                    F.col("ahead.lat"),
                    F.col("ahead.lon"),
                ),
                0,
            ).cast("double").alias("jarak"),
            F.when(
                F.col("behind.speed") > F.col("ahead.speed"),
                F.round(
                    _haversine_m(
                        F.col("behind.lat"),
                        F.col("behind.lon"),
                        F.col("ahead.lat"),
                        F.col("ahead.lon"),
                    )
                    / ((F.col("behind.speed") - F.col("ahead.speed")) * F.lit(1.852) * F.lit(1000.0 / 3600.0)),
                    0,
                ).cast("double"),
            ).alias("jarak_waktu"),
            F.round(F.col("behind.speed"), 1).alias("speed"),
            F.round(F.col("ahead.speed"), 1).alias("speed2"),
            F.col("behind.lat").alias("lat"),
            F.col("ahead.lat").alias("lat2"),
            F.col("behind.lon").alias("lon"),
            F.col("ahead.lon").alias("lon2"),
            F.col("behind.devicetime").alias("devicetime"),
            F.col("behind.grup_arah").alias("grup_arah"),
            F.col("behind.position_id").cast("string").alias("truk_id"),
            F.col("ahead.position_id").cast("string").alias("truk_depan_id"),
            F.trim(F.substring(F.trim(F.col("behind.truk")), 1, 4)).alias("grup_truk"),
            F.col("behind.geofenceids").alias("geofenceids"),
        )
    )

    geofence_rows = (
        base_pairs
        .withColumn(
            "geofence_id_raw",
            F.explode(
                F.split(
                    F.regexp_replace(F.coalesce(F.col("geofenceids"), F.lit("")), r"[\[\]]", ""),
                    ",",
                )
            ),
        )
        .withColumn("geofence_id_raw", F.trim(F.col("geofence_id_raw")))
        .filter(F.col("geofence_id_raw").rlike("^[0-9]+$"))
        .withColumn("geofence_lookup_id", F.col("geofence_id_raw").cast("long"))
        .join(geofences, "geofence_lookup_id", "inner")
    )

    return geofence_rows.select(
        "truk",
        "truk_depan",
        "sejalur",
        "jalur",
        "jalur2",
        "jarak",
        "jarak_waktu",
        "speed",
        "speed2",
        "lat",
        "lat2",
        "lon",
        "lon2",
        "devicetime",
        "grup_arah",
        "truk_id",
        "truk_depan_id",
        "grup_truk",
        "geofenceids",
        "geofence_name",
    )


# GOLD TABLES – 5-minute tumbling window aggregations over silver


def _silver_stream():
    return (
        spark.readStream.table("silver_famous_jarakaman")
        .withWatermark("devicetime", "30 seconds")
        .withColumn(
            "KM",
            F.regexp_extract(F.coalesce(F.col("geofence_name"), F.lit("")), r"KM\s+([\d.]+)", 1),
        )
        .withColumn("Interval", _interval_bucket(F.col("devicetime")))
        .withColumn("Hari", _hari_indonesia(F.col("devicetime")))
        .withColumn("week_of_year", _iso_week_label(F.col("devicetime")))
    )


@dlt.table(
    name="gold_v_famous_jarak_aman_gps",
    comment="5-minute GPS-level rollup: avg TTC & distance. Powers Interval × Hari heatmap.",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def gold_v_famous_jarak_aman_gps():
    return (
        _silver_stream()
        .groupBy(F.window("devicetime", "5 minutes").alias("event_window"),
                 "Interval", "Hari", "week_of_year")
        .agg(F.avg("jarak_waktu").alias("rerata_jarak_waktu"),
             F.avg("jarak").alias("rerata_jarak"))
        .select(F.col("event_window.start").alias("window_start"),
                F.col("event_window.end").alias("window_end"),
                "Interval", "Hari", "week_of_year",
                "rerata_jarak_waktu", "rerata_jarak")
    )


@dlt.table(
    name="gold_v_famous_jarak_aman_km",
    comment="5-minute KM-level rollup: avg TTC. Powers Top-5 Best/Worst KM chart.",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def gold_v_famous_jarak_aman_km():
    return (
        _silver_stream()
        .filter(F.col("KM").isNotNull() & (F.trim(F.col("KM")) != ""))
        .groupBy(F.window("devicetime", "5 minutes").alias("event_window"),
                 "KM", "Hari", "week_of_year")
        .agg(F.avg("jarak_waktu").alias("rerata_jarak_waktu"))
        .select(F.col("event_window.start").alias("window_start"),
                F.col("event_window.end").alias("window_end"),
                "KM", "Hari", "week_of_year", "rerata_jarak_waktu")
    )


@dlt.table(
    name="gold_v_famous_jarak_aman_grup_truk",
    comment="5-minute truck-group rollup: avg TTC. Powers truck group performance chart.",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"},
)
def gold_v_famous_jarak_aman_grup_truk():
    return (
        _silver_stream()
        .groupBy(F.window("devicetime", "5 minutes").alias("event_window"),
                 F.col("grup_truk").alias("grup_truk_name"),
                 "Hari", "week_of_year")
        .agg(F.avg("jarak_waktu").alias("rerata_jarak_waktu"))
        .select(F.col("event_window.start").alias("window_start"),
                F.col("event_window.end").alias("window_end"),
                "grup_truk_name", "Hari", "week_of_year", "rerata_jarak_waktu")
    )
