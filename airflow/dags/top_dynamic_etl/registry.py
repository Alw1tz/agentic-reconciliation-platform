"""Central registry for the dynamic ETL pipeline -- top_dynamic_etl_pipeline,
tier 3 capstone (projects/top_dynamic_etl_pipeline/README.md).

One @dataclass config per "interface." dag_factory.py loops over REGISTRY
and generates one Airflow DAG per entry -- N DAGs from one factory, instead
of N copy-pasted DAG files. This is the generalized shape of a real
production fixed-width-file ingestion framework; the two interfaces
registered here are a real public dataset (NOAA ISD) and a small synthetic
one, not real employer data (see CURRICULUM.md project 11's own stated
design principle).

Position/length numbers below are 1-indexed and inclusive, matching the
source format-spec documents' own numbering directly -- so a reviewer can
check a field against the doc without an off-by-one translation in their
head.
"""
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum


class FieldType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    DECIMAL = "decimal"   # requires scaling_factor
    DATE = "date"


@dataclass(frozen=True)
class FixedWidthField:
    """One column in a fixed-width layout."""

    name: str
    start_pos: int
    length: int
    field_type: FieldType
    scaling_factor: Decimal | None = None
    # Exact raw substrings (pre-cast, pre-trim-of-sign) that mean "missing."
    # Checked against the trimmed substring BEFORE casting -- TRY_CAST on a
    # sentinel like "+99999" would otherwise silently produce a real bogus
    # number instead of NULL.
    missing_sentinels: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.field_type is FieldType.DECIMAL and self.scaling_factor is None:
            raise ValueError(f"{self.name}: DECIMAL fields require a scaling_factor")

    @property
    def end_pos(self) -> int:
        return self.start_pos + self.length - 1


@dataclass(frozen=True)
class RecordType:
    """One record shape within a file. Most production fixed-width
    interfaces multiplex several record types by a discriminator at a fixed
    offset -- ISD's file has exactly one (discriminator=None, every line
    matches); the synthetic demo interface below has three, of which only
    one is registered here (see its own docstring for why)."""

    name: str
    fields: tuple[FixedWidthField, ...]
    min_line_length: int
    discriminator: tuple[int, int, str] | None = None  # (start_pos, length, value)


@dataclass(frozen=True)
class InterfaceConfig:
    interface_id: str
    description: str
    s3_prefix: str
    file_pattern: str
    raw_table: str
    bronze_table_prefix: str
    record_types: tuple[RecordType, ...]
    compression: str | None = "GZIP"
    count_tolerance_pct: float = 0.0


# ---------------------------------------------------------------------------
# Interface: noaa_isd
#
# NOAA Integrated Surface Database (ISD), hosted publicly at
# s3://noaa-isd-pds/data/<year>/ (no auth needed -- verified live via
# `aws s3 ls --no-sign-request`). Real government weather station data,
# genuinely fixed-width for its first 105 characters (Control Data Section,
# positions 1-60, + Mandatory Data Section, positions 61-105), per the real
# format-spec PDF from that same bucket (isd-format-document.pdf). Beyond
# position 105 is a variable-length "Additional Data Section" -- out of
# scope for this interface; positions 1-105 alone are a complete, genuinely
# fixed-width record.
#
# Every station-year file (e.g. 010080-99999-2024.gz) has exactly one
# record type -- no header/trailer, no discriminator needed. Real dirty-data
# characteristics: sentinel missing values, scaling factors, embedded
# quality-control flag codes.
# ---------------------------------------------------------------------------

_ISD_FIELDS: tuple[FixedWidthField, ...] = (
    # --- Control Data Section (positions 1-60) ---
    FixedWidthField("total_variable_characters", 1, 4, FieldType.INTEGER),
    FixedWidthField("usaf_station_id", 5, 6, FieldType.STRING),
    FixedWidthField("wban_station_id", 11, 5, FieldType.STRING, missing_sentinels=("99999",)),
    FixedWidthField("observation_date", 16, 8, FieldType.DATE),
    FixedWidthField("observation_time_hhmm", 24, 4, FieldType.INTEGER),
    FixedWidthField("data_source_flag", 28, 1, FieldType.STRING, missing_sentinels=("9",)),
    FixedWidthField(
        "latitude", 29, 6, FieldType.DECIMAL,
        scaling_factor=Decimal("1000"), missing_sentinels=("+99999",),
    ),
    FixedWidthField(
        "longitude", 35, 7, FieldType.DECIMAL,
        scaling_factor=Decimal("1000"), missing_sentinels=("+999999",),
    ),
    FixedWidthField("report_type_code", 42, 5, FieldType.STRING, missing_sentinels=("99999",)),
    FixedWidthField(
        "elevation_m", 47, 5, FieldType.DECIMAL,
        scaling_factor=Decimal("1"), missing_sentinels=("+9999",),
    ),
    FixedWidthField("call_letter_id", 52, 5, FieldType.STRING, missing_sentinels=("99999",)),
    FixedWidthField("qc_process_name", 57, 4, FieldType.STRING),
    # --- Mandatory Data Section (positions 61-105) ---
    FixedWidthField("wind_direction_angle", 61, 3, FieldType.INTEGER, missing_sentinels=("999",)),
    FixedWidthField("wind_direction_quality_code", 64, 1, FieldType.STRING),
    FixedWidthField("wind_type_code", 65, 1, FieldType.STRING, missing_sentinels=("9",)),
    FixedWidthField(
        "wind_speed_mps", 66, 4, FieldType.DECIMAL,
        scaling_factor=Decimal("10"), missing_sentinels=("9999",),
    ),
    FixedWidthField("wind_speed_quality_code", 70, 1, FieldType.STRING),
    FixedWidthField(
        "ceiling_height_m", 71, 5, FieldType.DECIMAL,
        scaling_factor=Decimal("1"), missing_sentinels=("99999",),
    ),
    FixedWidthField("ceiling_quality_code", 76, 1, FieldType.STRING),
    FixedWidthField("ceiling_determination_code", 77, 1, FieldType.STRING, missing_sentinels=("9",)),
    FixedWidthField("cavok_code", 78, 1, FieldType.STRING, missing_sentinels=("9",)),
    FixedWidthField(
        "visibility_m", 79, 6, FieldType.DECIMAL,
        scaling_factor=Decimal("1"), missing_sentinels=("999999",),
    ),
    FixedWidthField("visibility_quality_code", 85, 1, FieldType.STRING),
    FixedWidthField("visibility_variability_code", 86, 1, FieldType.STRING, missing_sentinels=("9",)),
    FixedWidthField("visibility_quality_variability_code", 87, 1, FieldType.STRING),
    FixedWidthField(
        "air_temp_c", 88, 5, FieldType.DECIMAL,
        scaling_factor=Decimal("10"), missing_sentinels=("+9999",),
    ),
    FixedWidthField("air_temp_quality_code", 93, 1, FieldType.STRING),
    FixedWidthField(
        "dew_point_c", 94, 5, FieldType.DECIMAL,
        scaling_factor=Decimal("10"), missing_sentinels=("+9999",),
    ),
    FixedWidthField("dew_point_quality_code", 99, 1, FieldType.STRING),
    FixedWidthField(
        "sea_level_pressure_hpa", 100, 5, FieldType.DECIMAL,
        scaling_factor=Decimal("10"), missing_sentinels=("99999",),
    ),
    FixedWidthField("sea_level_pressure_quality_code", 105, 1, FieldType.STRING),
)

NOAA_ISD = InterfaceConfig(
    interface_id="noaa_isd",
    description="NOAA Integrated Surface Database -- global weather station observations",
    s3_prefix="top-dynamic-etl/noaa_isd",
    file_pattern=r".*\.gz",
    raw_table="RAW_NOAA_ISD",
    bronze_table_prefix="BRONZE_NOAA_ISD",
    record_types=(
        RecordType(name="observation", fields=_ISD_FIELDS, min_line_length=105, discriminator=None),
    ),
    compression="GZIP",
    count_tolerance_pct=0.0,  # no header/trailer lines by design -- strict count
)


# ---------------------------------------------------------------------------
# Interface: demo_multi_record
#
# Small, synthetic, fictitious retail-transaction batch file -- exists
# purely to exercise what ISD alone can't: multiple record types
# discriminated by a fixed-position code (proving .expand() maps over more
# than one RecordType), and rows deliberately dropped between RAW and
# BRONZE by design (proving the SOFT_COUNT_INTERFACES allowlist path). No
# real bucket sync needed -- ships as a tiny fixture file in this project.
#
# Layout: every line starts with a 1-char record_type discriminator.
#   'H' = header (1 per file: batch_date, store_id) -- NOT parsed into
#         BRONZE; a header carries no transactional data of its own.
#   'D' = detail (N per file: transaction_id, amount, sku) -- the only
#         record type registered below, so it's the only one that lands
#         in BRONZE.
#   'T' = trailer (1 per file: detail_count) -- NOT parsed into BRONZE,
#         same reasoning as the header.
# RAW will therefore always have exactly 2 more lines than BRONZE
# (header + trailer) -- count_tolerance_pct is sized to that, not left
# unbounded.
# ---------------------------------------------------------------------------

_DEMO_DETAIL_FIELDS: tuple[FixedWidthField, ...] = (
    FixedWidthField("record_type", 1, 1, FieldType.STRING),
    FixedWidthField("transaction_id", 2, 10, FieldType.STRING),
    FixedWidthField(
        "amount", 12, 8, FieldType.DECIMAL,
        scaling_factor=Decimal("100"), missing_sentinels=("99999999",),
    ),
    FixedWidthField("sku", 20, 11, FieldType.STRING),
)

DEMO_MULTI_RECORD = InterfaceConfig(
    interface_id="demo_multi_record",
    description="Synthetic multi-record-type fixture -- proves the registry pattern "
    "generalizes beyond a single-record-type interface like noaa_isd",
    s3_prefix="top-dynamic-etl/demo_multi_record",
    file_pattern=r".*\.txt",
    raw_table="RAW_DEMO_MULTI_RECORD",
    bronze_table_prefix="BRONZE_DEMO_MULTI_RECORD",
    record_types=(
        RecordType(
            name="detail",
            fields=_DEMO_DETAIL_FIELDS,
            min_line_length=30,
            discriminator=(1, 1, "D"),
        ),
    ),
    compression=None,
    # Header + trailer = 2 rows dropped out of 22 total lines (20 detail +
    # 2) in generate_demo_fixture.py's fixture = 9.09%. Deliberately a
    # high percentage -- this is a *small* synthetic fixture by design, so
    # 2 fixed-count dropped rows are a much bigger share of the total than
    # the same 2 rows would be in a real production-scale file (which is
    # exactly why percentage-based tolerance is scale-sensitive: the same
    # absolute drop count means a very different percentage depending on
    # file size). 12% leaves headroom above the exact 9.09% without
    # masking a real regression.
    count_tolerance_pct=12.0,
)


REGISTRY: dict[str, InterfaceConfig] = {
    NOAA_ISD.interface_id: NOAA_ISD,
    DEMO_MULTI_RECORD.interface_id: DEMO_MULTI_RECORD,
}

# Interfaces allowed to lose rows between RAW and BRONZE by design (e.g.
# deliberately-unparsed header/trailer records). Everything else is held to
# an exact RAW-line-count == BRONZE-row-count match.
SOFT_COUNT_INTERFACES: set[str] = {DEMO_MULTI_RECORD.interface_id}
