"""
AWS Glue (Python shell) job: read a CSV from S3 and push its columns to Seeq as signals with SPy.

This is the reverse of ``datalab-to-s3/seeq_datalab_files_to_s3.py``.

Flow
----
1. Read the CSV from s3://{S3_BUCKET}/{S3_KEY} into a pandas DataFrame.
2. Use one column as the timestamp index (default: the first column). Naive timestamps are read in ``TIMEZONE``.
3. Log in to Seeq with an Application identity API Key via ``spy.login(access_key=..., password=...)``.
4. ``spy.push`` every other column as a signal into the datasource ``DATASOURCE_NAME``. Signal names are the column
   headers, optionally prefixed with ``SIGNAL_NAME_PREFIX``. Pushing again updates the same signals: samples with the
   same timestamp are overwritten and new ones are appended.
5. Log out.

CSV layout (wide, one row per timestamp)
---------------------------------------
    Timestamp,Reactor Temperature,Reactor Pressure,Batch Phase
    2026-11-01T00:00:00Z,71.2,101.3,Heating
    2026-11-01T00:01:00Z,71.9,101.5,Heating

Numeric columns become numeric signals and any other column becomes a string signal. Empty cells are skipped.

Glue job parameters (pass as ``--NAME value``)
---------------------------------------------
Required:
  --SEEQ_URL            Seeq server URL, e.g. https://acme.seeq.site
  --S3_BUCKET           Bucket that holds the CSV
  --S3_KEY              Key of the CSV, e.g. seeq-import/reactor.csv
Credentials (one of):
  --SEEQ_SECRET_ID      Secrets Manager secret holding JSON {"access_key": "...", "access_key_password": "..."}
                        (recommended; same secret as the Data Lab -> S3 job)
  --SEEQ_ACCESS_KEY + --SEEQ_ACCESS_KEY_PASSWORD   (plain job args; fine for testing only)
Optional:
  --DATASOURCE_NAME     Datasource that holds the pushed signals (default: "AWS Glue S3 CSV Import")
  --SIGNAL_NAME_PREFIX  Text put in front of every signal name, e.g. "Reactor 1 " (default: none)
  --TIMESTAMP_COLUMN    Column with the timestamps (default: the first column)
  --TIMEZONE            Time zone for timestamps that have no offset (default: UTC)
  --UNITS               JSON map of column -> unit of measure, e.g. {"Reactor Temperature": "degC"}
  --WORKBOOK            Workbook path to scope the signals to, e.g. "AWS Glue >> S3 CSV Import". When omitted the
                        signals are globally scoped, so any workbook can find them through the datasource.
  --VERIFY_SSL          true/false (default: true)

The Application identity API Key's user needs permission to create items (any normal user can). The Glue job role
needs s3:GetObject on the CSV (and secretsmanager:GetSecretValue if SEEQ_SECRET_ID is used). SPy is installed with the
Glue argument ``--additional-python-modules seeq~=66.0,seeq-spy``: ``seeq-spy`` does not include the SDK, and the
``seeq`` SDK package's major version must match the Seeq server's (R66 -> ``seeq~=66.0``).
"""
import io
import json
import logging
import sys
from typing import Dict, Optional, Tuple

import boto3
import pandas as pd

LOG = logging.getLogger("s3_csv_to_seeq")
# force=True: Glue's Python shell runner configures the root logger first, which would otherwise hide INFO lines.
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout,
                    force=True)

DEFAULT_DATASOURCE_NAME = "AWS Glue S3 CSV Import"


# --------------------------------------------------------------------------------------------------
# Arguments
# --------------------------------------------------------------------------------------------------
def _read_args() -> Dict[str, str]:
    required = ["SEEQ_URL", "S3_BUCKET", "S3_KEY"]
    optional = ["SEEQ_SECRET_ID", "SEEQ_ACCESS_KEY", "SEEQ_ACCESS_KEY_PASSWORD", "DATASOURCE_NAME",
                "SIGNAL_NAME_PREFIX", "TIMESTAMP_COLUMN", "TIMEZONE", "UNITS", "WORKBOOK", "VERIFY_SSL"]
    try:
        from awsglue.utils import getResolvedOptions  # Present inside Glue
    except ImportError:
        getResolvedOptions = None

    if getResolvedOptions is not None:
        args = getResolvedOptions(sys.argv, required)
        # getResolvedOptions fails on missing names, so resolve optional ones only if supplied.
        present = [name for name in optional if f"--{name}" in sys.argv]
        if present:
            args.update(getResolvedOptions(sys.argv, present))
    else:
        # Local fallback: --NAME value pairs
        args = {}
        argv = sys.argv[1:]
        for i, token in enumerate(argv):
            if token.startswith("--") and i + 1 < len(argv):
                args[token[2:]] = argv[i + 1]
        missing = [name for name in required if name not in args]
        if missing:
            raise SystemExit(f"Missing required arguments: {', '.join('--' + m for m in missing)}")
    return args


def _as_bool(value: Optional[str], default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "y")


def _resolve_credentials(args: Dict[str, str]) -> Tuple[str, str]:
    if args.get("SEEQ_SECRET_ID"):
        secret = boto3.client("secretsmanager").get_secret_value(SecretId=args["SEEQ_SECRET_ID"])
        payload = json.loads(secret["SecretString"])
        try:
            return payload["access_key"], payload["access_key_password"]
        except KeyError as e:
            raise SystemExit(f"Secret {args['SEEQ_SECRET_ID']} must contain keys 'access_key' and "
                             f"'access_key_password' (missing {e})")
    if args.get("SEEQ_ACCESS_KEY") and args.get("SEEQ_ACCESS_KEY_PASSWORD"):
        return args["SEEQ_ACCESS_KEY"], args["SEEQ_ACCESS_KEY_PASSWORD"]
    raise SystemExit("Provide --SEEQ_SECRET_ID, or both --SEEQ_ACCESS_KEY and --SEEQ_ACCESS_KEY_PASSWORD")


def _parse_units(raw: Optional[str]) -> Dict[str, str]:
    if not raw:
        return {}
    try:
        units = json.loads(raw)
    except json.JSONDecodeError as e:
        raise SystemExit(f'--UNITS must be a JSON object such as {{"Reactor Temperature": "degC"}} ({e})')
    if not isinstance(units, dict):
        raise SystemExit('--UNITS must be a JSON object such as {"Reactor Temperature": "degC"}')
    return {str(k): str(v) for k, v in units.items()}


# --------------------------------------------------------------------------------------------------
# CSV -> DataFrame
# --------------------------------------------------------------------------------------------------
def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except s3.exceptions.NoSuchKey:
        raise SystemExit(f"s3://{bucket}/{key} does not exist. Check --S3_BUCKET and --S3_KEY.")
    body = obj["Body"].read()
    LOG.info("Read s3://%s/%s (%d bytes)", bucket, key, len(body))
    return pd.read_csv(io.BytesIO(body))


def to_signal_frame(raw: pd.DataFrame, timestamp_column: Optional[str], timezone: str) -> pd.DataFrame:
    """Index the frame by timestamp and make each remaining column numeric where possible, string otherwise."""
    if raw.empty or len(raw.columns) < 2:
        raise SystemExit("The CSV needs a timestamp column and at least one data column, with at least one row.")

    column = timestamp_column or raw.columns[0]
    if column not in raw.columns:
        raise SystemExit(f"Timestamp column '{column}' is not in the CSV. Columns are: {list(raw.columns)}")

    timestamps = pd.to_datetime(raw[column], errors="coerce")
    bad = int(timestamps.isna().sum())
    if bad == len(timestamps):
        raise SystemExit(f"None of the values in '{column}' could be read as timestamps, e.g. "
                         f"'{raw[column].iloc[0]}'. Set --TIMESTAMP_COLUMN to the right column.")
    if bad:
        LOG.warning("Dropping %d row(s) whose '%s' value is not a timestamp", bad, column)

    # Seeq needs time-zone-aware timestamps. Naive values are read in --TIMEZONE.
    timestamps = timestamps.dt.tz_localize(timezone) if timestamps.dt.tz is None else timestamps
    df = raw.drop(columns=column)
    df.index = pd.DatetimeIndex(timestamps, name=None)
    df = df[df.index.notna()].sort_index()
    if df.index.has_duplicates:
        LOG.warning("Found %d duplicate timestamp(s); keeping the last row for each",
                    int(df.index.duplicated().sum()))
        df = df[~df.index.duplicated(keep="last")]

    for name in df.columns:
        values = df[name]
        numeric = pd.Series(pd.to_numeric(values, errors="coerce"), index=df.index)
        # Treat the column as numeric if every non-empty cell parsed as a number; otherwise it's a string signal.
        if numeric.notna().sum() == values.notna().sum():
            df[name] = numeric.astype(float)
        else:
            df[name] = values.astype(object).where(values.notna(), None)
    return df


def build_metadata(df: pd.DataFrame, prefix: str, units: Dict[str, str]) -> pd.DataFrame:
    unknown = sorted(set(units) - set(df.columns))
    if unknown:
        LOG.warning("--UNITS names column(s) not in the CSV, ignoring them: %s", unknown)
    rows = []
    for column in df.columns:
        row = {"Name": f"{prefix}{column}".strip(), "Type": "Signal"}
        if df[column].dtype == object:
            # String signals must be created with the 'string' unit; Seeq won't change it once samples are stored.
            row["Value Unit Of Measure"] = "string"
        elif column in units:
            row["Value Unit Of Measure"] = units[column]
        rows.append(row)
    # spy.push matches metadata rows to data columns by index.
    return pd.DataFrame(rows, index=df.columns)


# --------------------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------------------
def main() -> None:
    args = _read_args()
    server_url = args["SEEQ_URL"].rstrip("/")
    bucket, key = args["S3_BUCKET"], args["S3_KEY"].lstrip("/")
    datasource = args.get("DATASOURCE_NAME") or DEFAULT_DATASOURCE_NAME
    prefix = args.get("SIGNAL_NAME_PREFIX", "")
    workbook = args.get("WORKBOOK") or None
    verify_ssl = _as_bool(args.get("VERIFY_SSL"), True)
    units = _parse_units(args.get("UNITS"))

    df = to_signal_frame(read_csv_from_s3(bucket, key), args.get("TIMESTAMP_COLUMN"), args.get("TIMEZONE", "UTC"))
    metadata = build_metadata(df, prefix, units)
    LOG.info("Parsed %d signal(s) x %d timestamp(s) from %s to %s",
             len(df.columns), len(df), df.index.min(), df.index.max())

    # Imported here so argument and CSV errors show up even if SPy failed to install.
    from seeq import spy

    access_key, password = _resolve_credentials(args)
    spy.options.friendly_exceptions = False  # Plain tracebacks in CloudWatch
    spy.login(url=server_url, access_key=access_key, password=password, ignore_ssl_errors=not verify_ssl,
              quiet=True)
    LOG.info("Logged in to %s as %s (SPy %s)", server_url, spy.user.username, spy.__version__)

    try:
        results = spy.push(
            data=df,
            metadata=metadata,
            datasource=datasource,
            workbook=workbook,
            # A worksheet only makes sense when the signals are scoped to a workbook.
            worksheet="From S3" if workbook else None,
            quiet=True,
        )
    finally:
        spy.logout(quiet=True)

    LOG.info("Pushed to datasource '%s':\n%s", datasource,
             results[["Name", "ID", "Push Result"]].to_string(index=False))
    failed = results[results["Push Result"] != "Success"]
    if len(failed):
        raise SystemExit(f"{len(failed)} signal(s) failed to push: {failed['Name'].tolist()}")
    LOG.info("Done: %d signal(s), %d sample row(s) pushed from s3://%s/%s", len(results), len(df), bucket, key)


if __name__ == "__main__":
    main()
