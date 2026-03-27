"""
ADLS → S3 copy utility
=======================
Recursively copies all files from an Azure Data Lake Storage Gen2 container
to an AWS S3 bucket with the following behaviour:

  • S3 key  =  <last-modified-date>/<original-adls-path>
               e.g.  2024-03-15/raw/events/clicks.csv.gz

  • Files that are NOT already .gz archives are compressed on-the-fly with
    gzip before being uploaded; ".gz" is appended to their S3 key.

  • Files that are already .gz archives are streamed as-is.

Dependencies
------------
    pip install azure-storage-file-datalake boto3

Configuration
-------------
All settings are read from environment variables (see CONFIG section below).
You can also create a  .env  file and load it with python-dotenv if you prefer.
"""

import gzip
import io
import logging
import os
from datetime import timezone

import boto3
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIG  –  override any value via environment variable
# ---------------------------------------------------------------------------

# ── Azure ──────────────────────────────────────────────────────────────────
ADLS_ACCOUNT_NAME   = os.environ.get("ADLS_ACCOUNT_NAME", "")        # e.g. myadlsaccount
ADLS_CONTAINER_NAME = os.environ.get("ADLS_CONTAINER_NAME", "")      # filesystem / container name
ADLS_PATH_PREFIX    = os.environ.get("ADLS_PATH_PREFIX", "")         # sub-path to start from (optional)

# Auth – leave ADLS_TENANT_ID empty to fall back to DefaultAzureCredential
# (Managed Identity, az login, env vars …)
ADLS_TENANT_ID      = os.environ.get("ADLS_TENANT_ID", "")
ADLS_CLIENT_ID      = os.environ.get("ADLS_CLIENT_ID", "")
ADLS_CLIENT_SECRET  = os.environ.get("ADLS_CLIENT_SECRET", "")

# ── AWS ────────────────────────────────────────────────────────────────────
S3_BUCKET_NAME      = os.environ.get("S3_BUCKET_NAME", "")           # destination bucket
S3_REGION           = os.environ.get("S3_REGION", "eu-west-1")
AWS_ACCESS_KEY_ID   = os.environ.get("AWS_ACCESS_KEY_ID", "")        # leave empty to use instance role / ~/.aws
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

# ── Behaviour ──────────────────────────────────────────────────────────────
CHUNK_SIZE          = int(os.environ.get("CHUNK_SIZE", 8 * 1024 * 1024))   # 8 MB read chunks
DRY_RUN             = os.environ.get("DRY_RUN", "false").lower() == "true" # set to "true" to only log


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_adls_client() -> DataLakeServiceClient:
    """Return an authenticated DataLakeServiceClient."""
    account_url = f"https://{ADLS_ACCOUNT_NAME}.dfs.core.windows.net"

    if ADLS_TENANT_ID and ADLS_CLIENT_ID and ADLS_CLIENT_SECRET:
        log.info("Azure auth: service-principal credential")
        credential = ClientSecretCredential(
            tenant_id=ADLS_TENANT_ID,
            client_id=ADLS_CLIENT_ID,
            client_secret=ADLS_CLIENT_SECRET,
        )
    else:
        log.info("Azure auth: DefaultAzureCredential (Managed Identity / az login / env)")
        credential = DefaultAzureCredential()

    return DataLakeServiceClient(account_url=account_url, credential=credential)


def _build_s3_client():
    """Return a boto3 S3 client."""
    kwargs = {"region_name": S3_REGION}
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        kwargs["aws_access_key_id"]     = AWS_ACCESS_KEY_ID
        kwargs["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
    return boto3.client("s3", **kwargs)


def _iter_files(fs_client, path: str):
    """
    Yield every *file* path under `path` in the ADLS filesystem,
    together with its properties dict.
    Uses recursive listing via get_paths(recursive=True).
    """
    log.info("Listing ADLS paths under '%s' …", path or "/")
    paths = fs_client.get_paths(path=path or "/", recursive=True)
    for p in paths:
        if not p.is_directory:
            yield p


def _last_modified_date(path_item) -> str:
    """
    Return the UTC last-modified date of an ADLS path item as YYYY-MM-DD.
    Falls back to '1970-01-01' if the property is missing.
    """
    lm = getattr(path_item, "last_modified", None)
    if lm is None:
        return "1970-01-01"
    if lm.tzinfo is None:
        lm = lm.replace(tzinfo=timezone.utc)
    return lm.astimezone(timezone.utc).strftime("%Y-%m-%d")


def _s3_key(adls_path: str, date_prefix: str, compress: bool) -> str:
    """
    Build the destination S3 key:
        <date_prefix>/<adls_path>[.gz if we compress]

    Leading slashes are stripped from adls_path so the key is clean.
    """
    clean_path = adls_path.lstrip("/")
    key = f"{date_prefix}/{clean_path}"
    if compress:
        key += ".gz"
    return key


class _GzipStreamingBody(io.RawIOBase):
    """
    A readable stream that compresses data from `source_stream` on-the-fly
    using gzip.  Boto3's upload_fileobj reads from this wrapper.
    """

    def __init__(self, source_stream, chunk_size: int = CHUNK_SIZE):
        self._buf       = io.BytesIO()
        self._gzip      = gzip.GzipFile(fileobj=self._buf, mode="wb")
        self._source    = source_stream
        self._chunk_size = chunk_size
        self._done      = False

    def readable(self):
        return True

    def readinto(self, b):
        """Fill buffer `b` with compressed bytes."""
        while not self._done and self._buf.tell() < len(b):
            chunk = self._source.read(self._chunk_size)
            if chunk:
                self._gzip.write(chunk)
            else:
                self._gzip.close()
                self._done = True

        compressed = self._buf.getvalue()
        n = min(len(b), len(compressed))
        b[:n] = compressed[:n]
        # keep remaining bytes for the next call
        self._buf = io.BytesIO(compressed[n:])
        self._buf.seek(0, 2)          # move write pointer to end
        return n


# ---------------------------------------------------------------------------
# Main copy routine
# ---------------------------------------------------------------------------

def copy_adls_to_s3():
    _validate_config()

    adls_client = _build_adls_client()
    s3_client   = _build_s3_client()

    fs_client = adls_client.get_file_system_client(file_system=ADLS_CONTAINER_NAME)

    copied  = 0
    skipped = 0
    errors  = 0

    for path_item in _iter_files(fs_client, ADLS_PATH_PREFIX):
        adls_path   = path_item.name
        date_prefix = _last_modified_date(path_item)
        is_gz       = adls_path.lower().endswith(".gz")
        compress    = not is_gz
        key         = _s3_key(adls_path, date_prefix, compress)

        action = "STREAM    " if is_gz else "COMPRESS  "
        log.info("%s  adls://%s  →  s3://%s/%s", action, adls_path, S3_BUCKET_NAME, key)

        if DRY_RUN:
            skipped += 1
            continue

        try:
            file_client = fs_client.get_file_client(adls_path)
            download    = file_client.download_file()

            # download_file() returns a StorageStreamDownloader.
            # We wrap it in a regular BytesIO pipeline so boto3 can stream it.
            raw_stream = download.chunks()          # iterator of bytes chunks

            class _ChunkStream(io.RawIOBase):
                """Thin wrapper that turns the chunks iterator into a readable stream."""
                def __init__(self):
                    self._pending = b""
                    self._iter    = raw_stream

                def readable(self):
                    return True

                def readinto(self, b):
                    while len(self._pending) < len(b):
                        try:
                            self._pending += next(self._iter)
                        except StopIteration:
                            break
                    n = min(len(b), len(self._pending))
                    b[:n] = self._pending[:n]
                    self._pending = self._pending[n:]
                    return n

            readable = _ChunkStream()

            if compress:
                upload_stream = io.BufferedReader(_GzipStreamingBody(readable))
            else:
                upload_stream = io.BufferedReader(readable)

            s3_client.upload_fileobj(
                upload_stream,
                S3_BUCKET_NAME,
                key,
                ExtraArgs={"ContentType": "application/gzip"},
            )
            copied += 1

        except Exception as exc:  # noqa: BLE001
            log.error("FAILED    adls://%s  –  %s", adls_path, exc)
            errors += 1

    log.info("Done.  copied=%d  skipped(dry-run)=%d  errors=%d", copied, skipped, errors)


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

def _validate_config():
    missing = []
    for name, value in [
        ("ADLS_ACCOUNT_NAME",   ADLS_ACCOUNT_NAME),
        ("ADLS_CONTAINER_NAME", ADLS_CONTAINER_NAME),
        ("S3_BUCKET_NAME",      S3_BUCKET_NAME),
    ]:
        if not value:
            missing.append(name)
    if missing:
        raise EnvironmentError(
            f"The following required environment variables are not set: {', '.join(missing)}"
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    copy_adls_to_s3()