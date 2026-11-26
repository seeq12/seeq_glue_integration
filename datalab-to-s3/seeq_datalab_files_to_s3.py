"""
AWS Glue (Python shell) job: copy files from a Seeq Data Lab project folder to S3.

Flow
----
1. Log in to Seeq with an Application identity API Key via POST /api/auth/login.
   The response's ``x-sq-auth`` header is the session token used for all later calls.
2. Parse the Data Lab project ID from the supplied Data Lab URL.
3. List the folder through the Data Lab (Jupyter) contents API:
       GET {server}/data-lab/{projectId}/api/contents/{folder}
4. Download each file (base64 via the contents API) and put it in S3 under
       s3://{bucket}/{prefix}/{relative path inside the folder}
5. Log out.

Glue job parameters (pass as ``--NAME value``)
---------------------------------------------
Required:
  --DATALAB_URL      Any URL inside the project, e.g.
                     https://acme.seeq.site/data-lab/0EF1D2C3-.../notebooks/Main.ipynb
  --S3_BUCKET        Destination bucket
Credentials (one of):
  --SEEQ_SECRET_ID   Secrets Manager secret holding JSON {"access_key": "...", "access_key_password": "..."}
                     (recommended)
  --SEEQ_ACCESS_KEY + --SEEQ_ACCESS_KEY_PASSWORD   (plain job args; fine for testing only)
Optional:
  --SOURCE_FOLDER    Folder in the project to copy (default: files)
  --S3_PREFIX        Key prefix in the bucket (default: seeq-datalab/{projectId}/{SOURCE_FOLDER})
  --RECURSIVE        true/false - descend into sub-folders (default: true)
  --VERIFY_SSL       true/false (default: true)

The Application identity API Key's user needs Read access to the Data Lab project (a Read-only share is enough).
The Glue job role needs s3:PutObject on the destination (and secretsmanager:GetSecretValue if SEEQ_SECRET_ID is used).
"""
import base64
import json
import logging
import re
import sys
import time
from typing import Dict, Iterator, Optional, Tuple
from urllib.parse import quote

import boto3
import requests

LOG = logging.getLogger("seeq_datalab_to_s3")
# force=True: Glue's Python shell runner configures the root logger first, which would otherwise hide INFO lines.
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout,
                    force=True)

PROJECT_URL_PATTERN = re.compile(
    r"^(?P<base>https?://[^/]+(?:/[^/]+)*?)/data-lab/"
    r"(?P<project>[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12})",
    re.IGNORECASE,
)

# The Data Lab container may still be spinning up on the first request; these are retried.
RETRYABLE_STATUS = {502, 503, 504}
MAX_ATTEMPTS = 6
REQUEST_TIMEOUT_SECONDS = 300
# Seeq REST endpoints (/api/...) require the vendor media type; the Data Lab contents API is plain Jupyter JSON.
SEEQ_API_HEADERS = {"Content-Type": "application/vnd.seeq.v1+json", "Accept": "application/vnd.seeq.v1+json"}


# --------------------------------------------------------------------------------------------------
# Arguments
# --------------------------------------------------------------------------------------------------
def _read_args() -> Dict[str, str]:
    required = ["DATALAB_URL", "S3_BUCKET"]
    optional = ["SEEQ_SECRET_ID", "SEEQ_ACCESS_KEY", "SEEQ_ACCESS_KEY_PASSWORD", "SOURCE_FOLDER", "S3_PREFIX",
                "RECURSIVE", "VERIFY_SSL"]
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


def parse_datalab_url(url: str) -> Tuple[str, str]:
    """Return (server base URL, project ID) from any URL inside a Data Lab project."""
    match = PROJECT_URL_PATTERN.match(url.strip())
    if not match:
        raise SystemExit(f"'{url}' is not a Data Lab project URL. Expected something like "
                         f"https://<server>/data-lab/<PROJECT-ID>/...")
    return match.group("base").rstrip("/"), match.group("project").upper()


# --------------------------------------------------------------------------------------------------
# Seeq client
# --------------------------------------------------------------------------------------------------
class SeeqDataLabClient:
    def __init__(self, server_url: str, project_id: str, verify_ssl: bool = True):
        self.server_url = server_url
        self.project_id = project_id
        self.session = requests.Session()
        self.session.verify = verify_ssl
        self.session.headers.update({"Accept": "application/json"})

    @property
    def contents_url(self) -> str:
        return f"{self.server_url}/data-lab/{self.project_id}/api/contents"

    def login(self, access_key: str, password: str) -> None:
        # With an Application identity API Key, the key name is the username and the provider fields are omitted
        # (same as SPy).
        resp = self.session.post(f"{self.server_url}/api/auth/login",
                                 data=json.dumps({"username": access_key, "password": password}),
                                 headers=SEEQ_API_HEADERS, timeout=60)
        if resp.status_code == 401:
            raise SystemExit(f"Application identity API Key '{access_key}' was rejected. Check the key/password; if the key "
                             f"has expired or been revoked, create a new one and update the secret.")
        resp.raise_for_status()
        token = resp.headers.get("x-sq-auth")
        if not token:
            raise SystemExit("Login succeeded but no x-sq-auth header was returned; is a proxy stripping headers?")
        self.session.headers["x-sq-auth"] = token
        csrf = resp.headers.get("x-sq-csrf") or self.session.cookies.get("sq-csrf")
        if csrf:
            self.session.headers["x-sq-csrf"] = csrf
        LOG.info("Logged in to %s with API Key %s", self.server_url, access_key)

    def logout(self) -> None:
        try:
            self.session.delete(f"{self.server_url}/api/auth/logout", headers=SEEQ_API_HEADERS, timeout=30)
        except requests.RequestException:
            LOG.warning("Logout failed; the session will expire on its own")

    def _get(self, path: str, params: Dict[str, str]) -> dict:
        url = f"{self.contents_url}/{quote(path)}"
        for attempt in range(1, MAX_ATTEMPTS + 1):
            resp = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            if resp.status_code in RETRYABLE_STATUS and attempt < MAX_ATTEMPTS:
                wait = min(2 ** attempt, 30)
                LOG.info("Data Lab returned %s for %s (project may be starting); retrying in %ss",
                         resp.status_code, path, wait)
                time.sleep(wait)
                continue
            if resp.status_code == 403:
                raise SystemExit(f"Access denied to Data Lab project {self.project_id}. Share the project with the "
                                 f"API Key's application identity (Read is sufficient). Server said: {resp.text[:200]}")
            if resp.status_code == 404:
                raise FileNotFoundError(f"'{path}' not found in Data Lab project {self.project_id}")
            resp.raise_for_status()
            return resp.json()
        raise RuntimeError(f"Gave up on {url} after {MAX_ATTEMPTS} attempts")

    def iter_files(self, folder: str, recursive: bool = True) -> Iterator[dict]:
        """Yield contents-API models (without content) for every file under ``folder``."""
        listing = self._get(folder, {"type": "directory", "content": "1"})
        for entry in sorted(listing.get("content") or [], key=lambda e: e["path"]):
            if entry["type"] == "directory":
                if recursive:
                    yield from self.iter_files(entry["path"], recursive)
            else:  # "file" or "notebook"
                yield entry

    def download(self, path: str) -> bytes:
        # type=file + format=base64 returns raw bytes for any file, notebooks included.
        model = self._get(path, {"type": "file", "format": "base64", "content": "1"})
        return base64.b64decode(model["content"])


# --------------------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------------------
def main() -> None:
    args = _read_args()
    server_url, project_id = parse_datalab_url(args["DATALAB_URL"])
    folder = args.get("SOURCE_FOLDER", "files").strip("/")
    bucket = args["S3_BUCKET"]
    prefix = args.get("S3_PREFIX", f"seeq-datalab/{project_id}/{folder}").strip("/")
    recursive = _as_bool(args.get("RECURSIVE"), True)
    verify_ssl = _as_bool(args.get("VERIFY_SSL"), True)

    access_key, password = _resolve_credentials(args)
    s3 = boto3.client("s3")
    client = SeeqDataLabClient(server_url, project_id, verify_ssl)
    client.login(access_key, password)

    copied, failed = 0, []
    try:
        for entry in client.iter_files(folder, recursive):
            relative = entry["path"][len(folder):].lstrip("/")
            key = f"{prefix}/{relative}" if prefix else relative
            try:
                data = client.download(entry["path"])
                s3.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=data,
                    ContentType=entry.get("mimetype") or "application/octet-stream",
                    Metadata={
                        "seeq-project-id": project_id,
                        "seeq-path": entry["path"],
                        "seeq-last-modified": entry.get("last_modified", ""),
                    },
                )
                copied += 1
                LOG.info("Copied %s (%d bytes) -> s3://%s/%s", entry["path"], len(data), bucket, key)
            except Exception as e:  # keep going; report at the end
                LOG.error("Failed to copy %s: %s", entry["path"], e)
                failed.append(entry["path"])
    finally:
        client.logout()

    LOG.info("Done: %d file(s) copied, %d failed", copied, len(failed))
    if failed:
        raise SystemExit(f"{len(failed)} file(s) failed to copy: {failed}")


if __name__ == "__main__":
    main()
