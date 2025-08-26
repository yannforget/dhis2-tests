import hashlib
import logging
import os
import subprocess
from pathlib import Path
from time import sleep

import httpx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


DHIS2_VERSION = os.getenv("DHIS2_VERSION", "2.41")
DBDUMP_URL = os.getenv(
    "DBDUMP_URL",
    "https://storage.googleapis.com/hexa-public-assets/openhexa-toolbox/dhis2-databases/dhis2-db-sierra-leone-analytics_2.41.sql.gz",
)
DBDUMP_HASH = hashlib.md5(DBDUMP_URL.encode()).hexdigest()[:8]
DBDUMP_DIR = Path(f"./db-dump_{DBDUMP_HASH}")
POSTGRES_VOLUME = f"dhis2_{DBDUMP_HASH}_postgres-data"


def run(cmd: list[str]) -> subprocess.CompletedProcess:
    logger.info(f"Running command: {' '.join(cmd)}")
    return subprocess.run(
        cmd,
        check=True,
        env={
            "DHIS2_VERSION": DHIS2_VERSION,
            "DBDUMP_URL": DBDUMP_URL,
            "DBDUMP_HASH": DBDUMP_HASH,
        },
        capture_output=True,
        text=True,
    )


def restore_from_cache(cache_fp: Path, volume: str):
    """Restore postgres-data volume from cache."""
    run(["docker", "volume", "create", volume])
    run(
        [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{volume}:/data",
            "-v",
            f"{cache_fp.parent.absolute()}:/cache",
            "busybox",
            "tar",
            "xzf",
            f"/cache/{cache_fp.name}",
            "-C",
            "/data",
        ]
    )


def download_dump(dump_dir: Path, dump_url: str) -> Path:
    """Download database dump."""
    dump_dir.mkdir(parents=True, exist_ok=True)
    dump_fp = dump_dir / "dump.sql.gz"
    if dump_fp.exists():
        logger.info("Database dump already exists, skipping download")
        return dump_fp
    run(["wget", "-O", dump_fp.absolute().as_posix(), dump_url])
    return dump_fp


def start_db(volume: str):
    """Start db container."""
    run(["docker", "compose", "down", "-v"])
    run(["docker", "volume", "create", volume])
    run(["docker", "compose", "up", "-d", "db"])

    for _ in range(360):
        try:
            run(
                [
                    "docker",
                    "compose",
                    "exec",
                    "-T",
                    "db",
                    "pg_isready",
                    "-U",
                    "dhis",
                    "-d",
                    "dhis",
                ]
            )
            break
        except subprocess.CalledProcessError:
            sleep(3)


def wait_for_import():
    """Wait for data import to be complete."""
    r = run(
        ["docker", "compose", "ps", "-q", "db"],
    )
    container_id = r.stdout.strip()

    if not container_id:
        raise RuntimeError("DB container not found")

    for _ in range(360):
        p = run(
            ["docker", "logs", container_id, "--tail=100"],
        )

        if "database system is ready to accept connections" in p.stderr:
            return

        sleep(3)

    raise RuntimeError("Database import did not complete in time")


def cache_db(cache_file: Path, volume: str):
    """Cache the postgres-data volume."""
    run(["docker", "compose", "stop", "db"])
    run(
        [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{volume}:/data",
            "-v",
            f"{cache_file.parent.absolute()}:/cache",
            "busybox",
            "tar",
            "czf",
            f"/cache/{cache_file.name}",
            "-C",
            "/data",
            ".",
        ]
    )


def wait_for_dhis2():
    """Wait for DHIS2 web app to be available."""
    auth = httpx.BasicAuth("admin", "district")
    client = httpx.Client(timeout=30, auth=auth)
    for _ in range(360):
        try:
            r = client.get("http://localhost:8080/api/system/info")
            r.raise_for_status()
            return
        except httpx.HTTPError:
            sleep(3)
    raise RuntimeError("DHIS2 did not start in time")


def start_services(dbdump_url: str):
    """Setup DHIS2 database."""
    dbdump_hash = hashlib.md5(dbdump_url.encode()).hexdigest()[:8]
    postgres_volume_name = f"dhis2_{dbdump_hash}_postgres-data"
    dbdump_dir = Path(f"./db-dump_{dbdump_hash}")
    cache_fp = Path(f"./cache/postgres-{dbdump_hash}.tar.gz")
    if cache_fp.exists():
        logger.info("Restoring database from cache")
        restore_from_cache(cache_fp, postgres_volume_name)
        run(["docker", "compose", "up", "-d"])
    else:
        logger.info("No cache found, performing fresh import")
        download_dump(dbdump_dir, dbdump_url)
        logger.info("Starting database import")
        start_db(postgres_volume_name)
        logger.info("Waiting for import to complete")
        wait_for_import()
        logger.info("Cache database state")
        cache_db(cache_fp, postgres_volume_name)

    logger.info("Starting all services")
    run(["docker", "compose", "up", "-d"])

    logger.info("Waiting for DHIS2")
    wait_for_dhis2()

    logger.info("DHIS2 is up and running")


if __name__ == "__main__":
    start_services(DBDUMP_URL)
