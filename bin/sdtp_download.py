#!/usr/bin/env python3
"""
sdtp_download.py

SDTP (Science Data Transfer Protocol) download utility.
Driven by a Perl config/job management system that passes all
parameters as command-line arguments.

Usage:
    python sdtp_download.py \
        --stream MODAPS_AQUA_NRT \
        --maxfile 8 \
        --cert /path/to/cert.pem \
        --key /path/to/private.key \
        --output-dir /path/to/output

Workflow:
    1. Discover available files via GET /sdtp/v1/files
    2. For each file:
        a. Download via GET /sdtp/v1/files/{fileid}
        b. Verify MD5 checksum
        c. Acknowledge via DELETE /sdtp/v1/files/{fileid}
    3. Repeat until queue is empty
"""

import argparse
import hashlib
import os
import sys
import requests


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL     = "https://modaps.modaps.eosdis.nasa.gov/sdtp/v1"
MAX_RETRIES  = 2


# ---------------------------------------------------------------------------
# Argument Parsing
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="SDTP file download utility (driven by Perl job manager)"
    )
    parser.add_argument(
        "--stream",
        required=True,
        help="SDTP stream name (e.g. MODAPS_AQUA_NRT)"
    )
    parser.add_argument(
        "--maxfile",
        type=int,
        required=True,
        help="Maximum number of files to request per batch"
    )
    parser.add_argument(
        "--cert",
        required=True,
        help="Path to the client certificate file"
    )
    parser.add_argument(
        "--key",
        required=True,
        help="Path to the client private key file"
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory to save downloaded files to"
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def log_info(msg):
    """Write an info-level message to stdout."""
    print(f"[INFO]  {msg}", flush=True)


def log_error(msg):
    """Write an error-level message to stderr."""
    print(f"[ERROR] {msg}", file=sys.stderr, flush=True)


def verify_md5(filepath, expected_checksum):
    """
    Compute the MD5 of a file and compare against the expected checksum.

    Args:
        filepath (str):          Path to the downloaded file.
        expected_checksum (str): Checksum string from SDTP response.
                                 Expected format: "md5:<hex_digest>"

    Returns:
        bool: True if checksum matches, False otherwise.
    """
    # Strip the "md5:" prefix if present
    expected_hex = expected_checksum.lower()
    if expected_hex.startswith("md5:"):
        expected_hex = expected_hex[4:]

    md5 = hashlib.md5()
    try:
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                md5.update(chunk)
    except OSError as e:
        log_error(f"Could not read file for MD5 verification: {filepath} — {e}")
        return False

    computed_hex = md5.hexdigest()
    if computed_hex != expected_hex:
        log_error(
            f"MD5 mismatch for {filepath}: "
            f"expected={expected_hex}, got={computed_hex}"
        )
        return False

    return True


def ensure_output_dir(output_dir):
    """Create the output directory if it does not already exist."""
    try:
        os.makedirs(output_dir, exist_ok=True)
    except OSError as e:
        log_error(f"Failed to create output directory '{output_dir}': {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# SDTP Operations
# ---------------------------------------------------------------------------

def discover_files(session, stream, maxfile):
    """
    Step 1: Discover available files in the SDTP queue.

    Args:
        session  (requests.Session): Authenticated session.
        stream   (str):              SDTP stream name.
        maxfile  (int):              Max files to return per batch.

    Returns:
        list[dict]: List of file metadata dicts, or empty list if queue
                    is empty. Exits with code 1 on HTTP/network error.
    """
    url    = f"{BASE_URL}/files"
    params = {"stream": stream, "maxfile": maxfile}

    try:
        response = session.get(url, params=params)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        log_error(f"Failed to discover files: {e}")
        sys.exit(1)

    try:
        data = response.json()
    except ValueError as e:
        log_error(f"Failed to parse discovery response as JSON: {e}")
        sys.exit(1)

    files = data.get("files") or []
    return files


def download_file(session, fileid, filename, output_dir):
    """
    Step 2a/2b: Download a single file from the SDTP queue.

    Args:
        session    (requests.Session): Authenticated session.
        fileid     (int):              SDTP file ID.
        filename   (str):              Original filename from SDTP metadata.
        output_dir (str):              Directory to save the file.

    Returns:
        str | None: Full path to the saved file on success, None on failure.
    """
    url      = f"{BASE_URL}/files/{fileid}"
    filepath = os.path.join(output_dir, filename)

    try:
        with session.get(url, stream=True) as response:
            response.raise_for_status()
            with open(filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
    except requests.exceptions.RequestException as e:
        log_error(f"HTTP error downloading fileid={fileid} ({filename}): {e}")
        # Clean up partial file if it exists
        if os.path.exists(filepath):
            os.remove(filepath)
        return None
    except OSError as e:
        log_error(f"File system error saving fileid={fileid} ({filename}): {e}")
        return None

    return filepath


def acknowledge_file(session, fileid, filename):
    """
    Step 2c/2d: Acknowledge successful receipt of a file (DELETE).

    Args:
        session  (requests.Session): Authenticated session.
        fileid   (int):              SDTP file ID.
        filename (str):              Filename (for logging only).

    Returns:
        bool: True on success, False on failure.
    """
    url = f"{BASE_URL}/files/{fileid}"

    try:
        response = session.delete(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        log_error(f"Failed to acknowledge fileid={fileid} ({filename}): {e}")
        return False

    return True


# ---------------------------------------------------------------------------
# Core Download Loop
# ---------------------------------------------------------------------------

def process_file(session, file_meta, output_dir):
    """
    Attempt to download and acknowledge a single file, with up to
    MAX_RETRIES retry attempts on failure.

    Args:
        session    (requests.Session): Authenticated session.
        file_meta  (dict):             File metadata from SDTP discovery.
        output_dir (str):              Directory to save the file.
    """
    fileid   = file_meta["fileid"]
    filename = file_meta["name"]
    checksum = file_meta["checksum"]
    size     = file_meta.get("size", "unknown")

    log_info(f"Processing: {filename} (fileid={fileid}, size={size} bytes)")

    for attempt in range(1, MAX_RETRIES + 2):  # attempts: 1, 2, 3
        if attempt > 1:
            log_info(f"  Retry {attempt - 1}/{MAX_RETRIES} for {filename}")

        # --- Download ---
        filepath = download_file(session, fileid, filename, output_dir)
        if filepath is None:
            if attempt <= MAX_RETRIES:
                continue
            else:
                log_error(
                    f"Download failed after {MAX_RETRIES} retries: "
                    f"{filename} (fileid={fileid})"
                )
                sys.exit(1)

        # --- MD5 Verification ---
        log_info(f"  Verifying MD5 for {filename} ...")
        if not verify_md5(filepath, checksum):
            # Remove corrupted file before retry
            if os.path.exists(filepath):
                os.remove(filepath)
            if attempt <= MAX_RETRIES:
                continue
            else:
                log_error(
                    f"MD5 verification failed after {MAX_RETRIES} retries: "
                    f"{filename} (fileid={fileid})"
                )
                sys.exit(1)

        log_info(f"  MD5 verified for {filename}")

        # --- Acknowledge ---
        if not acknowledge_file(session, fileid, filename):
            if attempt <= MAX_RETRIES:
                # Remove file so we re-download cleanly on retry
                if os.path.exists(filepath):
                    os.remove(filepath)
                continue
            else:
                log_error(
                    f"Acknowledgement failed after {MAX_RETRIES} retries: "
                    f"{filename} (fileid={fileid})"
                )
                sys.exit(1)

        log_info(f"  Successfully downloaded and acknowledged: {filename}")
        return  # Success — exit retry loop


def run(args):
    """
    Main execution loop.

    Continuously pulls batches of files from the SDTP queue until
    the queue is empty.
    """
    ensure_output_dir(args.output_dir)

    # Build a persistent session with mutual TLS auth
    session        = requests.Session()
    session.cert   = (args.cert, args.key)

    log_info(f"Starting SDTP download — stream={args.stream}, maxfile={args.maxfile}")
    log_info(f"Output directory: {args.output_dir}")

    total_downloaded = 0
    batch_number     = 0

    while True:
        batch_number += 1
        log_info(f"--- Batch {batch_number}: Discovering files ---")

        files = discover_files(session, args.stream, args.maxfile)

        # Empty or null files array signals queue is exhausted
        if not files:
            log_info("Queue is empty — no more files to download.")
            break

        log_info(f"  Found {len(files)} file(s) in batch {batch_number}")

        for file_meta in files:
            process_file(session, file_meta, args.output_dir)
            total_downloaded += 1

    log_info(f"Done. Total files downloaded this run: {total_downloaded}")


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    args = parse_args()
    run(args)
