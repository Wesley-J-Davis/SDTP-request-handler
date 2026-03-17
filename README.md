# SDTP-request-handler

Science Data Transfer Protocol is a system used by NASA DAACs that uses http protocols to request, retrieve, and acknowledge delivery of data and is meant to be a replacement for the PAN/PDR protocol, which SDTP can be built on top of if required, though not recommended. 

This repository seeks to implement this process for a single job at first, perhaps expanding later.

A GET request to the server's sdtp directory will (on success) return a JSON file containing filenames and select metadata for the requested information.

This metadata will be parsed for the fileid field, which will be used in a second https GET request to actually retrieve the file.

Once the file is retrieved and validated a thid http request is sent to delete the fileid on the remote side. 
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

