# SDTP-request-handler

Science Data Transfer Protocol is a system used by NASA DAACs that uses http protocols to request, retrieve, and acknowledge delivery of data and is meant to be a replacement for the PAN/PDR protocol, which SDTP can be built on top of if required, though not recommended. 

This repository seeks to implement this process for a single job at first, perhaps expanding later.

A GET request to the server's sdtp directory will (on success) return a JSON file containing filenames and select metadata for the requested information.

This metadata will be parsed for the fileid field, which will be used in a second https GET request to actually retrieve the file.

Once the file is retrieved and validated a thid http request is sent to delete the fileid on the remote side. 
