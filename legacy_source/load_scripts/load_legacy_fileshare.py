#!/usr/bin/env python3
"""
load_legacy_fileshare.py

Copies Synthea's FHIR/C-CDA export files and the NIH ChestX-ray14 images
into the legacy MinIO file share (the `legacy-fileshare` bucket created by
legacy_source/docker-compose.yml's minio-init step).

This deliberately mirrors how a real legacy on-prem file share would hold
these -- flat directories, filename is the only "key", no metadata table
(see migration/schema_mapping.md: "...no metadata table in source" -- that
metadata table gets built later, during actual migration into bronze, not
here). Every file is uploaded as <bucket>/<prefix>/<original filename>,
with any source subdirectory nesting dropped -- e.g. the NIH Kaggle
download nests images under images_001/images/, images_002/images/, etc.,
but every filename is already globally unique across the dataset, so
flattening is both safe and realistic.

Uploads:
  --fhir-dir   -> legacy-fileshare/fhir/<filename>
  --ccda-dir   -> legacy-fileshare/ccda/<filename>
  --images-dir -> legacy-fileshare/images/<filename>   (recursive)

Any combination of the three flags can be given; omit the ones you don't
need for a given run.

Design notes:
  - Resumable: before uploading, checks whether an object with the same
    key and the same byte size already exists in the bucket, and skips it
    if so. Safe to Ctrl-C and re-run -- useful given the ~42GB image set
    will take a while on a home connection... except this is all local, so
    it's more about surviving an interrupted run than network flakiness.
  - Concurrent uploads (default 12 worker threads) -- uploading 112,000+
    individual image files one at a time would take a very long time
    otherwise.
  - Talks to MinIO via its S3-compatible API using boto3, the same way
    real code would talk to actual S3 -- no MinIO-specific SDK needed.

Usage:
    python3 load_legacy_fileshare.py \
        --fhir-dir /path/to/fhir_ccda_run/output/fhir \
        --ccda-dir /path/to/fhir_ccda_run/output/ccda \
        --images-dir /path/to/nih_chest_xrays

Connection defaults match legacy_source/docker-compose.yml (localhost:9000,
meridian_admin/changeme_local_only, bucket legacy-fileshare).
"""
import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError


def get_s3_client(endpoint, access_key, secret_key):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def object_exists_with_same_size(s3, bucket, key, local_size):
    """Returns True if `key` already exists in `bucket` with the same size
    as the local file -- used to make re-runs skip already-uploaded files."""
    try:
        resp = s3.head_object(Bucket=bucket, Key=key)
        return resp["ContentLength"] == local_size
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def upload_one(s3, bucket, local_path, key):
    size = local_path.stat().st_size
    if object_exists_with_same_size(s3, bucket, key, size):
        return "skipped"
    s3.upload_file(str(local_path), bucket, key)
    return "uploaded"


def collect_files(root_dir):
    root = Path(root_dir)
    if not root.is_dir():
        return []
    return [p for p in root.rglob("*") if p.is_file()]


def sync_directory(s3, bucket, local_dir, prefix, workers):
    files = collect_files(local_dir)
    total = len(files)
    if total == 0:
        print(f"  [warn] no files found under {local_dir}")
        return

    print(f"  found {total} files under {local_dir} -> uploading to {bucket}/{prefix}/")
    uploaded = skipped = errors = done = 0

    def _task(local_path):
        key = f"{prefix}/{local_path.name}"
        try:
            return upload_one(s3, bucket, local_path, key)
        except Exception as e:
            return f"error: {e}"

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_task, p): p for p in files}
        for fut in as_completed(futures):
            done += 1
            result = fut.result()
            if result == "uploaded":
                uploaded += 1
            elif result == "skipped":
                skipped += 1
            else:
                errors += 1
                print(f"  [error] {futures[fut].name}: {result}")
            if done % 2000 == 0 or done == total:
                print(f"  progress: {done}/{total} (uploaded={uploaded}, skipped={skipped}, errors={errors})")

    print(f"  done: uploaded={uploaded}, skipped={skipped}, errors={errors}, total={total}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fhir-dir", default=None, help="Synthea FHIR JSON output directory")
    parser.add_argument("--ccda-dir", default=None, help="Synthea C-CDA XML output directory")
    parser.add_argument("--images-dir", default=None, help="NIH ChestX-ray14 images directory (searched recursively)")
    parser.add_argument("--endpoint", default=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"))
    parser.add_argument("--access-key", default=os.environ.get("MINIO_ROOT_USER", "meridian_admin"))
    parser.add_argument("--secret-key", default=os.environ.get("MINIO_ROOT_PASSWORD", "changeme_local_only"))
    parser.add_argument("--bucket", default="legacy-fileshare")
    parser.add_argument("--workers", type=int, default=12)
    args = parser.parse_args()

    if not any([args.fhir_dir, args.ccda_dir, args.images_dir]):
        print("ERROR: provide at least one of --fhir-dir / --ccda-dir / --images-dir")
        sys.exit(1)

    print(f"Connecting to MinIO at {args.endpoint} ...")
    s3 = get_s3_client(args.endpoint, args.access_key, args.secret_key)

    try:
        s3.head_bucket(Bucket=args.bucket)
    except ClientError:
        print(f"ERROR: bucket '{args.bucket}' not found at {args.endpoint} -- is MinIO running? (docker compose ps)")
        sys.exit(1)

    if args.fhir_dir:
        print(f"[sync] FHIR: {args.fhir_dir}")
        sync_directory(s3, args.bucket, args.fhir_dir, "fhir", args.workers)
    if args.ccda_dir:
        print(f"[sync] C-CDA: {args.ccda_dir}")
        sync_directory(s3, args.bucket, args.ccda_dir, "ccda", args.workers)
    if args.images_dir:
        print(f"[sync] Images: {args.images_dir}")
        sync_directory(s3, args.bucket, args.images_dir, "images", args.workers)

    print("\nDone.")


if __name__ == "__main__":
    main()
