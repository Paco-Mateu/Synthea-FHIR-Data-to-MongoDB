import argparse
import os
import shutil
import urllib.request
import zipfile


DEFAULT_DOWNLOAD_URL = (
    "https://synthetichealth.github.io/synthea-sample-data/"
    "downloads/synthea_sample_data_fhir_r4_sep2019.zip"
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download and extract the official public Synthea FHIR R4 sample-data archive."
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_DOWNLOAD_URL,
        help="ZIP URL to download. Defaults to the maintained Synthea sample-data FHIR R4 archive.",
    )
    parser.add_argument(
        "--zip-path",
        default=os.path.join("Dataset", "downloads", "synthea_sample_data_fhir_r4_sep2019.zip"),
        help="Destination ZIP path.",
    )
    parser.add_argument(
        "--extract-dir",
        default=os.path.join("Dataset", "source_fhir_r4_sep2019"),
        help="Directory where the ZIP contents will be extracted.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite any existing ZIP or extracted directory.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    os.makedirs(os.path.dirname(args.zip_path), exist_ok=True)
    if args.force and os.path.exists(args.zip_path):
        os.remove(args.zip_path)
    if args.force and os.path.exists(args.extract_dir):
        shutil.rmtree(args.extract_dir)

    if not os.path.exists(args.zip_path):
        urllib.request.urlretrieve(args.url, args.zip_path)

    os.makedirs(args.extract_dir, exist_ok=True)
    with zipfile.ZipFile(args.zip_path) as archive:
        archive.extractall(args.extract_dir)
        bundle_count = len([name for name in archive.namelist() if name.endswith(".json")])

    print(
        {
            "downloadUrl": args.url,
            "zipPath": os.path.abspath(args.zip_path),
            "extractDir": os.path.abspath(args.extract_dir),
            "bundleCount": bundle_count,
        }
    )


if __name__ == "__main__":
    main()
