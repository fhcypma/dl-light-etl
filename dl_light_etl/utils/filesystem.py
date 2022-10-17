import logging
import os
import pathlib
from smart_open import open


def write_text_file(path: str, content: str):
    params = None
    if not path.startswith("s3://"):
        logging.debug(f"Creating dir {pathlib.Path(path).parent}")
        os.makedirs(pathlib.Path(path).parent, exist_ok=True)
    else:
        params = {
            "client_kwargs": {
                "S3.Client.put_object": {"ACL": "bucket-owner-full-control"}
            }
        }
    with open(path, "w", transport_params=params) as f:
        logging.debug(f"Writing {path}")
        f.write(content)


def read_text_file(path: str):
    with open(path) as f:
        logging.debug(f"Reading {path}")
        for line in f:
            yield line.replace("\n", "")  # Removing return char at the end
