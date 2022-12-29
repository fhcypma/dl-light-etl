import random
import string
from pathlib import Path

import pytest


def get_random_string(length):
    # With combination of lower and upper case
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


@pytest.fixture(scope="function")
def rand_path(tmp_path: Path) -> Path:
    file = tmp_path / get_random_string(10)
    return file


@pytest.fixture(scope="function")
def rand_dir_path(rand_path: Path) -> Path:
    rand_path.mkdir()
    return rand_path
