import string
from pathlib import Path
import random

import pytest


def get_random_string(length):
    # With combination of lower and upper case
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


@pytest.fixture(scope="function")
def rand_path(tmp_path: Path) -> Path:
    file = tmp_path / get_random_string(10)
    file.mkdir()
    return file
