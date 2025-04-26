import os
import pytest


@pytest.fixture
def upload_url():
    return "http://127.0.0.1:8000/upload-file"


@pytest.fixture
def test_file_path(tmp_path):
    file_path = tmp_path / "sample.txt"
    file_path.write_text("This is a test file.")
    return file_path
