import os
import pytest


@pytest.fixture
def anyio_backend():
    return 'asyncio'


@pytest.fixture(autouse=True)
def temp_flow_data_dir(monkeypatch, tmpdir):
    monkeypatch.setenv('FLOW_DATA_DIR', os.path.join(tmpdir, 'flow'))
