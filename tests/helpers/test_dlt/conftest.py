"""
NOTICE: this conftest file is copies from the https://github.com/dlt-hub/dlt/ repo
"""

import os
from os import environ
from typing import Iterator

import pytest
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import PluggableRunContext
from dlt.common.configuration.specs.pluggable_run_context import (
    SupportsRunContext,
)
from dlt.common.pipeline import PipelineContext
from dlt.common.runtime.run_context import DOT_DLT, RunContext
from dlt.common.storages import FileStorage

from dlt.common.utils import custom_environ
from dlt.common import known_env

TEST_STORAGE_ROOT = "_storage"


class MockableRunContext(RunContext):
    @property
    def name(self) -> str:
        return self._name

    @property
    def global_dir(self) -> str:
        return self._global_dir

    @property
    def run_dir(self) -> str:
        return os.environ.get(known_env.DLT_PROJECT_DIR, self._run_dir)

    # @property
    # def settings_dir(self) -> str:
    #     return self._settings_dir

    @property
    def data_dir(self) -> str:
        return os.environ.get(known_env.DLT_DATA_DIR, self._data_dir)

    _name: str
    _global_dir: str
    _run_dir: str
    _settings_dir: str
    _data_dir: str

    @classmethod
    def from_context(cls, ctx: SupportsRunContext) -> "MockableRunContext":
        cls_ = cls(ctx.run_dir)
        cls_._name = ctx.name
        cls_._global_dir = ctx.global_dir
        cls_._run_dir = ctx.run_dir
        cls_._settings_dir = ctx.settings_dir
        cls_._data_dir = ctx.data_dir
        return cls_


@pytest.fixture(autouse=True)
def duckdb_pipeline_location() -> Iterator[None]:
    with custom_environ({"DESTINATION__DUCKDB__CREDENTIALS": ":pipeline:"}):
        yield


@pytest.fixture(autouse=True)
def patch_home_dir() -> Iterator[None]:
    ctx = PluggableRunContext()
    mock = MockableRunContext.from_context(ctx.context)
    mock._global_dir = mock._data_dir = os.path.join(
        os.path.abspath(TEST_STORAGE_ROOT), DOT_DLT
    )
    ctx.context = mock

    with Container().injectable_context(ctx):
        yield


def clean_test_storage(
    init_normalize: bool = False, init_loader: bool = False, mode: str = "t"
) -> FileStorage:
    storage = FileStorage(TEST_STORAGE_ROOT, mode, makedirs=True)
    storage.delete_folder("", recursively=True, delete_ro=True)
    storage.create_folder(".")
    if init_normalize:
        from dlt.common.storages import NormalizeStorage

        NormalizeStorage(True)
    if init_loader:
        from dlt.common.storages import LoadStorage

        LoadStorage(True, LoadStorage.ALL_SUPPORTED_FILE_FORMATS)
    return storage


@pytest.fixture(autouse=True)
def autouse_test_storage() -> FileStorage:
    return clean_test_storage()


@pytest.fixture(scope="function", autouse=True)
def preserve_environ() -> Iterator[None]:
    saved_environ = environ.copy()
    # delta-rs sets those keys without updating environ and there's no
    # method to refresh environ
    known_environ = {
        key_: saved_environ.get(key_)
        for key_ in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_REGION",
            "AWS_SESSION_TOKEN",
        ]
    }
    try:
        yield
    finally:
        environ.clear()
        environ.update(saved_environ)
        for key_, value_ in known_environ.items():
            if value_ is not None or key_ not in environ:
                environ[key_] = value_ or ""
            else:
                del environ[key_]


@pytest.fixture(autouse=True)
def wipe_pipeline(preserve_environ) -> Iterator[None]:
    """Wipes pipeline local state and deactivates it"""
    container = Container()
    if container[PipelineContext].is_active():
        container[PipelineContext].deactivate()
    yield
    if container[PipelineContext].is_active():
        # take existing pipeline
        # NOTE: no more needed. test storage is wiped fully when test starts
        # p = dlt.pipeline()
        # p._wipe_working_folder()
        # deactivate context
        container[PipelineContext].deactivate()
