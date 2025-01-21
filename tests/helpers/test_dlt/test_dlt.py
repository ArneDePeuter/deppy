import dlt
from deppy.blueprint import Blueprint, Node, Const, Secret, Output, Object
from deppy.helpers.DLT import blueprint_to_source
from duckdb import CatalogException
import pytest


def test_sync(monkeypatch):
    def add(a, b):
        return a + b

    class Obj:
        def __init__(self, amount: int):
            self.list = list(range(amount))

        def get_list(self):
            return self.list

    class MyTest(Blueprint):
        obj = Object(Obj)

        my_const: int = Const()
        my_secret: int = Secret()

        add_node1 = Node(add)
        add_node2 = Node(add)
        items = Node(obj.get_list)
        item = Output(items, loop=True)

        edges = [
            (my_const, add_node1, "a"),
            (my_secret, add_node1, "b"),
            (add_node1, add_node2, "a"),
            (item, add_node2, "b"),
        ]

    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYTEST__OBJ__AMOUNT", "3")
    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYTEST__MY_CONST", "3")
    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYTEST__MY_SECRET", "4")

    source = blueprint_to_source(MyTest)

    pipeline = dlt.pipeline(
        pipeline_name="testpipeline", destination="duckdb", full_refresh=True
    )
    pipeline.run(source())

    with pipeline.sql_client() as sql_client:
        with sql_client.execute("SELECT * FROM add_node2") as cursor:
            rows = cursor.fetchall()
            assert len(rows) == 3
            assert all(item in rows[i] for item, i in zip([7, 8, 9], range(3)))
    # dont include secret and consts
    with pipeline.sql_client() as sql_client:
        with pytest.raises(CatalogException):
            sql_client.execute("SELECT * FROM my_const")
    with pipeline.sql_client() as sql_client:
        with pytest.raises(CatalogException):
            sql_client.execute("SELECT * FROM my_secret")


def test_target_nodes(monkeypatch):
    def add(a, b):
        return a + b

    class Obj:
        def __init__(self, amount: int):
            self.list = list(range(amount))

        def get_list(self):
            return self.list

    class MyTest(Blueprint):
        obj = Object(Obj)

        my_const: int = Const()
        my_secret: int = Secret()

        add_node1 = Node(add)
        add_node2 = Node(add)
        items = Node(obj.get_list)
        item = Output(items, loop=True)

        edges = [
            (my_const, add_node1, "a"),
            (my_secret, add_node1, "b"),
            (add_node1, add_node2, "a"),
            (item, add_node2, "b"),
        ]

    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYTEST__OBJ__AMOUNT", "3")
    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYTEST__MY_CONST", "3")
    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYTEST__MY_SECRET", "4")

    source = blueprint_to_source(MyTest, target_nodes=[MyTest.my_const])

    pipeline = dlt.pipeline(
        pipeline_name="testpipeline", destination="duckdb", full_refresh=True
    )
    pipeline.run(source())

    with pipeline.sql_client() as sql_client:
        with pytest.raises(CatalogException):
            sql_client.execute("SELECT * FROM add_node2")
        # include consts if specified in target_nodes
        with sql_client.execute("SELECT * FROM my_const") as cursor:
            rows = cursor.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == 3


def test_exclude(monkeypatch):
    def add(a, b):
        return a + b

    class Obj:
        def __init__(self, amount: int):
            self.list = list(range(amount))

        def get_list(self):
            return self.list

    class MyTest(Blueprint):
        obj = Object(Obj)

        my_const: int = Const()
        my_secret: int = Secret()

        add_node1 = Node(add)
        add_node2 = Node(add)
        items = Node(obj.get_list)
        item = Output(items, loop=True)

        edges = [
            (my_const, add_node1, "a"),
            (my_secret, add_node1, "b"),
            (add_node1, add_node2, "a"),
            (item, add_node2, "b"),
        ]

    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYTEST__OBJ__AMOUNT", "3")
    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYTEST__MY_CONST", "3")
    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYTEST__MY_SECRET", "4")

    source = blueprint_to_source(MyTest, exclude_for_storing=[MyTest.add_node2])

    pipeline = dlt.pipeline(
        pipeline_name="testpipeline", destination="duckdb", full_refresh=True
    )
    pipeline.run(source())

    with pipeline.sql_client() as sql_client:
        with pytest.raises(CatalogException):
            sql_client.execute("SELECT * FROM add_node2")


def test_sync_with_context(monkeypatch):
    def add(a, b):
        return a + b

    called_context = False

    class Obj:
        def __init__(self, amount: int):
            self.list = list(range(amount))

        def get_list(self):
            return self.list

        def __enter__(self):
            nonlocal called_context
            called_context = True
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            pass

    class MyTest(Blueprint):
        obj = Object(Obj)

        my_const: int = Const()
        my_secret: int = Secret()

        add_node1 = Node(add)
        add_node2 = Node(add)
        items = Node(obj.get_list)
        item = Output(items, loop=True)

        edges = [
            (my_const, add_node1, "a"),
            (my_secret, add_node1, "b"),
            (add_node1, add_node2, "a"),
            (item, add_node2, "b"),
        ]

    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYTEST__OBJ__AMOUNT", "3")
    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYTEST__MY_CONST", "3")
    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYTEST__MY_SECRET", "4")

    source = blueprint_to_source(MyTest)

    pipeline = dlt.pipeline(
        pipeline_name="testpipeline", destination="duckdb", full_refresh=True
    )
    pipeline.run(source())

    assert called_context, "Sync context manager was not called."

    with pipeline.sql_client() as sql_client:
        with sql_client.execute("SELECT * FROM add_node2") as cursor:
            rows = cursor.fetchall()
            assert len(rows) == 3
            assert all(item in rows[i] for item, i in zip([7, 8, 9], range(3)))


def test_async_with_context(monkeypatch):
    async def add(a, b):
        return a + b

    called_context = False

    class AsyncObj:
        def __init__(self, amount: int):
            self.list = list(range(amount))

        async def get_list(self):
            return self.list

        async def __aenter__(self):
            nonlocal called_context
            called_context = True
            return self

        async def __aexit__(self, exc_type, exc_value, traceback): ...

    class MyAsyncTest(Blueprint):
        obj = Object(AsyncObj)

        my_const: int = Const()
        my_secret: int = Secret()

        add_node1 = Node(add)
        add_node2 = Node(add)
        items = Node(obj.get_list)
        item = Output(items, loop=True)

        edges = [
            (my_const, add_node1, "a"),
            (my_secret, add_node1, "b"),
            (add_node1, add_node2, "a"),
            (item, add_node2, "b"),
        ]

    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYASYNCTEST__OBJ__AMOUNT", "3")
    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYASYNCTEST__MY_CONST", "3")
    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYASYNCTEST__MY_SECRET", "4")

    source = blueprint_to_source(MyAsyncTest)

    pipeline = dlt.pipeline(
        pipeline_name="testpipeline", destination="duckdb", full_refresh=True
    )
    pipeline.run(source())

    assert called_context, "Async context manager was not called."

    with pipeline.sql_client() as sql_client:
        with sql_client.execute("SELECT * FROM add_node2") as cursor:
            rows = cursor.fetchall()
            assert len(rows) == 3
            assert all(item in rows[i] for item, i in zip([7, 8, 9], range(3)))


def test_mixed_context(monkeypatch):
    def add(a, b):
        return a + b

    sync_called_context = False
    async_called_context = False

    class SyncObj:
        def __init__(self, amount: int):
            self.list = list(range(amount))

        def get_list(self):
            return self.list

        def __enter__(self):
            nonlocal sync_called_context
            sync_called_context = True
            return self

        def __exit__(self, exc_type, exc_value, traceback): ...

    class AsyncObj:
        def __init__(self, amount: int):
            self.list = list(range(amount))

        async def get_list(self):
            return self.list

        async def __aenter__(self):
            nonlocal async_called_context
            async_called_context = True
            return self

        async def __aexit__(self, exc_type, exc_value, traceback): ...

    class MyMixedTest(Blueprint):
        sync_obj = Object(SyncObj)
        async_obj = Object(AsyncObj)

        my_const: int = Const()

        add_node1 = Node(add)
        items_sync = Node(sync_obj.get_list)
        items_async = Node(async_obj.get_list)
        item = Output(items_sync, loop=True)

        edges = [
            (my_const, add_node1, "a"),
            (item, add_node1, "b"),
        ]

    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYMIXEDTEST__SYNC_OBJ__AMOUNT", "3")
    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYMIXEDTEST__ASYNC_OBJ__AMOUNT", "3")
    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYMIXEDTEST__MY_CONST", "3")

    source = blueprint_to_source(MyMixedTest)

    pipeline = dlt.pipeline(
        pipeline_name="testpipeline", destination="duckdb", full_refresh=True
    )
    pipeline.run(source())

    assert sync_called_context, "Sync context manager was not called."
    assert async_called_context, "Async context manager was not called."

    with pipeline.sql_client() as sql_client:
        with sql_client.execute("SELECT * FROM add_node1") as cursor:
            rows = cursor.fetchall()
            assert len(rows) == 3
            assert all(item in rows[i] for item, i in zip([3, 4, 5], range(3)))


def test_async_context_with_sync_function(monkeypatch):
    def add(a, b):
        return a + b

    class Obj:
        def __init__(self, amount: int):
            self.list = list(range(amount))

        def get_list(self):
            return self.list

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_value, traceback): ...

    class MyTest(Blueprint):
        obj = Object(Obj)

        my_const: int = Const()
        my_secret: int = Secret()

        add_node1 = Node(add)
        add_node2 = Node(add)
        items = Node(obj.get_list)
        item = Output(items, loop=True)

        edges = [
            (my_const, add_node1, "a"),
            (my_secret, add_node1, "b"),
            (add_node1, add_node2, "a"),
            (item, add_node2, "b"),
        ]

    source = blueprint_to_source(MyTest)

    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYTEST__OBJ__AMOUNT", "3")
    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYTEST__MY_CONST", "3")
    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYTEST__MY_SECRET", "4")

    pipeline = dlt.pipeline(
        pipeline_name="testpipeline", destination="duckdb", full_refresh=True
    )
    pipeline.run(source())

    with pipeline.sql_client() as sql_client:
        with sql_client.execute("SELECT * FROM add_node2") as cursor:
            rows = cursor.fetchall()
            assert len(rows) == 3
            assert all(item in rows[i] for item, i in zip([7, 8, 9], range(3)))


def test_sync_context_with_async_func(monkeypatch):
    async def add(a, b):
        return a + b

    class Obj:
        def __init__(self, amount: int):
            self.list = list(range(amount))

        def get_list(self):
            return self.list

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback): ...

    class MyTest(Blueprint):
        obj = Object(Obj)

        my_const: int = Const()
        my_secret: int = Secret()

        add_node1 = Node(add)
        add_node2 = Node(add)
        items = Node(obj.get_list)
        item = Output(items, loop=True)

        edges = [
            (my_const, add_node1, "a"),
            (my_secret, add_node1, "b"),
            (add_node1, add_node2, "a"),
            (item, add_node2, "b"),
        ]

    source = blueprint_to_source(MyTest)

    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYTEST__OBJ__AMOUNT", "3")
    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYTEST__MY_CONST", "3")
    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYTEST__MY_SECRET", "4")

    pipeline = dlt.pipeline(
        pipeline_name="testpipeline", destination="duckdb", full_refresh=True
    )
    pipeline.run(source())

    with pipeline.sql_client() as sql_client:
        with sql_client.execute("SELECT * FROM add_node2") as cursor:
            rows = cursor.fetchall()
            assert len(rows) == 3
            assert all(item in rows[i] for item, i in zip([7, 8, 9], range(3)))


def test_async_no_context(monkeypatch):
    async def add(a, b):
        return a + b

    class Obj:
        def __init__(self, amount: int):
            self.list = list(range(amount))

        async def get_list(self):
            return self.list

    class MyTest(Blueprint):
        obj = Object(Obj)

        my_const: int = Const()
        my_secret: int = Secret()

        add_node1 = Node(add)
        add_node2 = Node(add)
        items = Node(obj.get_list)
        item = Output(items, loop=True)

        edges = [
            (my_const, add_node1, "a"),
            (my_secret, add_node1, "b"),
            (add_node1, add_node2, "a"),
            (item, add_node2, "b"),
        ]

    source = blueprint_to_source(MyTest)

    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYTEST__OBJ__AMOUNT", "3")
    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYTEST__MY_CONST", "3")
    monkeypatch.setenv("TESTPIPELINE__SOURCES__MYTEST__MY_SECRET", "4")

    pipeline = dlt.pipeline(
        pipeline_name="testpipeline", destination="duckdb", full_refresh=True
    )
    pipeline.run(source())

    with pipeline.sql_client() as sql_client:
        with sql_client.execute("SELECT * FROM add_node2") as cursor:
            rows = cursor.fetchall()
            assert len(rows) == 3
            assert all(item in rows[i] for item, i in zip([7, 8, 9], range(3)))


def test_resource_kwargs():
    async def get_items():
        return [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
            {"id": 4, "name": "Jeff"},
        ]

    class MyTest(Blueprint):
        items = Node(get_items)
        item = Output(items, loop=True)

    source = blueprint_to_source(
        MyTest, resource_kwargs={MyTest.item: {"primary_key": "id"}}
    )
    pipeline = dlt.pipeline(
        pipeline_name="testpipeline", destination="duckdb", full_refresh=True
    )
    pipeline.run(source())

    tables = pipeline.default_schema.data_tables()
    assert "primary_key" in tables[0]["columns"]["id"]
    assert tables[0]["columns"]["id"]["primary_key"]

    source = blueprint_to_source(MyTest, resource_kwargs={})
    pipeline = dlt.pipeline(
        pipeline_name="testpipeline", destination="duckdb", full_refresh=True
    )
    pipeline.run(source())

    tables = pipeline.default_schema.data_tables()
    assert "primary_key" not in tables[0]["columns"]["id"]
