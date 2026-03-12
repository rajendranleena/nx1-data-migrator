import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_RANGER_DIR = Path(__file__).resolve().parent.parent
_MODULE_NAME = "ranger_policies_generator_airflow3"


def _make_airflow_stubs():
    """Build fake sys.modules entries so the DAG file can be imported without real deps."""
    airflow = MagicMock()

    # DAG context manager must yield itself
    dag_instance = MagicMock()
    dag_instance.__enter__ = MagicMock(return_value=dag_instance)
    dag_instance.__exit__ = MagicMock(return_value=False)
    airflow.DAG.return_value = dag_instance

    # @task stub: keep __wrapped__ pointing at the real function so tests can
    # call it directly, but return a mock XComArg for the DAG wiring at import.
    def _wrap_task_func(f):
        mock_xcom = MagicMock(name=f"XComArg<{f.__name__}>")
        mock_xcom.override = lambda **kw: mock_xcom

        def task_callable(*args, **kwargs):
            return mock_xcom

        task_callable.__wrapped__ = f
        task_callable._is_task = True
        task_callable.__name__ = f.__name__
        task_callable.__qualname__ = f.__qualname__
        task_callable.override = lambda **kw: task_callable
        return task_callable

    def passthrough_task(func=None, **kwargs):
        if func is not None:
            return _wrap_task_func(func)
        def wrapper(f):
            return _wrap_task_func(f)
        return wrapper

    passthrough_task.pyspark = lambda **kw: passthrough_task

    airflow_decorators = MagicMock()
    airflow_decorators.task = passthrough_task

    # Variable.get returns default_var kwarg
    variable_mock = MagicMock()
    variable_mock.get.side_effect = lambda key, default_var='', **kw: default_var

    stubs = {
        "airflow": airflow,
        "airflow.DAG": airflow.DAG,
        "airflow.decorators": airflow_decorators,
        "airflow.decorators.task": passthrough_task,
        "airflow.models": MagicMock(Variable=variable_mock),
        "airflow.models.Variable": variable_mock,
        "airflow.utils": MagicMock(),
        "airflow.utils.trigger_rule": MagicMock(TriggerRule=MagicMock(ALL_DONE="all_done")),
        "airflow.utils.email": MagicMock(),
        "dotenv": MagicMock(),
        "ranger_utils": MagicMock(),
        "pyspark": MagicMock(),
    }
    return stubs, variable_mock


@pytest.fixture(scope="session")
def dag_module():
    """Import the DAG module with heavy deps stubbed out."""
    stubs, _ = _make_airflow_stubs()

    saved = {}
    for mod_name, fake in stubs.items():
        saved[mod_name] = sys.modules.get(mod_name)
        sys.modules[mod_name] = fake

    str_dir = str(_RANGER_DIR)
    if str_dir not in sys.path:
        sys.path.insert(0, str_dir)

    try:
        if _MODULE_NAME in sys.modules:
            del sys.modules[_MODULE_NAME]
        module = importlib.import_module(_MODULE_NAME)
        yield module
    finally:
        for mod_name, original in saved.items():
            if original is None:
                sys.modules.pop(mod_name, None)
            else:
                sys.modules[mod_name] = original
        sys.modules.pop(_MODULE_NAME, None)
        if str_dir in sys.path:
            sys.path.remove(str_dir)
