# Enables the `pytester` fixture (a built-in pytest plugin, off by default)
# for sdk/python/tests/test_pytest_plugin.py's pytest-in-pytest smoke tests.
# This conftest only matters when this directory is run under pytest itself
# (`make test-pytest-plugin`); the unittest-based suites (test_client.py,
# test_langgraph.py, run via `python3 -m unittest discover`) never load it.
pytest_plugins = ["pytester"]
