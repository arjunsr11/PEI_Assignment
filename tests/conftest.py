"""
Shared fixtures for the PEI Assignment test suite.

The pipeline implementation lives in the layer library notebooks in
src/utils/ (01_raw_utils, 02_enriched_utils, 03_curated_utils).
Notebooks cannot be imported like Python modules,
so this conftest reads each notebook's .ipynb source from the workspace
filesystem and executes its Python cells into a single module namespace. Every
test therefore runs against the REAL implementation, never a copy.
"""

import json
import sys
import types
from pathlib import Path

sys.dont_write_bytecode = True  # /Workspace folders reject .pyc writes

import pytest
from pyspark.sql import SparkSession

NOTEBOOK_PATHS = [
    Path("/Workspace/PEI Assignment/src/utils/01_raw_utils.ipynb"),       # Task 1 : raw loads
    Path("/Workspace/PEI Assignment/src/utils/02_enriched_utils.ipynb"),  # Tasks 2-3 : enrichment
    Path("/Workspace/PEI Assignment/src/utils/03_curated_utils.ipynb"),   # Tasks 3-4 : curated + aggregates
]


def _notebook_code_cells(path):
    """Yield the Python source of every executable code cell in a notebook."""
    with open(path, "r", encoding="utf-8") as f:
        nb = json.load(f)
    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue  # markdown cells are documentation only
        src = cell.get("source", "")
        if isinstance(src, list):  # nbformat stores source as a list of lines
            src = "".join(src)
        src = src.strip()
        if not src or src.startswith("%"):
            continue  # %run / %pip / %skip magic cells are notebook only side effects
        yield src


def load_pipeline_module():
    """Execute the layer library notebook code cells into a module object."""
    module = types.ModuleType("data_load_methods")
    module.__dict__["spark"] = SparkSession.getActiveSession()
    module.__dict__["display"] = lambda *a, **k: None  # display is a notebook-only builtin
    for path in NOTEBOOK_PATHS:
        for src in _notebook_code_cells(path):
            exec(compile(src, str(path), "exec"), module.__dict__)
    return module


@pytest.fixture(scope="session")
def dlm():
    """The actual pipeline implementation from the layer library notebooks."""
    return load_pipeline_module()


@pytest.fixture(scope="session")
def spark():
    """The active Spark session (the suite runs inside a Databricks notebook)."""
    return SparkSession.getActiveSession()
