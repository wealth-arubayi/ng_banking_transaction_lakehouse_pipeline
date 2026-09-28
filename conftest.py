"""
conftest.py — Pytest configuration for Databricks workspace.

The Databricks workspace filesystem (FUSE mount) does not support creating
__pycache__ directories, which pytest's assertion rewriter attempts during
test collection. Setting sys.dont_write_bytecode = True here (before any test
modules are imported/rewritten) prevents the OSError that would otherwise
abort collection.
"""
import sys

sys.dont_write_bytecode = True