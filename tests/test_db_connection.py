"""Tests for src/db/connection.py."""

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

import src.config as cfg_module
from src.db.connection import get_connection, get_db

from tests.conftest import TEAMS_CONFIG


@pytest.fixture(autouse=True)
def reset_config():
    cfg_module._config = None
    yield
    cfg_module._config = None


@pytest.fixture
def sqlite_config(tmp_path):
    db_file = tmp_path / "test.db"
    return {
        "db": {"url": str(db_file)},
        "jira": {"api_token": "tok"},
        "github": {"token": "tok"},
        "teams": TEAMS_CONFIG,
    }


class TestGetConnection:
    def test_sqlite_connection_returned(self, sqlite_config):
        with patch("src.db.connection.get_config", return_value=sqlite_config):
            conn = get_connection()
        assert conn is not None
        conn.close()

    def test_sqlite_row_factory_set(self, sqlite_config):
        with patch("src.db.connection.get_config", return_value=sqlite_config):
            conn = get_connection()
        assert conn.row_factory == sqlite3.Row
        conn.close()

    def test_sqlite_wal_mode_enabled(self, sqlite_config):
        with patch("src.db.connection.get_config", return_value=sqlite_config):
            conn = get_connection()
        journal = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert journal == "wal"
        conn.close()

    def test_sqlite_creates_directory_if_missing(self, tmp_path):
        nested_path = tmp_path / "nested" / "dir" / "metrics.db"
        config = {
            "db": {"url": str(nested_path)},
            "jira": {"api_token": "t"},
            "github": {"token": "t"},
        }
        with patch("src.db.connection.get_config", return_value=config):
            conn = get_connection()
        assert nested_path.parent.exists()
        conn.close()

    def test_postgresql_raises_import_error_when_psycopg2_missing(self):
        config = {
            "db": {"url": "postgresql://user:pass@localhost/db"},
            "jira": {"api_token": "t"},
            "github": {"token": "t"},
        }
        with patch("src.db.connection.get_config", return_value=config):
            import builtins
            original_import = builtins.__import__

            def mock_import(name, *args, **kwargs):
                if name == "psycopg2":
                    raise ImportError("No module named 'psycopg2'")
                return original_import(name, *args, **kwargs)

            with patch("builtins.__import__", side_effect=mock_import):
                with pytest.raises(ImportError, match="psycopg2 required"):
                    get_connection()

    def test_postgresql_uses_psycopg2(self):
        config = {
            "db": {"url": "postgresql://user:pass@localhost/db"},
            "jira": {"api_token": "t"},
            "github": {"token": "t"},
        }
        mock_conn = MagicMock()
        mock_psycopg2 = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn

        with patch("src.db.connection.get_config", return_value=config):
            with patch.dict("sys.modules", {"psycopg2": mock_psycopg2}):
                conn = get_connection()

        mock_psycopg2.connect.assert_called_once_with("postgresql://user:pass@localhost/db")
        assert conn is mock_conn


class TestGetDb:
    def test_context_manager_yields_connection(self, sqlite_config):
        with patch("src.db.connection.get_config", return_value=sqlite_config):
            with get_db() as conn:
                assert conn is not None

    def test_context_manager_closes_connection(self, sqlite_config):
        with patch("src.db.connection.get_config", return_value=sqlite_config):
            with get_db() as conn:
                pass
            # After the context manager exits the connection should be closed
            with pytest.raises(Exception):
                conn.execute("SELECT 1")

    def test_context_manager_commits_on_success(self, tmp_path):
        db_file = tmp_path / "commit_test.db"
        config = {"db": {"url": str(db_file)}, "jira": {"api_token": "t"}, "github": {"token": "t"}}

        with patch("src.db.connection.get_config", return_value=config):
            with get_db() as conn:
                conn.execute("CREATE TABLE test_t (id INTEGER)")
                conn.execute("INSERT INTO test_t VALUES (1)")

            # Verify data persisted after commit
            verify_conn = sqlite3.connect(str(db_file))
            row = verify_conn.execute("SELECT id FROM test_t").fetchone()
            verify_conn.close()

        assert row[0] == 1

    def test_context_manager_rolls_back_on_exception(self, tmp_path):
        db_file = tmp_path / "rollback_test.db"
        config = {"db": {"url": str(db_file)}, "jira": {"api_token": "t"}, "github": {"token": "t"}}

        with patch("src.db.connection.get_config", return_value=config):
            # First create table
            with get_db() as conn:
                conn.execute("CREATE TABLE test_r (id INTEGER)")

            with pytest.raises(Exception):
                with get_db() as conn:
                    conn.execute("INSERT INTO test_r VALUES (42)")
                    raise RuntimeError("simulated error")

            # Verify the insert was rolled back
            verify_conn = sqlite3.connect(str(db_file))
            row = verify_conn.execute("SELECT COUNT(*) FROM test_r").fetchone()
            verify_conn.close()

        assert row[0] == 0
