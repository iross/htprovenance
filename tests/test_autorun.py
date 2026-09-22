"""Tests for the .pth auto-run hook's import-time entrypoint (see
htprovenance/_autorun.py and hatch-autorun's config in pyproject.toml,
which is what actually triggers `import htprovenance._autorun` at
interpreter startup once the package is installed -- see test_autocapture.py
and this task's manual wheel-install verification for that end of it).

Since _autorun's whole effect happens at *import* time, these tests reload
the module fresh each time (it's cheap and side-effect-free to import
otherwise) rather than testing a function call.
"""

import importlib
import sys


def _reload_autorun():
    sys.modules.pop("htprovenance._autorun", None)
    return importlib.import_module("htprovenance._autorun")


def test_autorun_calls_capture_and_emit_when_not_disabled(monkeypatch):
    monkeypatch.delenv("HTPROVENANCE_NO_AUTORUN", raising=False)
    calls = []
    monkeypatch.setattr(
        "htprovenance.autocapture.capture_and_emit", lambda: calls.append(1)
    )

    _reload_autorun()

    assert calls == [1]


def test_autorun_skips_when_disabled_via_env_var(monkeypatch):
    monkeypatch.setenv("HTPROVENANCE_NO_AUTORUN", "1")
    calls = []
    monkeypatch.setattr(
        "htprovenance.autocapture.capture_and_emit", lambda: calls.append(1)
    )

    _reload_autorun()

    assert calls == []


def test_autorun_import_never_raises_even_if_capture_blows_up(monkeypatch):
    monkeypatch.delenv("HTPROVENANCE_NO_AUTORUN", raising=False)

    def boom():
        raise RuntimeError("simulated capture failure")

    monkeypatch.setattr("htprovenance.autocapture.capture_and_emit", boom)

    _reload_autorun()  # must not raise
