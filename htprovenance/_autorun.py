"""Import-time entrypoint for the packaged auto-run hook (see hatch_build.py).

A .pth file installed alongside this package runs `import
htprovenance._autorun` at the start of every Python process in an
environment where htprovenance is installed (see Python's site module: any .pth
line starting with "import " is executed at interpreter startup). That makes
this module's body run constantly, for every python invocation on every
machine with htprovenance installed -- not just inside HTCondor jobs -- so it must
be near-instant and must never raise for the overwhelming common case (not
running inside an HTCondor job): htprovenance.autocapture.capture_and_emit
already no-ops immediately when $_CONDOR_JOB_AD isn't set, before doing any
real work.

Not meant to be imported for its own functions -- import
htprovenance.autocapture directly for that; this module exists only to
be the .pth hook's import target, and is a no-op to import for anything else
(the capture_and_emit() call is skipped when HTPROVENANCE_NO_AUTORUN is
set, so tests and other tooling can import this package's modules freely
without triggering it).
"""

from __future__ import annotations

import os

if not os.environ.get("HTPROVENANCE_NO_AUTORUN"):
    try:
        from htprovenance.autocapture import capture_and_emit

        capture_and_emit()
    except Exception:  # noqa: BLE001, S110 -- deliberately blind and silent:
        # Python's site module already catches and logs a traceback for a
        # failing .pth import line without crashing the interpreter, but
        # capture_and_emit() is itself best-effort and shouldn't raise in
        # the first place -- this is a second, redundant safety net for
        # exactly the situation it exists to prevent (see module docstring).
        pass
