"""Shared defaults used across htprovenance and by DAG generators that drive it.

Kept dependency-free (no htcondor2/typer imports) so any module -- including
one on the DAG-generation side of the package boundary -- can import from here
without pulling in the rest of the package's runtime dependencies.
"""

DEFAULT_CLASSAD_FIELDS_FILE = "provenance_fields.yaml"
