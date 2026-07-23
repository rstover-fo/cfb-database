"""Team-name crosswalk resolver over ref.team_name_xwalk (T3).

External flat files spell teams their own way ("Ohio St", "OhioState",
"Miami-Ohio"); warehouse identity is the exact CFBD full-name string
("Ohio State", "Miami (OH)") used across core.games/ref.teams. The resolver
loads the source's mapping once and resolves per row; misses are counted so
the framework's unmapped gate can fail loud (see
flat_files.UnmappedNamesError) instead of silently dropping rows.
"""

import logging

import psycopg2

from .load_ledger import get_db_url

logger = logging.getLogger(__name__)


def normalize_name(name: str) -> str:
    """Canonicalize a source spelling for matching: trim, collapse whitespace.

    Pure; used by both the resolver (lookup key) and the seed generator.
    Implemented in T3. Keep conservative -- normalization is for lookup only,
    the stored mapping stays verbatim.
    """
    return " ".join(name.strip().split()).casefold()


class XwalkResolver:
    """Resolves one source's team spellings to CFBD names, counting misses."""

    def __init__(self, source: str, mapping: dict[str, str]):
        """Bind a source name to its {normalized_source_name: cfbd_name} mapping."""
        self.source = source
        self._mapping = mapping
        self._misses: dict[str, int] = {}

    @classmethod
    def load(cls, source: str, db_url: str | None = None) -> "XwalkResolver":
        """Load the source's rows from ref.team_name_xwalk. Implemented in T3."""
        dsn = db_url or get_db_url()
        conn = psycopg2.connect(dsn)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT source_name, cfbd_name FROM ref.team_name_xwalk WHERE source = %s",
                    (source,),
                )
                rows = cur.fetchall()
        finally:
            conn.close()

        mapping = {normalize_name(source_name): cfbd_name for source_name, cfbd_name in rows}
        return cls(source, mapping)

    def resolve(self, source_name: str) -> str | None:
        """CFBD name for a source spelling, or None (recorded as a miss). Implemented in T3."""
        cfbd_name = self._mapping.get(normalize_name(source_name))
        if cfbd_name is None:
            self._misses[source_name] = self._misses.get(source_name, 0) + 1
            return None
        return cfbd_name

    @property
    def misses(self) -> dict[str, int]:
        """Distinct unmapped source names -> occurrence counts."""
        return dict(self._misses)
