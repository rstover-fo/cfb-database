"""Pure parsers for flat-file sources.

Contract (see src/pipelines/sources/flat_files.py): each module exposes pure
functions ``parse*(raw: bytes, ctx: ParseContext) -> Iterator[dict]`` -- no
I/O, no DB, unit-tested against fixtures in tests/fixtures/flatfiles/. The
availability module is the exception (kind="archiver"): it owns discovery +
raw-PDF archival and exposes ``archive(...)`` instead.
"""
