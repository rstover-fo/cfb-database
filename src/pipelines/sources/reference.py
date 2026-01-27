"""Reference data sources - full refresh, no year iteration.

These tables contain relatively static reference/dimension data.
"""

import dlt
from dlt.sources import DltSource

from ..utils.api_client import get_client
from .base import make_request


@dlt.source(name="cfbd_reference")
def reference_source() -> DltSource:
    """Source for all reference/dimension data.

    Includes: conferences, teams, venues, coaches, play_types
    """
    return [
        conferences_resource(),
        teams_resource(),
        venues_resource(),
        coaches_resource(),
        play_types_resource(),
    ]


@dlt.resource(
    name="conferences",
    write_disposition="replace",
    primary_key="id",
)
def conferences_resource():
    """Load all conferences."""
    client = get_client()
    try:
        data = make_request(client, "/conferences")
        yield from data
    finally:
        client.close()


@dlt.resource(
    name="teams",
    write_disposition="replace",
    primary_key="id",
)
def teams_resource():
    """Load all teams."""
    client = get_client()
    try:
        data = make_request(client, "/teams")
        yield from data
    finally:
        client.close()


@dlt.resource(
    name="venues",
    write_disposition="replace",
    primary_key="id",
)
def venues_resource():
    """Load all venues."""
    client = get_client()
    try:
        data = make_request(client, "/venues")
        yield from data
    finally:
        client.close()


@dlt.resource(
    name="coaches",
    write_disposition="replace",
    primary_key=["first_name", "last_name"],
)
def coaches_resource():
    """Load all coaches.

    Note: CFBD returns coaches with their full season history.
    """
    client = get_client()
    try:
        data = make_request(client, "/coaches")
        yield from data
    finally:
        client.close()


@dlt.resource(
    name="play_types",
    write_disposition="replace",
    primary_key="id",
)
def play_types_resource():
    """Load all play types."""
    client = get_client()
    try:
        data = make_request(client, "/plays/types")
        yield from data
    finally:
        client.close()
