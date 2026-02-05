"""Reference data sources - full refresh, no year iteration.

These tables contain relatively static reference/dimension data.
"""

import dlt
from dlt.sources import DltSource

from ..config.years import get_current_season
from ..utils.api_client import get_client
from .base import make_request


@dlt.source(name="cfbd_reference")
def reference_source() -> DltSource:
    """Source for all reference/dimension data.

    Includes: conferences, teams, teams_fbs, venues, coaches, play_types,
              play_stat_types, draft_positions, draft_teams, stat_categories, calendar
    """
    return [
        conferences_resource(),
        teams_resource(),
        teams_fbs_resource(),
        venues_resource(),
        coaches_resource(),
        play_types_resource(),
        play_stat_types_resource(),
        draft_positions_resource(),
        draft_teams_resource(),
        stat_categories_resource(),
        calendar_resource(),
    ]


@dlt.resource(
    name="conferences",
    write_disposition="merge",
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
    write_disposition="merge",
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
    write_disposition="merge",
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
    write_disposition="merge",
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
    write_disposition="merge",
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


@dlt.resource(
    name="teams_fbs",
    write_disposition="merge",
    primary_key="id",
)
def teams_fbs_resource():
    """Load all FBS teams."""
    client = get_client()
    try:
        data = make_request(client, "/teams/fbs")
        yield from data
    finally:
        client.close()


@dlt.resource(
    name="play_stat_types",
    write_disposition="merge",
    primary_key="id",
)
def play_stat_types_resource():
    """Load play stat type definitions."""
    client = get_client()
    try:
        data = make_request(client, "/plays/stats/types")
        yield from data
    finally:
        client.close()


@dlt.resource(
    name="draft_positions",
    write_disposition="merge",
    primary_key="name",
)
def draft_positions_resource():
    """Load NFL draft position categories."""
    client = get_client()
    try:
        data = make_request(client, "/draft/positions")
        yield from data
    finally:
        client.close()


@dlt.resource(
    name="draft_teams",
    write_disposition="merge",
    primary_key=["location", "nickname"],
)
def draft_teams_resource():
    """Load NFL draft teams."""
    client = get_client()
    try:
        data = make_request(client, "/draft/teams")
        yield from data
    finally:
        client.close()


@dlt.resource(
    name="stat_categories",
    write_disposition="merge",
    primary_key="name",
)
def stat_categories_resource():
    """Load stat category definitions."""
    client = get_client()
    try:
        data = make_request(client, "/stats/categories")
        yield from data
    finally:
        client.close()


@dlt.resource(
    name="calendar",
    write_disposition="merge",
    primary_key=["season", "week"],
)
def calendar_resource():
    """Load season calendar for current year."""
    client = get_client()
    try:
        year = get_current_season()
        data = make_request(client, "/calendar", params={"year": year})
        yield from data
    finally:
        client.close()
