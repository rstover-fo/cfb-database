"""Bounded, read-only provider check for the September 2026 recovery."""

import json
import os

import httpx


def main():
    # One request, no automatic retry or warehouse mutation.
    token = os.environ["SOURCES__CFBD__API_KEY"]
    response = httpx.get(
        "https://api.collegefootballdata.com/games",
        params={"year": 2026, "team": "Campbell", "seasonType": "regular"},
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    response.raise_for_status()
    games = response.json()
    if not isinstance(games, list):
        raise ValueError("Expected a games list")
    fields = (
        "id",
        "season",
        "week",
        "seasonType",
        "startDate",
        "homeTeam",
        "awayTeam",
        "completed",
        "homePoints",
        "awayPoints",
        "notes",
    )
    print(json.dumps([{field: game.get(field) for field in fields} for game in games]))


if __name__ == "__main__":
    main()
