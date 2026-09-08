"""Pure, deterministic planning over the declared SQL refresh graph."""

import re
from collections.abc import Iterable
from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter

from src.pipelines.config.refresh_assets import EXTERNAL_RELATIONS, REFRESH_ASSETS, RefreshAsset

_RELATION = re.compile(r"[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*\Z")


@dataclass(frozen=True)
class RefreshPlan:
    views: tuple[str, ...]
    changed: tuple[str, ...] = ()


class RefreshGraph:
    """Validate once; retain SQL lineage even through unselected parents."""

    def __init__(self, assets: Iterable[RefreshAsset], external: Iterable[str] = ()):
        records = tuple(assets)
        self.assets = {asset.name: asset for asset in records}
        if len(records) != len(self.assets):
            raise ValueError("duplicate refresh asset")
        self.external = frozenset(external)
        if self.external & self.assets.keys():
            raise ValueError("refresh assets cannot also be external relations")
        known = self.external | self.assets.keys()
        if any(not _RELATION.fullmatch(name) for name in known):
            raise ValueError("asset names must be unquoted schema-qualified relation names")
        self.dependencies = {name: frozenset() for name in self.external}
        for asset in records:
            unknown = set(asset.depends_on) - known
            if unknown:
                raise ValueError(f"{asset.name} has unknown dependencies: {sorted(unknown)}")
            self.dependencies[asset.name] = frozenset(asset.depends_on)
        try:
            tuple(TopologicalSorter(self.dependencies).static_order())
        except CycleError as exc:
            raise ValueError(f"refresh dependency cycle: {exc.args[1]}") from exc
        self.order = {asset.name: index for index, asset in enumerate(records)}
        self._ancestors: dict[str, frozenset[str]] = {}

    def ancestors(self, name: str) -> frozenset[str]:
        if name not in self.dependencies:
            raise ValueError(f"unknown asset: {name}")
        if name not in self._ancestors:
            parents = self.dependencies[name]
            self._ancestors[name] = parents | frozenset(
                ancestor for parent in parents for ancestor in self.ancestors(parent)
            )
        return self._ancestors[name]

    def plan(
        self,
        *,
        views: Iterable[str] | None = None,
        changed: Iterable[str] | None = None,
        schema: str | None = None,
    ) -> RefreshPlan:
        if views is not None and changed is not None:
            raise ValueError("--views and --changed cannot be combined")
        roots = tuple(dict.fromkeys(changed)) if changed is not None else ()
        if changed is not None:
            if not roots:
                raise ValueError("--changed requires at least one relation")
            unknown = set(roots) - self.dependencies.keys()
            if unknown:
                raise ValueError(f"unknown changed relations: {sorted(unknown)}")
            selected = {
                name
                for name in self.assets
                if name not in roots and self.ancestors(name).intersection(roots)
            }
        elif views is not None:
            selected = set(views)
            unknown = selected - self.assets.keys()
            if unknown:
                raise ValueError(f"unknown refresh views: {sorted(unknown)}")
            if not selected:
                raise ValueError("--views requires at least one view")
        else:
            if schema not in (None, "marts", "analytics"):
                raise ValueError(f"unknown refresh schema: {schema}")
            selected = {
                name for name in self.assets if schema is None or name.startswith(schema + ".")
            }
        # Subset planning considers transitive dependencies, not only direct
        # edges; an unselected intermediate view must not reverse their order.
        pending = {name: set(self.ancestors(name)).intersection(selected) for name in selected}
        ordered = []
        while pending:
            ready = min(
                (name for name, parents in pending.items() if not parents),
                key=self.order.__getitem__,
            )
            ordered.append(ready)
            del pending[ready]
            for parents in pending.values():
                parents.discard(ready)
        return RefreshPlan(tuple(ordered), roots)


REFRESH_GRAPH = RefreshGraph(REFRESH_ASSETS, EXTERNAL_RELATIONS)
