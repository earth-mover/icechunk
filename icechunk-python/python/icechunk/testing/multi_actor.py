"""Multi-actor coordinator for stateful tests.

Manages multiple actors sharing a repository. Each actor has its own
IcechunkModel. The coordinator swaps the active model and computes
the blocked set (paths other actors have modified).

The test class's self.model is always a plain IcechunkModel — the
coordinator just tracks which one is active and what's blocked.
The committed baseline lives on the test (self.committed), not here.
"""

from __future__ import annotations

from pathlib import PurePosixPath

from icechunk.testing.stateful_models import IcechunkModel


class MultiActorCoordinator:
    """Coordinates multiple actors sharing a repository.

    Holds the actor dict. Computes the blocked set from other actors'
    changes. The test class owns self.model and self.committed directly.
    """

    @staticmethod
    def _has_blocked_ancestor(path: str, blocked: set[str]) -> bool:
        """True if path or any ancestor of path is in the blocked set."""
        pp = PurePosixPath(path)
        return any(pp.is_relative_to(p) for p in blocked)

    @staticmethod
    def _has_blocked_relative(path: str, blocked: set[str]) -> bool:
        """True if any blocked path is an ancestor, descendant, or equal."""
        pp = PurePosixPath(path)
        return any(
            pp.is_relative_to(p) or PurePosixPath(p).is_relative_to(path) for p in blocked
        )

    def __init__(self) -> None:
        self.current_actor: str = ""
        self._actors: dict[str, IcechunkModel] = {}
        self.blocked: set[str] = set()

    @property
    def current(self) -> IcechunkModel:
        """The current actor's model."""
        return self._actors[self.current_actor]

    @current.setter
    def current(self, model: IcechunkModel) -> None:
        self._actors[self.current_actor] = model

    def recompute_blocked(self, committed: IcechunkModel | None) -> None:
        """Recompute the blocked set for the current actor.

        The blocked set is:
        1. Other actors' uncommitted changes (structural + data)
        2. Main drift: paths that changed on committed since this actor's baseline
        """
        actor = self.current_actor
        result: set[str] = set()

        for name, state in self._actors.items():
            if name != actor:
                result |= state.changes()

        current = self._actors.get(actor)
        if current is not None and committed is not None:
            committed_nodes = frozenset(committed.all_arrays) | frozenset(
                committed.all_groups
            )
            result |= committed_nodes ^ current.baseline

        self.blocked = result

    # ── Actor management ─────────────────────────────────────────────

    def switch(self, actor: str, committed: IcechunkModel | None) -> None:
        """Switch to an existing actor and recompute the blocked set."""
        self.current_actor = actor
        self.recompute_blocked(committed)

    def init_actor(
        self, committed: IcechunkModel | None, bootstrap: str = "default"
    ) -> None:
        """Promote the bootstrap model to the current actor's model."""
        self._actors[self.current_actor] = self._actors.pop(bootstrap)
        if committed is not None:
            self.current.sync_baseline(committed)
        self.recompute_blocked(committed)

    def other_actors_clean(self) -> bool:
        """True if no other actor has uncommitted changes."""
        return all(
            not state.changes()
            for name, state in self._actors.items()
            if name != self.current_actor
        )

    def can_add(self, path: str) -> bool:
        return not self._has_blocked_ancestor(path, self.blocked)
