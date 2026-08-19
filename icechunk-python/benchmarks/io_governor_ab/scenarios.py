"""Chunk profiles and the scenario matrix."""

from __future__ import annotations

from dataclasses import dataclass, replace

DTYPE = "float32"
ITEMSIZE = 4


@dataclass(frozen=True)
class ChunkProfile:
    name: str
    chunk_elems: int
    nchunks: int

    @property
    def chunk_bytes(self) -> int:
        return self.chunk_elems * ITEMSIZE

    @property
    def shape(self) -> tuple[int]:
        return (self.chunk_elems * self.nchunks,)

    @property
    def chunks(self) -> tuple[int]:
        return (self.chunk_elems,)

    @property
    def total_bytes(self) -> int:
        return self.chunk_bytes * self.nchunks

    @property
    def array_name(self) -> str:
        return f"data-{self.name}"

    def scaled(self, factor: int) -> ChunkProfile:
        return replace(self, nchunks=max(2, self.nchunks // factor))


# small and 512k are sized for seconds of steady state at their (slow,
# latency-bound) achievable rates, so fixed round overhead can't hide a gap
SMALL = ChunkProfile("small", 8 * 1024, 65536)  # 32 KiB chunks, 2 GiB
HALF_MIB = ChunkProfile("512k", 128 * 1024, 16384)  # 512 KiB chunks, 8 GiB
MEDIUM = ChunkProfile("medium", 1024 * 1024, 512)  # 4 MiB chunks, 2 GiB
# 128 MiB chunks (exercises S3 multipart on write), 3 GiB
BIG = ChunkProfile("big", 32 * 1024 * 1024, 24)

PROFILES = (SMALL, HALF_MIB, MEDIUM, BIG)

# mixed keeps its original small+medium+big composition; 512k stays out so
# adding it doesn't double every mixed round
MIXED_NAMES = ("small", "medium", "big")

LOADS = ("read", "write", "readwrite")


@dataclass(frozen=True)
class Scenario:
    name: str
    backend: str  # "local" | "s3" | "gcs"
    load: str  # "read" | "write" | "readwrite" | "contend"
    profiles: tuple[ChunkProfile, ...]
    dataset: str  # setup repo key (a profile name, "mixed", "multi") or, for
    # external scenarios, the full "org/repo" name
    num_repos: int = 1
    external: bool = False  # read-only, not harness-written (realistic reads)
    # contend only: the noisy neighbor's dataset, read continuously while the
    # victim rounds are measured
    aggressor_profile: ChunkProfile | None = None
    aggressor_dataset: str = "big"

    @property
    def read_bytes_expected(self) -> int:
        if self.load == "write":
            return 0
        return sum(p.total_bytes for p in self.profiles) * self.num_repos

    @property
    def write_bytes_expected(self) -> int:
        if self.load == "read":
            return 0
        return sum(p.total_bytes for p in self.profiles) * self.num_repos


QUICK_FACTOR = 8


def _profiles_by_name(quick: bool) -> dict[str, ChunkProfile]:
    factor = QUICK_FACTOR if quick else 1
    return {p.name: p.scaled(factor) for p in PROFILES}


def dataset_keys(scenario: Scenario) -> list[str]:
    """Storage keys (one per repo) a scenario reads/writes."""
    if scenario.num_repos == 1:
        return [scenario.dataset]
    return [f"{scenario.dataset}-{i}" for i in range(scenario.num_repos)]


def all_dataset_keys(
    *, num_repos: int, quick: bool
) -> dict[str, tuple[ChunkProfile, ...]]:
    """Every dataset key the full matrix can use, with its profiles (for setup)."""
    by_name = _profiles_by_name(quick)
    keys: dict[str, tuple[ChunkProfile, ...]] = {
        name: (p,) for name, p in by_name.items()
    }
    keys["mixed"] = tuple(by_name[n] for n in MIXED_NAMES)
    for i in range(num_repos):
        keys[f"multi-{i}"] = (by_name["medium"],)
    return keys


def build_scenarios(
    *,
    where: list[str],
    num_repos: int,
    quick: bool,
    gcs_repo: str | None = None,
    s3_repo: str | None = None,
) -> list[Scenario]:
    by_name = _profiles_by_name(quick)
    profiles = list(by_name.values())
    mixed = tuple(by_name[n] for n in MIXED_NAMES)

    scenarios = []
    for backend in ("local", "s3"):
        if backend not in where:
            continue
        for load in LOADS:
            scenarios.extend(
                Scenario(
                    name=f"{backend}-{load}-{profile.name}",
                    backend=backend,
                    load=load,
                    profiles=(profile,),
                    dataset=profile.name,
                )
                for profile in profiles
            )
            scenarios.append(
                Scenario(
                    name=f"{backend}-{load}-mixed",
                    backend=backend,
                    load=load,
                    profiles=mixed,
                    dataset="mixed",
                )
            )
        # noisy neighbor: victim reads `medium` while an aggressor
        # continuously reads `big`; modes vary how the aggressor is governed
        scenarios.append(
            Scenario(
                name=f"{backend}-contend",
                backend=backend,
                load="contend",
                profiles=(by_name["medium"],),
                dataset="medium",
                aggressor_profile=by_name["big"],
            )
        )
        # multi-repo: N concurrently driven repos, medium profile
        medium = by_name["medium"]
        scenarios.extend(
            Scenario(
                name=f"{backend}-multi-{load}",
                backend=backend,
                load=load,
                profiles=(medium,),
                dataset="multi",
                num_repos=num_repos,
            )
            for load in ("read", "readwrite")
        )
    # realistic reads against existing (not harness-written) Arraylake repos
    if "gcs" in where and gcs_repo:
        scenarios.append(
            Scenario(
                name="gcs-read-external",
                backend="gcs",
                load="read",
                profiles=(),
                dataset=gcs_repo,
                external=True,
            )
        )
    if "s3" in where and s3_repo:
        scenarios.append(
            Scenario(
                name="s3-read-external",
                backend="s3",
                load="read",
                profiles=(),
                dataset=s3_repo,
                external=True,
            )
        )
    return scenarios
