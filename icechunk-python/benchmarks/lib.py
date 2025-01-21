import operator
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from itertools import product, repeat

import numpy as np
import pandas as pd
import toolz as tlz


def stats(data):
    q = np.quantile(data, q=[0.25, 0.5, 0.75])
    _, trend = np.polyfit(np.arange(data.size), data, deg=1)
    return {
        "rounds": data.size,
        "max": data.max(),
        "trend": trend,
        "mean": data.mean(),
        "iqr": q[2] - q[0],
        "min": data.min(),
        "median": q[1],
        "q1": q[0],
        "q3": q[2],
        "stddev": np.std(data),
    }


def slices_from_chunks(shape: tuple[int, ...], chunks: tuple[int, ...]):
    """slightly modified from dask.array.core.slices_from_chunks to be lazy"""

    extras = ((s % c,) if s % c > 0 else () for s, c in zip(shape, chunks, strict=True))
    # need this twice
    chunks = tuple(
        tuple(tlz.concatv(repeat(c, s // c), e))
        for s, c, e in zip(shape, chunks, extras, strict=True)
    )
    cumdims = (tlz.accumulate(operator.add, bds[:-1], 0) for bds in chunks)
    slices = (
        (slice(s, s + dim) for s, dim in zip(starts, shapes, strict=True))
        for starts, shapes in zip(cumdims, chunks, strict=True)
    )
    return product(*slices)


def normalize_chunks(
    *, shape: tuple[int, ...], chunks: tuple[int, ...]
) -> tuple[int, ...]:
    assert len(shape) == len(chunks)
    chunks = tuple(s if c == -1 else c for s, c in zip(shape, chunks, strict=True))
    return chunks


def get_task_chunk_shape(
    *, task_nchunks: int, shape: tuple[int, ...], chunks: tuple[int, ...]
) -> tuple[int, ...]:
    left = task_nchunks
    task_chunk_shape = []
    for s, c in zip(shape, chunks, strict=True):
        if c == s or left is None:
            task_chunk_shape.append(c)
        else:
            q, r = divmod(s, c)
            if q > left:
                task_chunk_shape.append(left * c)
            else:
                task_chunk_shape.append(q * c)
                left /= q
    print(f"{task_chunk_shape=!r}")
    return task_chunk_shape


@dataclass
class Timer:
    diagnostics: list = field(default_factory=list)

    @contextmanager
    def time(self, **kwargs):
        tic = time.perf_counter()
        yield
        toc = time.perf_counter()
        kwargs["runtime"] = toc - tic
        self.diagnostics.append(kwargs)

    def as_dict(self) -> dict:
        out = {}
        for item in self.diagnostics:
            out[item["op"]] = item["runtime"]
        return out

    def dataframe(self):
        return pd.DataFrame(self.diagnostics)
