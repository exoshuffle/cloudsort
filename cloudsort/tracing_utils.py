import collections
import functools
import json
import logging
import os
import time
from typing import Callable

import pandas as pd
import ray
from ray.util import metrics

from cloudsort import constants, logging_utils
from cloudsort.config import JobConfig

Span = collections.namedtuple(
    "Span",
    ["time", "duration", "event", "address", "pid"],
)
SAVE_SPANS_EVERY = 1000


class timeit:
    def __init__(
        self,
        event: str,
        report_completed: bool = True,
    ):
        self.event = event
        self.report_completed = report_completed
        self.tracker = None
        self.begin_time = time.time()

    def _get_tracker(self) -> ray.actor.ActorHandle:
        if self.tracker is None:
            self.tracker = get_progress_tracker()
        return self.tracker

    def __enter__(self) -> None:
        if not self.report_completed:
            return
        tracker = self._get_tracker()
        if self.event == "sort":
            tracker.record_start_time.remote()
        tracker.inc.remote(f"{self.event}_started")

    def __exit__(self, *_args) -> bool:
        duration = time.time() - self.begin_time
        tracker = self._get_tracker()
        tracker.record_span.remote(
            Span(
                self.begin_time,
                duration,
                self.event,
                ray.util.get_node_ip_address(),
                os.getpid(),
            ),
        )
        if self.report_completed:
            tracker.inc.remote(
                f"{self.event}_completed",
                echo=True,
            )
        return False


def timeit_wrapper(fn: Callable):
    @functools.wraps(fn)
    def wrapped_fn(*args, **kwargs):
        with timeit(fn.__name__):
            return fn(*args, **kwargs)

    return wrapped_fn


def record_value(*args, **kwargs):
    tracker = get_progress_tracker()
    tracker.record_value.remote(*args, **kwargs)


def get_progress_tracker() -> ray.actor.ActorHandle:
    return ray.get_actor(constants.PROGRESS_TRACKER_ACTOR)


def create_progress_tracker(*args, **kwargs) -> ray.actor.ActorHandle:
    return ProgressTracker.options(name=constants.PROGRESS_TRACKER_ACTOR).remote(
        *args, **kwargs
    )


def _make_trace_event(span: Span):
    return {
        "cat": span.event,
        "name": span.event,
        "pid": span.address,
        "tid": span.pid,
        "ts": int(span.time * 1_000_000),
        "dur": int(span.duration * 1_000_000),
        "ph": "X",
    }


class _DefaultDictWithKey(collections.defaultdict):
    def __missing__(self, key):
        if self.default_factory:
            self[key] = self.default_factory(key)  # pylint: disable=not-callable
            return self[key]
        return super().__missing__(key)


def symlink(src: str, dst: str, **kwargs):
    try:
        os.symlink(src, dst, **kwargs)
    except FileExistsError:
        os.remove(dst)
        os.symlink(src, dst, **kwargs)


@ray.remote(resources={"head": 1e-3})
class ProgressTracker:
    def __init__(self, job_cfg: JobConfig):
        self.job_cfg = job_cfg
        self.counts = collections.defaultdict(int)
        self.gauges = _DefaultDictWithKey(metrics.Gauge)
        self.series = collections.defaultdict(list)
        self.spans = []
        self.start_time = None
        logging_utils.init()
        logging.info(job_cfg)

    def inc(self, metric: str, value: int = 1, echo=False):
        new_value = self.counts[metric] + value
        self.counts[metric] = new_value
        self.gauges[metric].set(new_value)
        if echo:
            logging.info("%s %s", metric, new_value)

    def dec(self, metric: str, value: int = 1, echo=False):
        return self.inc(metric, -value, echo)

    def record_value(
        self,
        metric: str,
        value: float,
        relative_to_start=False,
        echo=False,
    ):
        if relative_to_start and self.start_time:
            value -= self.start_time
        self.series[metric].append(value)
        if echo:
            logging.info("%s %s", metric, value)

    def record_span(self, span: Span, also_record_value=True):
        self.spans.append(span)
        if also_record_value:
            self.record_value(span.event, span.duration)
        if len(self.spans) % SAVE_SPANS_EVERY == 0:
            self.save_trace()

    def record_start_time(self):
        self.start_time = time.time()

    def report(self):
        ret = []
        for key, values in self.series.items():
            ss = pd.Series(values)
            ret.append(
                [
                    key,
                    ss.median(),
                    ss.mean(),
                    ss.std(),
                    ss.max(),
                    ss.min(),
                    ss.count(),
                ]
            )
        df = pd.DataFrame(
            ret, columns=["task", "median", "mean", "std", "max", "min", "count"]
        ).set_index("task")
        pd.set_option("display.max_colwidth", None)
        print(self.series.get("output_time"))
        print(df)
        self.job_cfg.app.worker_ids = []
        self.job_cfg.app.worker_ips = []
        self.job_cfg.app.worker_ip_to_id = {}
        print(self.job_cfg, flush=True)

    def save_trace(self):
        ret = [_make_trace_event(span) for span in self.spans]
        filename = f"/tmp/cloudsort-{self.start_time}.json"
        with open(filename, "w") as fout:
            json.dump(ret, fout)
        symlink(filename, "/tmp/cloudsort-latest.json")

    def performance_report(self):
        self.save_trace()
        self.report()
