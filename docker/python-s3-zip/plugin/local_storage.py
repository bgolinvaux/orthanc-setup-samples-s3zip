from __future__ import annotations

import logging
import os
import orthanc
import sys
import threading
import subprocess
import time
import queue
import shutil
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, ClassVar, Dict, Iterator, List, Optional, Tuple
from local_storage_interface import (
    LocalStorageInterface,
    S3_UPLOADED_MARKER_NAME,
    S3_UPLOADED_MARKER_TMP_PREFIX,
)
from collections import deque
from s3zip_logging import get_logger

FolderStatEntry = Tuple[float, str, int]


@dataclass
class EvictionResult:
    """Outcome of an eviction pass.

    Returned both by the make-room slow path and by the explicit
    ``evict_all_safe`` admin entry point so callers (REST endpoint, CI tests,
    health page) can render uniform stats.
    """
    freed_folders: int
    freed_bytes: int
    skipped_folders: int
    available_bytes_after: int
    # Bytes held by the folders the pass looked at and refused to delete
    # (leased, not yet on S3, or whose deletion failed). Diagnostic only.
    skipped_bytes: int = 0


@dataclass
class _EvictionCounters:
    """Diagnostic counters behind ``GET /s3-zip/local-cache/eviction-stats`` (QM-10227).

    They exist so that a test, or an operator reading the endpoint, can check
    that the over-eviction headroom does its job without knowing how fast the
    disk is: how many writes came in, how many of them paid for an eviction
    pass, how much each pass freed and how far under the budget it landed.
    Every field is read and written under ``LocalStorage._lock``;
    ``LocalStorage.reset_eviction_stats()`` starts a new observation window.
    """
    since_epoch: float = 0.0
    # write_file() calls, i.e. instances landing in the cache (C-STORE and
    # rehydration alike), and the bytes they reserved.
    write_count: int = 0
    write_bytes: int = 0
    fast_path_count: int = 0
    cooldown_admission_count: int = 0
    # Writers that queued for the scan slot behind another writer's pass and
    # found their room already made when they got it (no scan of their own).
    admitted_after_another_pass_count: int = 0
    # Every `du` over the cache: startup, make-room passes, admin evictions
    # and stats snapshots.
    du_count: int = 0
    du_seconds_total: float = 0.0
    du_seconds_max: float = 0.0
    # Make-room passes (the slow path), classified by where they landed:
    # at or past the over-eviction target, under budget but short of the
    # target, or still over budget. "futile" passes freed nothing at all.
    pass_count: int = 0
    pass_reached_target_count: int = 0
    pass_under_budget_short_of_target_count: int = 0
    pass_over_budget_count: int = 0
    pass_futile_count: int = 0
    # Wall time the writer that triggered the pass was stalled: waiting for
    # in-flight writes, the du, and the deletions.
    pass_seconds_total: float = 0.0
    pass_seconds_max: float = 0.0
    pass_min_available_after_reached_target: Optional[int] = None
    pass_min_available_after: Optional[int] = None
    pass_max_available_after: Optional[int] = None
    evicted_folders: int = 0
    evicted_bytes: int = 0
    evicted_bytes_max_in_pass: int = 0
    # headroom = max_size - bytes the pass may not delete (pending + leased)
    #          - bytes reserved by the writes waiting on the pass,
    # measured at each pass: the best `available` a pass could possibly reach.
    headroom_min_bytes: Optional[int] = None
    headroom_max_bytes: Optional[int] = None
    admin_eviction_count: int = 0
    admin_evicted_folders: int = 0
    admin_evicted_bytes: int = 0


logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Per-module log-level override for eviction / disk-space monitoring.
#
# S3ZIP_LOCAL_STORAGE_LOG_LEVEL overrides the level for the s3zip.local_storage
# logger only, without affecting any other s3zip.* module.  This makes it easy
# to enable verbose eviction tracing in CI without flooding the logs with
# debug output from upload/download/read paths.
#
# Accepted values: DEBUG, INFO, WARNING, ERROR (case-insensitive).
# If unset, the level is inherited from the parent s3zip logger
# (controlled by S3ZIP_LOG_LEVEL, default INFO).
#
# Example (CI compose file):
#   S3ZIP_LOCAL_STORAGE_LOG_LEVEL: DEBUG
# ---------------------------------------------------------------------------
_LOCAL_STORAGE_LOG_LEVEL_ENV_VAR = "S3ZIP_LOCAL_STORAGE_LOG_LEVEL"
_module_level_override: Optional[str] = os.environ.get(_LOCAL_STORAGE_LOG_LEVEL_ENV_VAR)
if _module_level_override:
    _lvl: Any = getattr(logging, _module_level_override.upper(), None)
    if _lvl is not None:
        # Set the level on this module's logger so isEnabledFor() returns True for
        # records at the override level.
        logger.setLevel(_lvl)
        # Also lower the level on any handler attached directly to the s3zip root logger
        # (the one installed by _ensure_s3zip_logging before inject_logger_factory is
        # called).  Without this, Python's callHandlers() would filter the record at the
        # handler even though the originating logger accepted it — the handler-level check
        # is independent of the logger-level check and is the one that matters during
        # propagation.  After inject_logger_factory() the s3zip root handler is removed
        # and replaced by the root-logger handler (level=NOTSET=0), so this adjustment is
        # only relevant during the import-time window and in standalone setups that never
        # call inject_logger_factory().
        _s3zip_root: logging.Logger = logging.getLogger("s3zip")
        for _h in _s3zip_root.handlers:
            if _lvl < _h.level:
                _h.setLevel(_lvl)
        print(
            f"[s3zip] local_storage: log level overridden by "
            f"{_LOCAL_STORAGE_LOG_LEVEL_ENV_VAR}={_module_level_override.upper()}",
            file=sys.stderr,
        )
    else:
        print(
            f"[s3zip] local_storage: ignoring invalid "
            f"{_LOCAL_STORAGE_LOG_LEVEL_ENV_VAR}={_module_level_override!r}",
            file=sys.stderr,
        )


# du(1) exits non-zero (typically 1) when it cannot stat an entry — e.g. a
# series folder is evicted/removed by another thread while du is walking the
# cache. In that case du still prints valid sizes for every surviving entry on
# stdout, so a partial result is safe to use. A run that produces NO usable
# output at all is treated as transient and retried with exponential backoff.
_DU_MAX_ATTEMPTS: int = 3
_DU_RETRY_BACKOFF_BASE_SEC: float = 0.1


# How long to stop re-running the scan+evict slow path after a pass that could
# not free a single byte.
#
# Running out of room is a designed-for state: every folder still waiting for
# its S3 upload is protected, so a cache under pressure legitimately has
# nothing to evict. What must not happen is what the code did before: once
# `_available_size` goes negative, EVERY subsequent instance write takes the
# slow path -- pause all writers, fork a `du -b` over the whole cache, walk the
# LRU queue, free nothing -- and then writes anyway. The cost of ingesting an
# instance becomes proportional to the size of the cache, precisely when the
# server is least able to afford it, and the copy thread that would actually
# release space is competing with that storm for the same I/O.
#
# So after a futile pass we admit writes without rescanning for a short while.
# Nothing is lost by waiting: the pass is a no-op until the copy thread
# publishes a marker, and the next scan recomputes the true occupancy from
# disk, so the accounting self-corrects.
_FUTILE_EVICTION_COOLDOWN_SEC: float = 5.0

# Cap on how many folder names the cache summary lists. The summary is a
# diagnostic payload served over REST, not a directory listing.
_CACHE_SUMMARY_MAX_NAMED_FOLDERS: int = 20


class LocalStorage(LocalStorageInterface):


    _root: str
    _max_size: int    # all sizes are in [bytes]
    # QM-10227: how much of the budget a make-room pass leaves free. The pass
    # is triggered when a write would take the cache over `_max_size` and it
    # runs until `_over_eviction_bytes` are free, i.e. down to
    # `_max_size - _over_eviction_bytes`, so the next pass -- and the next
    # full-cache `du` it starts with -- only comes once ingest has used that
    # headroom up, instead of on the very next write. 0 keeps the pass
    # stopping the moment the triggering write fits. The headroom lives
    # inside the budget; nothing here lets the cache go above `_max_size`.
    _over_eviction_bytes: int
    _available_size: int
    _block_size: int
    _lock: threading.RLock
    _io_condition: threading.Condition
    _active_writers: int
    _scan_in_progress: bool
    _reserved_bytes: int
    _folder_lease_counts: dict[str, int]
    _folder_marker_cs_locks: dict[str, tuple[threading.Lock, int]]
    _folder_stats: queue.PriorityQueue[FolderStatEntry]
    _is_folder_safe_to_evict: Callable[[str], bool] | None
    _futile_eviction_until: float
    _counters: _EvictionCounters

    def __init__(self, root: str, max_size_mb: int, over_eviction_mb: int = 0) -> None:
        self._root = root
        self._max_size = max_size_mb * 1024 * 1024
        self._over_eviction_bytes = self._clamp_over_eviction_bytes(over_eviction_mb * 1024 * 1024)
        self._lock = threading.RLock()
        self._io_condition = threading.Condition()
        self._active_writers = 0
        self._scan_in_progress = False
        self._reserved_bytes = 0
        self._folder_lease_counts = {}
        self._folder_marker_cs_locks = {}
        self._is_folder_safe_to_evict = None
        self._futile_eviction_until = 0.0
        self._counters = _EvictionCounters(since_epoch=time.time())

        self._update_local_storage_stats()

        logger.debug("LocalStorage initialized",
                     root=root,
                     max_size_mb=max_size_mb,
                     max_size_bytes=self._max_size,
                     over_eviction_mb=over_eviction_mb,
                     over_eviction_bytes=self._over_eviction_bytes)

    def _clamp_over_eviction_bytes(self, over_eviction_bytes: int, max_size: Optional[int] = None) -> int:
        """Validate an over-eviction headroom against a budget, the current one by default.

        Negative is a configuration error. Larger than the budget is legal --
        it means "every pass drains everything evictable" -- but the target
        `available >= headroom` could then never be met, which would make the
        pass statistics lie, so it is clamped to the budget with a warning.
        ``set_budget`` passes the budget it is about to apply, so that a
        request can be validated in full before anything changes.
        """
        budget: int = self._max_size if max_size is None else max_size
        if over_eviction_bytes < 0:
            raise ValueError(f"OverEvictionMB must be >= 0, got {over_eviction_bytes} bytes")
        if over_eviction_bytes > budget:
            logger.warning(
                "LocalStorage: OverEvictionMB is larger than LocalStorageMaxSizeMB; "
                "clamping it to the budget (every eviction pass will drain everything evictable)",
                over_eviction_bytes=over_eviction_bytes,
                max_size_bytes=budget,
            )
            return budget
        return over_eviction_bytes

    def set_eviction_guard(self, is_folder_safe_to_evict: Callable[[str], bool]) -> None:
        """
        - Set a callback that determines if a local series folder is safe to evict.
        - The callback receives the folder name (not the full path) and
          must return True if the folder's data has been safely backed up to S3.
        - If this callback is not set, all folders are considered safe to evict.
        """
        self._is_folder_safe_to_evict = is_folder_safe_to_evict

    @contextmanager
    def lease_folder(self, local_series_folder: str) -> Iterator[None]:
        """Keep a series folder stable while a caller depends on its contents.

        Eviction removes whole series folders. A caller that checks a file path
        and then opens it, or extracts several files into the same folder, needs
        the folder to remain present for the full operation rather than only for
        one filesystem call. The lease is a refcount checked by eviction; it is
        not a global I/O mutex.
        """
        self._acquire_folder_lease(local_series_folder)
        try:
            yield
        finally:
            self._release_folder_lease(local_series_folder)

    def _acquire_folder_lease(self, local_series_folder: str) -> None:
        with self._lock:
            self._folder_lease_counts[local_series_folder] = (
                self._folder_lease_counts.get(local_series_folder, 0) + 1
            )
            logger.debug(
                "LocalStorage: folder lease acquired",
                local_series_folder=local_series_folder,
                lease_count=self._folder_lease_counts[local_series_folder],
            )

    def _release_folder_lease(self, local_series_folder: str) -> None:
        with self._lock:
            lease_count = self._folder_lease_counts.get(local_series_folder, 0)
            if lease_count <= 1:
                self._folder_lease_counts.pop(local_series_folder, None)
                lease_count = 0
            else:
                lease_count -= 1
                self._folder_lease_counts[local_series_folder] = lease_count
            logger.debug(
                "LocalStorage: folder lease released",
                local_series_folder=local_series_folder,
                lease_count=lease_count,
            )

    def _get_folder_lease_count(self, local_series_folder: str) -> int:
        return self._folder_lease_counts.get(local_series_folder, 0)

    @contextmanager
    def folder_marker_critical_section(self, local_series_folder: str) -> Iterator[None]:
        """Per-folder mutex for marker publish/invalidate operations.

        Two paths race for the marker file: ``copy_series_to_s3`` writes it
        after a final attachment-set recheck, and ``storage_create`` deletes
        it whenever a new instance lands on disk. Without serialization, the
        copy can publish a marker AFTER the create's delete has already run
        as a no-op, leaving a stale marker that hides an un-uploaded file.

        Acquiring is refcounted: the underlying ``threading.Lock`` is created
        lazily on first use and removed from the dict when no thread is in
        the section, so memory does not grow with the lifetime catalogue of
        series folders.
        """
        lock = self._acquire_folder_marker_cs(local_series_folder)
        try:
            with lock:
                yield
        finally:
            self._release_folder_marker_cs(local_series_folder)

    def _acquire_folder_marker_cs(self, local_series_folder: str) -> threading.Lock:
        with self._lock:
            entry = self._folder_marker_cs_locks.get(local_series_folder)
            if entry is None:
                new_lock = threading.Lock()
                self._folder_marker_cs_locks[local_series_folder] = (new_lock, 1)
                return new_lock
            existing_lock, count = entry
            self._folder_marker_cs_locks[local_series_folder] = (existing_lock, count + 1)
            return existing_lock

    def _release_folder_marker_cs(self, local_series_folder: str) -> None:
        with self._lock:
            entry = self._folder_marker_cs_locks.get(local_series_folder)
            if entry is None:
                return
            existing_lock, count = entry
            if count <= 1:
                del self._folder_marker_cs_locks[local_series_folder]
            else:
                self._folder_marker_cs_locks[local_series_folder] = (existing_lock, count - 1)

    def _update_local_storage_stats(self) -> None:
        self._pause_writes_for_scan()
        try:
            self._update_local_storage_stats_with_writes_paused()
        finally:
            self._resume_writes_after_scan()

    def _pause_writes_for_scan(self) -> None:
        # A scan must not overlap physical writes: otherwise `du` can see a
        # finished write while the reservation still counts it as in-flight.
        with self._io_condition:
            while self._scan_in_progress:
                self._io_condition.wait()
            self._scan_in_progress = True
            # If wait() is interrupted (e.g. by a signal) after we have already
            # claimed the scan slot, we must clear the flag and wake any other
            # blocked threads. Otherwise _scan_in_progress stays True forever
            # and every subsequent _enter_write / scan-pauser deadlocks.
            try:
                while self._active_writers > 0:
                    self._io_condition.wait()
            except BaseException:
                self._scan_in_progress = False
                self._io_condition.notify_all()
                raise

    def _resume_writes_after_scan(self) -> None:
        with self._io_condition:
            self._scan_in_progress = False
            self._io_condition.notify_all()

    def _enter_write(self) -> None:
        with self._io_condition:
            while self._scan_in_progress:
                self._io_condition.wait()
            self._active_writers += 1

    def _exit_write(self) -> None:
        with self._io_condition:
            self._active_writers -= 1
            if self._active_writers == 0:
                self._io_condition.notify_all()

    def _run_du_capture(self, cmd: List[str]) -> str:
        """Run ``du`` and return its stdout, hardened against the local cache
        mutating mid-scan.

        ``du`` exits non-zero when it fails to stat an entry — e.g. a series
        folder is evicted/removed by another thread while ``du`` walks the
        cache. When that happens ``du`` still prints valid sizes for every
        surviving entry on stdout, so we accept that partial output instead of
        letting a ``CalledProcessError`` crash the caller: the missing folders
        are exactly the ones being removed, so under-counting their bytes is
        self-correcting and gets picked up on the next scan. A run that yields
        no usable output at all is treated as transient (e.g. an ``ENOMEM``
        fork failure under load) and retried with exponential backoff before
        finally giving up.
        """
        last_stderr: str = ""
        for attempt in range(1, _DU_MAX_ATTEMPTS + 1):
            result: subprocess.CompletedProcess[str] = subprocess.run(
                cmd, capture_output=True, text=True
            )
            if result.returncode == 0:
                return result.stdout

            last_stderr = (result.stderr or "").strip()
            if result.stdout.strip():
                logger.warning(
                    "du exited non-zero but produced usable output; treating it as a partial scan (cache mutated mid-walk)",
                    returncode=result.returncode,
                    attempt=attempt,
                    stderr=last_stderr[:500],
                )
                return result.stdout

            logger.warning(
                "du produced no usable output; will retry with backoff",
                returncode=result.returncode,
                attempt=attempt,
                max_attempts=_DU_MAX_ATTEMPTS,
                stderr=last_stderr[:500],
            )
            if attempt < _DU_MAX_ATTEMPTS:
                backoff_sec: float = _DU_RETRY_BACKOFF_BASE_SEC * (1 << (attempt - 1))
                time.sleep(backoff_sec)

        raise RuntimeError(
            f"du failed to produce usable output after {_DU_MAX_ATTEMPTS} attempts for command {cmd!r}; last stderr: {last_stderr[:500]!r}"
        )

    def _update_local_storage_stats_with_writes_paused(self) -> None:
        block_size: int = os.statvfs(self._root).f_frsize
        folder_stats: queue.PriorityQueue[FolderStatEntry] = queue.PriorityQueue()

        cmd: List[str] = ["du", "-b", "--max-depth=1", self._root]
        du_started: float = time.monotonic()
        du_stdout: str = self._run_du_capture(cmd)
        du_seconds: float = time.monotonic() - du_started
        lines: List[str] = du_stdout.strip().split("\n")

        total_folders: int = 0
        total_apparent_bytes: int = 0
        for line in lines:
            if "\t" not in line:
                # Defensive: du emits complete "<size>\t<path>" lines, but a
                # mutating-cache scan is exactly when we must not trust that a
                # blank/partial line never sneaks in.
                continue
            size_str, path = line.split("\t", 1)
            folder_size: int = int(size_str)
            if path != self._root:
                last_modified: float = os.path.getmtime(path)
                folder_stats.put((last_modified, path, folder_size))
                total_folders += 1
                total_apparent_bytes += folder_size

        with self._lock:
            prev_available: Optional[int] = getattr(self, '_available_size', None)
            self._block_size = block_size
            self._folder_stats = folder_stats
            self._available_size = self._max_size - total_apparent_bytes - self._reserved_bytes
            self._counters.du_count += 1
            self._counters.du_seconds_total += du_seconds
            self._counters.du_seconds_max = max(self._counters.du_seconds_max, du_seconds)

            logger.debug(
                "LocalStorage: disk stats refreshed",
                max_size_mb=self._max_size // (1024 * 1024),
                max_size_bytes=self._max_size,
                block_size=self._block_size,
                total_folders=total_folders,
                total_apparent_bytes=total_apparent_bytes,
                total_apparent_mb=round(total_apparent_bytes / (1024 * 1024), 2),
                reserved_bytes=self._reserved_bytes,
                reserved_mb=round(self._reserved_bytes / (1024 * 1024), 2),
                available_bytes=self._available_size,
                available_mb=round(self._available_size / (1024 * 1024), 2),
                prev_available_bytes=prev_available,
                du_seconds=round(du_seconds, 3),
            )


    def _evict_until(self, target_available_bytes: Optional[int]) -> EvictionResult:
        """Evict oldest-first folders until ``self._available_size`` reaches the target.

        Caller MUST hold ``self._lock`` and MUST have paused writes via
        ``_pause_writes_for_scan``. Caller also MUST have refreshed
        ``self._folder_stats`` (e.g. via ``_update_local_storage_stats``)
        before calling: this method neither acquires the lock nor refreshes
        the queue.

        Args:
            target_available_bytes: stop the loop as soon as
                ``self._available_size >= target_available_bytes``. Pass
                ``None`` to drain the LRU queue completely (i.e. evict
                everything that the eviction guard considers safe to evict).

        Folders with active leases are skipped because another thread is using
        their contents across multiple filesystem calls. Folders without a
        ``.s3-uploaded`` marker are protected by the eviction guard set via
        ``set_eviction_guard``. Skipped folders are put back into the queue so
        they remain candidates for future eviction.
        """
        skipped: List[FolderStatEntry] = []
        freed_bytes: int = 0
        freed_folders: int = 0

        def _need_more() -> bool:
            if target_available_bytes is None:
                return True
            return self._available_size < target_available_bytes

        while _need_more() and not self._folder_stats.empty():
            entry: FolderStatEntry = self._folder_stats.get()
            _, path, folder_size = entry

            folder_name: str = os.path.basename(path)
            lease_count = self._get_folder_lease_count(folder_name)
            if lease_count > 0:
                # DEBUG, not INFO: skipping is the no-op outcome, and it is by
                # far the most common one -- a pass over a cache of protected
                # folders emits one line per folder per pass. In the CI run
                # that motivated this, "skipping eviction" accounted for 8700
                # lines and nearly half the container's log budget, which
                # clipped the log before the end of the test. The pass summary
                # (evict_all_safe / _make_room) already reports how many
                # folders were skipped.
                logger.debug(
                    "LocalStorage: skipping eviction of leased folder",
                    folder=folder_name,
                    folder_size=folder_size,
                    lease_count=lease_count)
                skipped.append(entry)
                continue

            if self._is_folder_safe_to_evict is not None:
                try:
                    safe = self._is_folder_safe_to_evict(folder_name)
                except Exception as e:
                    logger.warning("eviction guard check failed, skipping folder",
                                   folder=folder_name, error=str(e))
                    safe = False

                if not safe:
                    logger.debug(
                        "LocalStorage: skipping eviction of folder not yet on S3",
                        folder=folder_name, folder_size=folder_size)
                    skipped.append(entry)
                    continue

            # INFO, unlike the skips above: this is the destructive branch and
            # the only record that a given series left the local cache. It used
            # to be the other way round -- skips at INFO, the actual delete at
            # DEBUG plus an orthanc.LogInfo that Orthanc's default verbosity
            # drops -- so a post-mortem could see every folder that survived
            # and not one that was deleted.
            orthanc.LogInfo(f"LocalStorage: reclaiming space by deleting local folder '{path}'")
            logger.info(
                "LocalStorage: evicting folder",
                folder=folder_name,
                folder_size=folder_size,
                available_before=self._available_size,
            )

            try:
                shutil.rmtree(path)
            except FileNotFoundError:
                logger.info(
                    "LocalStorage: folder already gone during eviction",
                    folder=folder_name,
                    folder_size=folder_size,
                )
            except OSError as e:
                logger.warning(
                    "LocalStorage: failed to evict folder, leaving it queued for a future pass",
                    folder=folder_name,
                    folder_size=folder_size,
                    error=str(e),
                )
                skipped.append(entry)
                continue
            self._available_size += folder_size
            freed_bytes += folder_size
            freed_folders += 1

            logger.debug(
                "LocalStorage: folder evicted",
                folder=folder_name,
                folder_size=folder_size,
                available_after=self._available_size,
            )

        # put skipped entries back
        for entry in skipped:
            self._folder_stats.put(entry)

        return EvictionResult(
            freed_folders=freed_folders,
            freed_bytes=freed_bytes,
            skipped_folders=len(skipped),
            available_bytes_after=self._available_size,
            skipped_bytes=sum(entry[2] for entry in skipped),
        )

    def _protected_bytes_locked(self) -> int:
        """Bytes in the folders an eviction pass may not delete right now.

        Leased folders plus the folders the eviction guard refuses (no
        ``.s3-uploaded`` marker). ``max_size - protected - reserved`` is the
        headroom a pass can reach at best (``_record_pass_locked``): below the
        over-eviction target the pass cannot meet its target whatever it
        deletes, and below zero the pending set alone is over budget. One
        ``exists()`` per series folder, right after a ``du`` that walked every
        one of them, so the dentries are warm. Caller MUST hold ``self._lock``.
        """
        protected: int = 0
        for _last_modified, path, folder_size in list(self._folder_stats.queue):
            folder_name: str = os.path.basename(path)
            if self._get_folder_lease_count(folder_name) > 0:
                protected += folder_size
                continue
            if self._is_folder_safe_to_evict is None:
                continue
            try:
                safe = self._is_folder_safe_to_evict(folder_name)
            except Exception:
                safe = False
            if not safe:
                protected += folder_size
        return protected

    def _record_pass_locked(self,
                            result: EvictionResult,
                            target_available_bytes: int,
                            protected_bytes: int,
                            pass_seconds: float) -> None:
        """Fold one make-room pass into the diagnostic counters. Caller MUST hold ``self._lock``."""
        c: _EvictionCounters = self._counters
        after: int = result.available_bytes_after

        c.pass_count += 1
        c.pass_seconds_total += pass_seconds
        c.pass_seconds_max = max(c.pass_seconds_max, pass_seconds)

        if after >= target_available_bytes:
            c.pass_reached_target_count += 1
            c.pass_min_available_after_reached_target = (
                after if c.pass_min_available_after_reached_target is None
                else min(c.pass_min_available_after_reached_target, after)
            )
        elif after >= 0:
            c.pass_under_budget_short_of_target_count += 1
        else:
            c.pass_over_budget_count += 1
        if result.freed_bytes == 0:
            c.pass_futile_count += 1

        c.pass_min_available_after = after if c.pass_min_available_after is None else min(c.pass_min_available_after, after)
        c.pass_max_available_after = after if c.pass_max_available_after is None else max(c.pass_max_available_after, after)

        c.evicted_folders += result.freed_folders
        c.evicted_bytes += result.freed_bytes
        c.evicted_bytes_max_in_pass = max(c.evicted_bytes_max_in_pass, result.freed_bytes)

        # The best `available` this pass could have reached: everything
        # evictable gone, leaving the protected folders and the bytes the
        # writers waiting on the pass (the triggering one included) have
        # reserved and are about to land. `available` itself is
        # `max_size - on disk - reserved`, so the reservations count here too.
        headroom: int = self._max_size - protected_bytes - self._reserved_bytes
        c.headroom_min_bytes = headroom if c.headroom_min_bytes is None else min(c.headroom_min_bytes, headroom)
        c.headroom_max_bytes = headroom if c.headroom_max_bytes is None else max(c.headroom_max_bytes, headroom)

    def _make_room(self, size: int) -> int:
        reservation_size: int = max(size, 0)

        with self._lock:
            self._available_size -= reservation_size
            self._reserved_bytes += reservation_size
            self._counters.write_count += 1
            self._counters.write_bytes += reservation_size

            logger.debug(
                "LocalStorage: _make_room called",
                requested_bytes=size,
                reservation_bytes=reservation_size,
                reserved_bytes=self._reserved_bytes,
                current_available_bytes=self._available_size,
                current_available_mb=round(self._available_size / (1024 * 1024), 2),
            )

            if self._available_size >= 0:
                self._counters.fast_path_count += 1
                logger.debug(
                    "LocalStorage: fast path - sufficient space, no eviction needed",
                    available_after_bytes=self._available_size,
                    available_after_mb=round(self._available_size / (1024 * 1024), 2),
                    reserved_bytes=self._reserved_bytes,
                )
                return reservation_size

            if time.monotonic() < self._futile_eviction_until:
                # A recent pass already established that nothing here is
                # evictable. Admit the write without re-scanning; see
                # _FUTILE_EVICTION_COOLDOWN_SEC.
                self._counters.cooldown_admission_count += 1
                logger.debug(
                    "LocalStorage: over budget but a recent eviction pass freed nothing; "
                    "admitting the write without re-scanning",
                    reservation_bytes=reservation_size,
                    available_bytes=self._available_size,
                    cooldown_remaining_sec=round(self._futile_eviction_until - time.monotonic(), 2),
                )
                return reservation_size

            logger.debug(
                "LocalStorage: slow path - available space insufficient, refreshing disk stats",
                reservation_bytes=reservation_size,
                available_before_refresh_bytes=self._available_size,
                reserved_bytes=self._reserved_bytes,
            )

        scan_paused = False
        # Measured from here: the wall time this writer -- and, once the scan
        # slot is taken, every other writer -- is stalled by the pass.
        pass_started: float = time.monotonic()
        try:
            self._pause_writes_for_scan()
            scan_paused = True

            # Writers that cross the budget together queue on the scan slot.
            # The first one's pass refreshes the occupancy with every waiting
            # reservation already counted, then evicts until the headroom is
            # free, so by the time the next one gets the slot the room it
            # reserved is there. Re-running the `du` and the pass for it would
            # stall every writer once more, for nothing: with N concurrent
            # writers (a PACS with several associations, a parallel upload)
            # that was N full-cache scans per budget crossing instead of one.
            with self._lock:
                if self._available_size >= 0:
                    self._counters.admitted_after_another_pass_count += 1
                    logger.debug(
                        "LocalStorage: room already made by a concurrent pass; skipping the scan",
                        reservation_bytes=reservation_size,
                        available_bytes=self._available_size,
                        reserved_bytes=self._reserved_bytes,
                    )
                    return reservation_size

            self._update_local_storage_stats_with_writes_paused()

            logger.debug(
                "LocalStorage: disk stats refreshed, starting eviction loop",
                reservation_bytes=reservation_size,
            )

            with self._lock:
                # QM-10227: the pass evicts until `_over_eviction_bytes` are
                # free, not until the triggering write merely fits. With a
                # zero target every write past the budget re-ran the full
                # cache `du` above, because each pass freed one series and the
                # next write was over budget again. The trigger is unchanged
                # (available < 0); only the target moves, and the warning and
                # futile cooldown below still key on the hard budget.
                target_available_bytes: int = self._over_eviction_bytes

                logger.debug(
                    "LocalStorage: starting eviction loop",
                    reservation_bytes=reservation_size,
                    available_after_refresh_bytes=self._available_size,
                    reserved_bytes=self._reserved_bytes,
                    folders_queued=self._folder_stats.qsize(),
                    target_available_bytes=target_available_bytes,
                )

                # Reclaim space -- evict oldest folders first, but protect folders
                # whose data has not yet been safely backed up to S3.
                # This can be useful if the plugin receives a burst of uploads that
                # exceed the local storage capacity, but the S3 upload process is
                # still catching up
                result = self._evict_until(target_available_bytes=target_available_bytes)

                protected_bytes: int = self._protected_bytes_locked()
                pass_seconds: float = time.monotonic() - pass_started
                self._record_pass_locked(result, target_available_bytes, protected_bytes, pass_seconds)

                # INFO: passes are meant to be rare now, and one line per pass
                # is what lets an operator see the headroom at work (or not).
                logger.info(
                    "LocalStorage: eviction pass complete",
                    freed_folders=result.freed_folders,
                    freed_bytes=result.freed_bytes,
                    freed_mb=round(result.freed_bytes / (1024 * 1024), 2),
                    skipped_folders=result.skipped_folders,
                    protected_bytes=protected_bytes,
                    protected_mb=round(protected_bytes / (1024 * 1024), 2),
                    target_available_bytes=target_available_bytes,
                    reached_target=self._available_size >= target_available_bytes,
                    reservation_bytes=reservation_size,
                    reserved_bytes=self._reserved_bytes,
                    available_after_eviction_bytes=self._available_size,
                    available_after_eviction_mb=round(self._available_size / (1024 * 1024), 2),
                    pass_seconds=round(pass_seconds, 3),
                )

                if self._available_size < 0:
                    # Expected under sustained ingest: everything still on its
                    # way to S3 is protected. The write proceeds anyway -- the
                    # budget is a target, not a hard wall, and failing the
                    # C-STORE would push the problem onto the modality for
                    # what is usually a transient backlog. The real filesystem
                    # remains the hard limit, and a write that hits ENOSPC
                    # does fail the C-STORE.
                    if result.freed_bytes == 0:
                        self._futile_eviction_until = (
                            time.monotonic() + _FUTILE_EVICTION_COOLDOWN_SEC
                        )
                    logger.warning(
                        "LocalStorage: could not free enough space. "
                        "Some folders are protected because they are not yet on S3.",
                        needed=-self._available_size,
                        available=self._available_size,
                        available_mb=round(self._available_size / (1024 * 1024), 2),
                        reserved_bytes=self._reserved_bytes,
                        protected_folders=result.skipped_folders,
                        freed_bytes=result.freed_bytes,
                        rescan_paused_for_sec=(
                            _FUTILE_EVICTION_COOLDOWN_SEC if result.freed_bytes == 0 else 0
                        ),
                        max_size_mb=self._max_size // (1024 * 1024),
                        over_eviction_bytes=self._over_eviction_bytes,
                    )
                else:
                    self._futile_eviction_until = 0.0
        except Exception:
            self._rollback_write_reservation(reservation_size)
            raise
        finally:
            if scan_paused:
                self._resume_writes_after_scan()

        return reservation_size

    def _commit_write_reservation(self, reserved_bytes: int) -> None:
        if reserved_bytes <= 0:
            return

        with self._lock:
            self._reserved_bytes = max(0, self._reserved_bytes - reserved_bytes)
            logger.debug(
                "LocalStorage: write reservation committed",
                released_reserved_bytes=reserved_bytes,
                reserved_bytes=self._reserved_bytes,
                available_bytes=self._available_size,
            )

    def _rollback_write_reservation(self, reserved_bytes: int) -> None:
        if reserved_bytes <= 0:
            return

        with self._lock:
            self._reserved_bytes = max(0, self._reserved_bytes - reserved_bytes)
            self._available_size += reserved_bytes

    def _release_deleted_file_bytes(self, file_size: int) -> None:
        """Give a deleted file's bytes back to the budget.

        Deliberately NOT _rollback_write_reservation: that one also decrements
        ``_reserved_bytes``, which belongs to writes that are still in flight.
        Cancelling part of another thread's reservation makes the cache look
        emptier than it is until the next scan, which is the wrong direction
        to be wrong in when the point of the budget is to keep the disk from
        filling.
        """
        if file_size <= 0:
            return

        with self._lock:
            self._available_size += file_size

    def _touch_lru_reference(self, folder_path: str) -> None:
        try:
            os.utime(folder_path, None)
        except OSError as e:
            logger.debug(
                "LocalStorage: failed to update folder LRU timestamp",
                folder=folder_path,
                error=str(e),
            )

    def evict_all_safe(self) -> EvictionResult:
        """Force-evict every locally-cached series that has been safely uploaded to S3.

        Admin/diagnostic entry point. Refreshes the LRU queue from disk, then
        drains it: every folder whose eviction guard returns True
        (``.s3-uploaded`` marker present) is removed from the local cache;
        folders still in flight are skipped and remain on disk.

        Synchronous: pauses concurrent ``write_file`` disk writes during the
        scan/eviction pass. The cost is O(num_local_folders) ``shutil.rmtree``
        calls plus one
        ``du -b --max-depth=1`` invocation.

        Returns an ``EvictionResult`` describing what was freed.
        """
        self._pause_writes_for_scan()
        try:
            self._update_local_storage_stats_with_writes_paused()
            with self._lock:
                result = self._evict_until(target_available_bytes=None)

                self._counters.admin_eviction_count += 1
                self._counters.admin_evicted_folders += result.freed_folders
                self._counters.admin_evicted_bytes += result.freed_bytes

                if result.freed_bytes > 0:
                    # Something became evictable, so the "nothing to free"
                    # conclusion that paused the make-room rescans no longer
                    # holds.
                    self._futile_eviction_until = 0.0

                logger.info(
                    "LocalStorage: evict_all_safe complete",
                    freed_folders=result.freed_folders,
                    freed_bytes=result.freed_bytes,
                    skipped_folders=result.skipped_folders,
                    available_after_bytes=self._available_size,
                    available_after_mb=round(self._available_size / (1024 * 1024), 2),
                    reserved_bytes=self._reserved_bytes,
                )
                return result
        finally:
            self._resume_writes_after_scan()

    def get_cache_summary(self, marker_filename: str = S3_UPLOADED_MARKER_NAME) -> Dict[str, int]:
        """Return a snapshot of local-cache occupancy.

        Refreshes the disk stats (does NOT evict) and walks the temp folder
        once to count how many series are already on S3 (i.e. have the
        ``marker_filename`` sentinel) versus still in flight.

        The counts are advisory — they are racy with the upload thread.
        """
        self._pause_writes_for_scan()
        try:
            self._update_local_storage_stats_with_writes_paused()
            uploaded: int = 0
            pending: int = 0
            # Which series are holding the cache open, not just how many.
            # "1 folder not on S3" sends you to the logs; the folder's name
            # sends you to the series.
            pending_names: List[str] = []
            try:
                for name in os.listdir(self._root):
                    folder: str = os.path.join(self._root, name)
                    if not os.path.isdir(folder):
                        continue
                    if os.path.exists(os.path.join(folder, marker_filename)):
                        uploaded += 1
                    else:
                        pending += 1
                        if len(pending_names) < _CACHE_SUMMARY_MAX_NAMED_FOLDERS:
                            pending_names.append(name)
            except FileNotFoundError:
                pass
            with self._lock:
                total_folders: int = self._folder_stats.qsize()
                used_bytes: int = self._max_size - self._available_size
                return {
                    "max_bytes": self._max_size,
                    "over_eviction_bytes": self._over_eviction_bytes,
                    "available_bytes": self._available_size,
                    "used_bytes": used_bytes,
                    "reserved_bytes": self._reserved_bytes,
                    "total_folders": total_folders,
                    "folders_on_s3": uploaded,
                    "folders_not_on_s3": pending,
                    "folders_not_on_s3_names": pending_names,
                }
        finally:
            self._resume_writes_after_scan()

    def get_eviction_stats(self) -> Dict[str, Any]:
        """Snapshot of the make-room diagnostic counters (QM-10227).

        Pure bookkeeping: no scan, no eviction, no I/O. See
        ``_EvictionCounters`` for what each group means.
        """
        def _avg(total: float, count: int) -> float:
            return total / count if count else 0.0

        with self._lock:
            c: _EvictionCounters = self._counters
            return {
                "since_epoch": round(c.since_epoch, 3),
                "window_seconds": round(time.time() - c.since_epoch, 3),
                "config": {
                    "max_bytes": self._max_size,
                    "over_eviction_bytes": self._over_eviction_bytes,
                    # The cache size right after a pass that reached its
                    # target, and the largest pending set for which a pass can
                    # still reach it.
                    "effective_threshold_bytes": self._max_size - self._over_eviction_bytes,
                    "available_bytes": self._available_size,
                    "reserved_bytes": self._reserved_bytes,
                },
                "writes": {
                    "count": c.write_count,
                    "bytes": c.write_bytes,
                    "avg_bytes": round(_avg(c.write_bytes, c.write_count)),
                    "fast_path": c.fast_path_count,
                    "admitted_during_cooldown": c.cooldown_admission_count,
                    "admitted_after_another_pass": c.admitted_after_another_pass_count,
                    "slow_path": c.pass_count,
                },
                "du": {
                    "count": c.du_count,
                    "seconds_total": round(c.du_seconds_total, 3),
                    "seconds_max": round(c.du_seconds_max, 3),
                    "seconds_avg": round(_avg(c.du_seconds_total, c.du_count), 3),
                },
                "passes": {
                    "count": c.pass_count,
                    "reached_target": c.pass_reached_target_count,
                    "under_budget_short_of_target": c.pass_under_budget_short_of_target_count,
                    "over_budget": c.pass_over_budget_count,
                    "futile": c.pass_futile_count,
                    "seconds_total": round(c.pass_seconds_total, 3),
                    "seconds_max": round(c.pass_seconds_max, 3),
                    "seconds_avg": round(_avg(c.pass_seconds_total, c.pass_count), 3),
                    "min_available_after_reached_target_bytes": c.pass_min_available_after_reached_target,
                    "min_available_after_bytes": c.pass_min_available_after,
                    "max_available_after_bytes": c.pass_max_available_after,
                },
                "evictions": {
                    "folders": c.evicted_folders,
                    "bytes": c.evicted_bytes,
                    "avg_bytes_per_pass": round(_avg(c.evicted_bytes, c.pass_count)),
                    "avg_bytes_per_folder": round(_avg(c.evicted_bytes, c.evicted_folders)),
                    "max_bytes_in_pass": c.evicted_bytes_max_in_pass,
                },
                "headroom": {
                    "min_bytes": c.headroom_min_bytes,
                    "max_bytes": c.headroom_max_bytes,
                },
                "admin_evictions": {
                    "count": c.admin_eviction_count,
                    "folders": c.admin_evicted_folders,
                    "bytes": c.admin_evicted_bytes,
                },
            }

    def reset_eviction_stats(self) -> Dict[str, Any]:
        """Start a new observation window for ``get_eviction_stats``; returns the fresh snapshot."""
        with self._lock:
            self._counters = _EvictionCounters(since_epoch=time.time())
            logger.info("LocalStorage: eviction stats reset")
            return self.get_eviction_stats()

    def get_budget(self) -> Dict[str, int]:
        """The budget and over-eviction headroom in force, in the config's MB units and in bytes."""
        with self._lock:
            return {
                "LocalStorageMaxSizeMB": self._max_size // (1024 * 1024),
                "OverEvictionMB": self._over_eviction_bytes // (1024 * 1024),
                "max_bytes": self._max_size,
                "over_eviction_bytes": self._over_eviction_bytes,
                "available_bytes": self._available_size,
            }

    def set_budget(self,
                   max_size_mb: Optional[int] = None,
                   over_eviction_mb: Optional[int] = None) -> Dict[str, int]:
        """Change the budget and/or the over-eviction headroom at runtime (QM-10227).

        Backs ``PUT /s3-zip/local-cache/budget``. Nothing is rescanned:
        ``_available_size`` is ``budget - bytes on disk - reserved``, so a new
        budget moves it by the difference. Nothing is evicted here either; a
        smaller budget takes effect on the next write, which finds itself over
        budget and runs a pass. The futile-pass cooldown is cleared so that
        write does rescan rather than coast on a conclusion drawn under the
        old budget. The change lives in memory only: a restart goes back to
        the configuration file. Both inputs are validated before anything is
        touched, so a rejected request (a 400 from the REST handler) leaves
        the budget exactly as it was, even when only the second key is bad.
        """
        with self._lock:
            new_max_size: int = self._max_size
            if max_size_mb is not None:
                if max_size_mb < 0:
                    raise ValueError(f"LocalStorageMaxSizeMB must be >= 0, got {max_size_mb}")
                new_max_size = max_size_mb * 1024 * 1024
            # Re-clamping the existing headroom against the new budget covers
            # the budget shrinking under a headroom set earlier.
            new_over_eviction_bytes: int = self._clamp_over_eviction_bytes(
                over_eviction_mb * 1024 * 1024 if over_eviction_mb is not None else self._over_eviction_bytes,
                max_size=new_max_size,
            )

            self._available_size += new_max_size - self._max_size
            self._max_size = new_max_size
            self._over_eviction_bytes = new_over_eviction_bytes
            self._futile_eviction_until = 0.0

            logger.info(
                "LocalStorage: budget changed at runtime",
                max_size_mb=self._max_size // (1024 * 1024),
                max_size_bytes=self._max_size,
                over_eviction_mb=self._over_eviction_bytes // (1024 * 1024),
                over_eviction_bytes=self._over_eviction_bytes,
                available_bytes=self._available_size,
                available_mb=round(self._available_size / (1024 * 1024), 2),
            )
            return self.get_budget()


    def write_file(self, local_series_folder: str, uuid: str, content: bytes) -> None:
        reserved_bytes: int = self._make_room(len(content))
        write_failed: bool = False

        self._enter_write()
        try:
            try:
                self._write_file(uuid=uuid,
                                 local_series_folder=local_series_folder,
                                 content_type=orthanc.ContentType.DICOM,
                                 content=content)
            except Exception:
                write_failed = True
                self._rollback_write_reservation(reserved_bytes)
                raise
            else:
                self._commit_write_reservation(reserved_bytes)
        finally:
            self._exit_write()
            if write_failed:
                try:
                    self._update_local_storage_stats()
                except Exception as e:
                    logger.warning(
                        "LocalStorage: failed to refresh stats after write failure; restored reservation optimistically",
                        released_reserved_bytes=reserved_bytes,
                        error=str(e),
                    )


    def _write_file(self, uuid: str, local_series_folder: str, content_type: orthanc.ContentType, content: bytes) -> None:

        path: str = self.get_local_path(uuid=uuid,
                                        local_series_folder=local_series_folder,
                                        content_type=content_type)

        logger.debug("writing file to local storage",
                     uuid=uuid,
                     local_series_folder=local_series_folder,
                     path=path,
                     size_bytes=len(content))

        os.makedirs(os.path.dirname(path), exist_ok=True)

        try:
            try:
                with open(path, "wb") as f:
                    f.write(content)
            except FileNotFoundError:
                # The parent folder disappeared between the makedirs and the
                # open: eviction, or the empty-folder cleanup in remove(), got
                # in between. Re-create it and try once more rather than
                # failing a C-STORE over a lost race.
                logger.debug("series folder vanished between makedirs and open; retrying once",
                             uuid=uuid,
                             path=path)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "wb") as f:
                    f.write(content)
        except BaseException:
            # A write that fails part-way -- ENOSPC is the case that matters,
            # and running out of space is a designed-for condition, not an
            # exotic one -- leaves a truncated file behind. Orthanc fails the
            # C-STORE and never records the attachment, so nothing will ever
            # read those bytes, but they are far from harmless: they occupy
            # the cache, and they are a file no future zip can account for,
            # which would keep this folder permanently ineligible for eviction
            # (see _files_on_disk_not_in_zip). Remove it.
            try:
                os.remove(path)
            except OSError as cleanup_error:
                logger.warning("failed to remove a partially-written file after a write error",
                               uuid=uuid,
                               path=path,
                               error=str(cleanup_error))
            raise

        self._touch_lru_reference(os.path.dirname(path))

        logger.debug("file written to local storage",
                     uuid=uuid,
                     path=path,
                     size_bytes=len(content))

    def read_file(self, uuid: str, local_series_folder: str) -> bytes:

        return self._read_file(uuid=uuid,
                               local_series_folder=local_series_folder,
                               content_type=orthanc.ContentType.DICOM,
                               range_start=0,
                               size=0)

    def _read_file(self,
                   uuid: str,
                   local_series_folder: str,
                   content_type: orthanc.ContentType,
                   range_start: int,
                   size: int) -> bytes:

        path: str = self.get_local_path(uuid=uuid,
                                        local_series_folder=local_series_folder,
                                        content_type=content_type)

        logger.debug("reading file from local storage",
                     uuid=uuid,
                     path=path,
                     range_start=range_start,
                     requested_size=size)

        with open(path, "rb") as f:
            if range_start > 0:
                f.seek(range_start)

            if size > 0:
                data: bytes = f.read(size)
            else:
                data = f.read()

        logger.debug("file read from local storage",
                     uuid=uuid,
                     path=path,
                     bytes_read=len(data),
                     range_start=range_start)
        return data


    SUPPORTED_CONTENT_TYPES: ClassVar[Tuple[orthanc.ContentType, ...]] = (orthanc.ContentType.DICOM, orthanc.ContentType.DICOM_UNTIL_PIXEL_DATA)

    def create(self,
               uuid: str,
               local_series_folder: str,
               content_type: orthanc.ContentType,
               compression_type: orthanc.CompressionType,
               content: bytes) -> orthanc.ErrorCode:

        if content_type not in self.SUPPORTED_CONTENT_TYPES:
            raise RuntimeError(f"Unsupported content type: {content_type}")

        logger.debug("create called",
                     uuid=uuid,
                     local_series_folder=local_series_folder,
                     content_type=str(content_type),
                     size_bytes=len(content))

        try:
            self.write_file(uuid=uuid,
                            local_series_folder=local_series_folder,
                            content=content)

            logger.debug("create succeeded", uuid=uuid, local_series_folder=local_series_folder)
            return orthanc.ErrorCode.SUCCESS
        except IOError as e:
            logger.error("IO error creating local storage file",
                         uuid=uuid,
                         local_series_folder=local_series_folder,
                         error=str(e))
            return orthanc.ErrorCode.PLUGIN
        except Exception as e:
            logger.error("unexpected error creating local storage file",
                         uuid=uuid,
                         local_series_folder=local_series_folder,
                         error=str(e))
            return orthanc.ErrorCode.PLUGIN


    def read_range(self,
                   uuid: str,
                   local_series_folder: str,
                   content_type: orthanc.ContentType,
                   range_start: int,
                   size: int) -> Tuple[orthanc.ErrorCode, Optional[bytes]]:

        logger.debug("read_range called",
                     uuid=uuid,
                     local_series_folder=local_series_folder,
                     content_type=str(content_type),
                     range_start=range_start,
                     size=size)

        try:
            data: bytes = self._read_file(uuid=uuid,
                                          local_series_folder=local_series_folder,
                                          content_type=content_type,
                                          range_start=range_start,
                                          size=size)

            logger.debug("read_range succeeded",
                         uuid=uuid,
                         bytes_read=len(data))
            return orthanc.ErrorCode.SUCCESS, data
        except FileNotFoundError:
            logger.error("file not found in local storage",
                         uuid=uuid,
                         local_series_folder=local_series_folder,
                         content_type=str(content_type))
            return orthanc.ErrorCode.UNKNOWN_RESOURCE, None
        except Exception as e:
            logger.error("error reading file from local storage",
                         uuid=uuid,
                         local_series_folder=local_series_folder,
                         error=str(e))
            return orthanc.ErrorCode.PLUGIN, None


    def remove(self,
               uuid: str,
               local_series_folder: str,
               content_type: orthanc.ContentType,
               file_size: int) -> None:

        # Note: if it appears that deletions are too slow, we should implement an asynchronous file deleter.

        with self.lease_folder(local_series_folder=local_series_folder):
            path: str = self.get_local_path(uuid=uuid,
                                            local_series_folder=local_series_folder,
                                            content_type=content_type)

            # os.path.exists() + os.remove() is not atomic: eviction can rmtree
            # the parent folder between the two calls. Catch FileNotFoundError
            # rather than letting it surface as a storage callback error.
            try:
                os.remove(path)
                existed: bool = True
                self._release_deleted_file_bytes(file_size)
            except FileNotFoundError:
                existed = False

            logger.debug("remove called",
                        uuid=uuid,
                        path=path,
                        existed=existed)

            self._discard_folder_if_empty(local_series_folder)

    def _discard_folder_if_empty(self, local_series_folder: str) -> None:
        """Drop a series folder once its last instance file is gone.

        Deleting a study leaves the folder behind, and an empty directory is
        not free: ``du -b`` charges it a block, so the cache never reports
        zero usage again, and the folder counts as "not yet on S3" forever
        (it has no marker) in every stats snapshot and eviction pass. One husk
        per deleted series adds up on a long-lived pod.

        Best effort throughout. ``rmdir`` refuses a non-empty directory, which
        is exactly the guard we want against removing a folder that has
        already been repopulated; ``_write_file`` re-creates the folder (and
        retries once) if a concurrent create loses the race.
        """
        folder_path: str = self.get_folder_path(local_series_folder)
        try:
            remaining = os.listdir(folder_path)
        except OSError:
            return

        # A folder that only holds its own marker is just as much a husk.
        if any(name != S3_UPLOADED_MARKER_NAME
               and not name.startswith(S3_UPLOADED_MARKER_TMP_PREFIX)
               for name in remaining):
            return

        for name in remaining:
            try:
                os.remove(os.path.join(folder_path, name))
            except OSError:
                return

        try:
            os.rmdir(folder_path)
            logger.debug("LocalStorage: removed empty series folder",
                         local_series_folder=local_series_folder)
        except OSError as e:
            logger.debug("LocalStorage: could not remove empty series folder",
                         local_series_folder=local_series_folder,
                         error=str(e))


    def get_local_path(self, uuid: str, local_series_folder: str, content_type: orthanc.ContentType) -> str:

        if content_type not in self.SUPPORTED_CONTENT_TYPES:
            raise RuntimeError(f"Unsupported content type: {content_type}")

        return os.path.join(self._root, os.path.join(local_series_folder, uuid))

    def get_folder_path(self, local_series_folder: str) -> str:
        """Returns the full path for a series folder."""
        return os.path.join(self._root, local_series_folder)

    def has_local_file(self, uuid: str, local_series_folder: str, content_type: orthanc.ContentType) -> bool:
        path: str = self.get_local_path(uuid=uuid,
                                        local_series_folder=local_series_folder,
                                        content_type=content_type)
        exists: bool = os.path.exists(path)

        logger.debug("has_local_file check",
                     uuid=uuid,
                     local_series_folder=local_series_folder,
                     path=path,
                     exists=exists)
        return exists
