import logging
import os
import orthanc
import sys
import threading
import subprocess
import queue
import shutil
from typing import Callable, Tuple, Optional
from local_storage_interface import LocalStorageInterface
from collections import deque
from s3zip_logging import get_logger

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
_module_level_override = os.environ.get(_LOCAL_STORAGE_LOG_LEVEL_ENV_VAR)
if _module_level_override:
    _lvl = getattr(logging, _module_level_override.upper(), None)
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
        _s3zip_root = logging.getLogger("s3zip")
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


class LocalStorage(LocalStorageInterface):


    _root: str
    _max_size: int    # all sizes are in [bytes]
    _available_size: int
    _block_size: int
    _lock: threading.RLock
    _folder_stats: queue.PriorityQueue
    _is_folder_safe_to_evict: Optional[Callable[[str], bool]]

    def __init__(self, root: str, max_size_mb: int):
        self._root = root
        self._max_size = max_size_mb * 1024 * 1024
        self._lock = threading.RLock()
        self._is_folder_safe_to_evict = None

        self._update_local_storage_stats()

        logger.debug("LocalStorage initialized",
                     root=root,
                     max_size_mb=max_size_mb,
                     max_size_bytes=self._max_size)

    def set_eviction_guard(self, is_folder_safe_to_evict: callable):
        """
        - Set a callback that determines if a local series folder is safe to evict.
        - The callback receives the folder name (not the full path) and
          must return True if the folder's data has been safely backed up to S3.
        - If this callback is not set, all folders are considered safe to evict.
        """
        self._is_folder_safe_to_evict = is_folder_safe_to_evict

    def _update_local_storage_stats(self):
        with self._lock:
            prev_available = getattr(self, '_available_size', None)
            self._available_size = self._max_size
            self._block_size = os.statvfs(self._root).f_frsize

            self._folder_stats = queue.PriorityQueue()

            cmd = ["du", "-b", "--max-depth=1", self._root]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            lines = result.stdout.strip().split("\n")

            total_folders = 0
            total_apparent_bytes = 0
            for line in lines:
                size_str, path = line.split("\t")
                folder_size = int(size_str)
                if path != self._root:
                    last_modified = os.path.getmtime(path)
                    self._available_size -= folder_size
                    self._folder_stats.put((last_modified, path, folder_size))
                    total_folders += 1
                    total_apparent_bytes += folder_size

            logger.debug(
                "LocalStorage: disk stats refreshed",
                max_size_mb=self._max_size // (1024 * 1024),
                max_size_bytes=self._max_size,
                block_size=self._block_size,
                total_folders=total_folders,
                total_apparent_bytes=total_apparent_bytes,
                total_apparent_mb=round(total_apparent_bytes / (1024 * 1024), 2),
                available_bytes=self._available_size,
                available_mb=round(self._available_size / (1024 * 1024), 2),
                prev_available_bytes=prev_available,
            )


    def _make_room(self, size: int):
        with self._lock:
            estimated_disk_size = ((size + self._block_size - 1) // self._block_size) * self._block_size

            logger.debug(
                "LocalStorage: _make_room called",
                requested_bytes=size,
                estimated_disk_bytes=estimated_disk_size,
                current_available_bytes=self._available_size,
                current_available_mb=round(self._available_size / (1024 * 1024), 2),
            )

            if estimated_disk_size < self._available_size:
                self._available_size -= estimated_disk_size
                logger.debug(
                    "LocalStorage: fast path - sufficient space, no eviction needed",
                    available_after_bytes=self._available_size,
                    available_after_mb=round(self._available_size / (1024 * 1024), 2),
                )
                return

            logger.debug(
                "LocalStorage: slow path - available space insufficient, refreshing disk stats",
                estimated_disk_bytes=estimated_disk_size,
                available_before_refresh_bytes=self._available_size,
            )
            self._update_local_storage_stats()

            logger.debug(
                "LocalStorage: disk stats refreshed, starting eviction loop",
                estimated_disk_bytes=estimated_disk_size,
                available_after_refresh_bytes=self._available_size,
                folders_queued=self._folder_stats.qsize(),
            )

            # Reclaim space -- evict oldest folders first, but protect folders
            # whose data has not yet been safely backed up to S3.
            # This can be useful if the plugin receives a burst of uploads that
            # exceed the local storage capacity, but the S3 upload process is
            # still catching up
            skipped = []
            freed_bytes = 0
            freed_folders = 0
            while estimated_disk_size > self._available_size and not self._folder_stats.empty():
                entry = self._folder_stats.get()
                _, path, folder_size = entry

                folder_name = os.path.basename(path)

                if self._is_folder_safe_to_evict is not None:
                    try:
                        safe = self._is_folder_safe_to_evict(folder_name)
                    except Exception as e:
                        logger.warning("eviction guard check failed, skipping folder",
                                       folder=folder_name, error=str(e))
                        safe = False

                    if not safe:
                        logger.info(
                            "LocalStorage: skipping eviction of folder not yet on S3",
                            folder=folder_name, folder_size=folder_size)
                        skipped.append(entry)
                        continue

                orthanc.LogInfo(f"LocalStorage: reclaiming space by deleting local folder '{path}'")
                logger.debug(
                    "LocalStorage: evicting folder",
                    folder=folder_name,
                    folder_size=folder_size,
                    available_before=self._available_size,
                )

                # TODO: handle errors here (e.g. if the file gets locked by
                # another process, or if it is being deleted by someone trying
                # to help! Unlikely but what can go wrong will go wrong...)
                shutil.rmtree(path)
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

            logger.debug(
                "LocalStorage: eviction loop complete",
                freed_folders=freed_folders,
                freed_bytes=freed_bytes,
                skipped_folders=len(skipped),
                estimated_disk_size=estimated_disk_size,
                available_after_eviction_bytes=self._available_size,
                available_after_eviction_mb=round(self._available_size / (1024 * 1024), 2),
            )

            if estimated_disk_size > self._available_size:
                logger.warning(
                    "LocalStorage: could not free enough space. "
                    "Some folders are protected because they are not yet on S3.",
                    needed=estimated_disk_size,
                    available=self._available_size,
                    available_mb=round(self._available_size / (1024 * 1024), 2),
                    protected_folders=len(skipped),
                    max_size_mb=self._max_size // (1024 * 1024),
                )


    def write_file(self, local_series_folder: str, uuid: str, content: bytes):
        self._make_room(len(content))

        self._write_file(uuid=uuid,
                         local_series_folder=local_series_folder,
                         content_type=orthanc.ContentType.DICOM,
                         content=content)


    def _write_file(self, uuid: str, local_series_folder: str, content_type: orthanc.ContentType, content: bytes):

        path = self.get_local_path(uuid=uuid,
                                   local_series_folder=local_series_folder,
                                   content_type=content_type)

        logger.debug("writing file to local storage",
                     uuid=uuid,
                     local_series_folder=local_series_folder,
                     path=path,
                     size_bytes=len(content))

        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, "wb") as f:
            f.write(content)

        # with self._lock:

        # TODO: increment used_size + LRU references

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

        path = self.get_local_path(uuid=uuid,
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
                data = f.read(size)
            else:
                data = f.read()

        logger.debug("file read from local storage",
                     uuid=uuid,
                     path=path,
                     bytes_read=len(data),
                     range_start=range_start)
        return data


    SUPPORTED_CONTENT_TYPES = (orthanc.ContentType.DICOM, orthanc.ContentType.DICOM_UNTIL_PIXEL_DATA)

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
            data = self._read_file(uuid=uuid,
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
               content_type: orthanc.ContentType):

        # TODO: we should probably implement an asynchronous file deleter

        path = self.get_local_path(uuid=uuid,
                                   local_series_folder=local_series_folder,
                                   content_type=content_type)

        existed = os.path.exists(path)

        if existed:
            os.remove(path)

        logger.debug("remove called",
                     uuid=uuid,
                     path=path,
                     existed=existed)


    def get_local_path(self, uuid: str, local_series_folder: str, content_type: orthanc.ContentType) -> str:

        if content_type not in self.SUPPORTED_CONTENT_TYPES:
            raise RuntimeError(f"Unsupported content type: {content_type}")

        return os.path.join(self._root, os.path.join(local_series_folder, uuid))

    def get_folder_path(self, local_series_folder: str) -> str:
        """Returns the full path for a series folder."""
        return os.path.join(self._root, local_series_folder)

    def has_local_file(self, uuid: str, local_series_folder: str, content_type: orthanc.ContentType) -> bool:
        path = self.get_local_path(uuid=uuid,
                                   local_series_folder=local_series_folder,
                                   content_type=content_type)
        exists = os.path.exists(path)

        logger.debug("has_local_file check",
                     uuid=uuid,
                     local_series_folder=local_series_folder,
                     path=path,
                     exists=exists)
        return exists
