import orthanc
import os
import json
from helpers import Helpers
from typing import Optional, Tuple
from boto3 import client as S3Client
from local_storage import LocalStorage
from local_to_s3_zip_manager import (
    DEFAULT_S3_RETRIEVAL_MAX_ATTEMPTS,
    DEFAULT_S3_RETRIEVAL_RETRY_BASE_DELAY_SECONDS,
    DEFAULT_S3_RETRIEVAL_RETRY_MAX_DELAY_SECONDS,
    LocalToS3ZipManager,
    SeriesS3Info,
)
from uncommitted_series_handler import UncommittedSeriesHandler
from custom_data import CustomData
from s3zip_logging import get_logger

logger = get_logger(__name__)


class S3ZipStorage:

    _local_storage: LocalStorage
    _zip_manager: LocalToS3ZipManager
    _uncommitted_series_handler: UncommittedSeriesHandler

    def __init__(self,
                 temporary_folder_root: str,
                 temp_folder_max_size_mb: int,
                 s3_client: S3Client,
                 bucket_name: str,
                 enable_compression: bool,
                 key_prefix: str = "",
                 s3_retrieval_max_attempts: int = DEFAULT_S3_RETRIEVAL_MAX_ATTEMPTS,
                 s3_retrieval_retry_base_delay_sec: float = DEFAULT_S3_RETRIEVAL_RETRY_BASE_DELAY_SECONDS,
                 s3_retrieval_retry_max_delay_sec: float = DEFAULT_S3_RETRIEVAL_RETRY_MAX_DELAY_SECONDS):
        logger.debug("initializing S3ZipStorage",
                     temp_folder=temporary_folder_root,
                     max_size_mb=temp_folder_max_size_mb,
                     bucket=bucket_name,
                     compression=enable_compression,
                     key_prefix=key_prefix or "<none>",
                     s3_retrieval_max_attempts=s3_retrieval_max_attempts,
                     s3_retrieval_retry_base_delay_sec=s3_retrieval_retry_base_delay_sec,
                     s3_retrieval_retry_max_delay_sec=s3_retrieval_retry_max_delay_sec)

        self._uncommitted_series_handler = UncommittedSeriesHandler()

        self._local_storage = LocalStorage(root=temporary_folder_root,
                                           max_size_mb=temp_folder_max_size_mb)

        self._zip_manager = LocalToS3ZipManager(s3_client=s3_client,
                                                bucket_name=bucket_name,
                                                local_storage=self._local_storage,
                                                enable_compression=enable_compression,
                                                uncommitted_series_handler=self._uncommitted_series_handler,
                                                key_prefix=key_prefix,
                                                s3_retrieval_max_attempts=s3_retrieval_max_attempts,
                                                s3_retrieval_retry_base_delay_sec=s3_retrieval_retry_base_delay_sec,
                                                s3_retrieval_retry_max_delay_sec=s3_retrieval_retry_max_delay_sec)

        # Set up the eviction guard: a folder is safe to evict only if it has the
        # .s3-uploaded marker file written by the copy thread after a successful S3 upload.
        def _is_folder_safe_to_evict(folder_name: str) -> bool:
            marker_path = os.path.join(self._local_storage.get_folder_path(folder_name), ".s3-uploaded")
            return os.path.exists(marker_path)

        self._local_storage.set_eviction_guard(_is_folder_safe_to_evict)

        logger.debug("S3ZipStorage initialized")

    def evict_local_cache(self) -> dict:
        """Force-evict every locally-cached series already on S3.

        Returns a JSON-friendly dict (folders evicted, bytes freed, folders
        skipped because not yet uploaded, available bytes after, max bytes).
        """
        result = self._local_storage.evict_all_safe()
        return {
            "freed_folders": result.freed_folders,
            "freed_bytes": result.freed_bytes,
            "skipped_folders": result.skipped_folders,
            "available_bytes": result.available_bytes_after,
            "max_bytes": self._local_storage._max_size,
        }

    def get_local_cache_stats(self) -> dict:
        """Return a snapshot of local-cache occupancy without triggering eviction."""
        return self._local_storage.get_cache_summary()

    def start(self):
        logger.debug("cleaning uncommitted series")
        self._uncommitted_series_handler.on_orthanc_started()

        logger.debug("starting S3ZipStorage manager")
        self._zip_manager.start()

    def stop(self):
        logger.debug("stopping S3ZipStorage manager")
        self._zip_manager.stop()

    def on_new_series(self, series_id: str):
        self._uncommitted_series_handler.on_new_series(series_id=series_id)

    def storage_create(self,
                       uuid: str,
                       content_type: orthanc.ContentType,
                       compression_type: orthanc.CompressionType,
                       content: bytes,
                       dicom_instance: orthanc.DicomInstance) -> Tuple[orthanc.ErrorCode, Optional[bytes]]:

        logger.debug("storage_create entered",
                     uuid=uuid,
                     content_type=str(content_type),
                     size_bytes=len(content))

        if content_type not in (orthanc.ContentType.DICOM, orthanc.ContentType.DICOM_UNTIL_PIXEL_DATA):
            logger.debug("storage_create: ignoring unsupported content type",
                         uuid=uuid,
                         content_type=str(content_type))
            return orthanc.ErrorCode.SUCCESS, None

        logger.debug("calling Helpers.get_series_hash()", uuid=uuid)
        series_hash = Helpers.get_series_hash(dicom_instance)
        logger.debug("Helpers.get_series_hash() returned", uuid=uuid, series_hash=series_hash)

        logger.debug("storage_create called",
                     uuid=uuid,
                     content_type=str(content_type),
                     compression_type=str(compression_type),
                     size_bytes=len(content),
                     series_hash=series_hash)

        # we always write only to the local storage
        logger.debug("calling local_storage.create()", uuid=uuid, series_hash=series_hash)
        error_code = self._local_storage.create(uuid=uuid,
                                                local_series_folder=series_hash,
                                                content_type=content_type,
                                                compression_type=compression_type,
                                                content=content)
        logger.debug("local_storage.create() returned", uuid=uuid, error_code=str(error_code))

        # Any new file invalidates "everything in this folder is on S3". Wipe
        # the marker so eviction cannot purge the folder before the next copy
        # captures this instance. The per-folder critical section pairs with
        # the matching one in copy_series_to_s3 around its recheck+marker
        # write: ordering between the two is now sequential, so a stale
        # marker cannot be published after this invalidate runs as a no-op.
        # Idempotent on a missing marker, so safe to call even when create()
        # failed.
        with self._local_storage.folder_marker_critical_section(series_hash):
            _ = self._zip_manager.invalidate_s3_uploaded_marker(local_series_folder=series_hash)

        custom_data = CustomData(CustomData.Storage.LOCAL, local_series_folder=series_hash)

        logger.debug("storage_create completed",
                     uuid=uuid,
                     series_hash=series_hash,
                     error_code=str(error_code),
                     storage=custom_data.storage.value)

        return error_code, custom_data.to_binary()


    def storage_read_range(self,
                           uuid: str,
                           content_type: orthanc.ContentType,
                           range_start: int,
                           size: int,
                           custom_data: bytes) -> Tuple[orthanc.ErrorCode, Optional[bytes]]:

        logger.debug("storage_read_range entered",
                     uuid=uuid,
                     content_type=str(content_type),
                     range_start=range_start,
                     size=size)

        cd: CustomData = CustomData.from_binary(custom_data)

        logger.debug("storage_read_range called",
                     uuid=uuid,
                     content_type=str(content_type),
                     range_start=range_start,
                     size=size,
                     storage=cd.storage.value,
                     local_series_folder=cd.local_series_folder,
                     s3_zip_key=cd.s3_zip_key or "<none>")

        # Eviction removes whole series folders. Keep the folder leased from the
        # local existence check through the final read so a safe-to-evict folder
        # cannot disappear between `has_local_file()` and `read_range()`.
        with self._local_storage.lease_folder(cd.local_series_folder):
            logger.debug("calling local_storage.has_local_file()", uuid=uuid)
            has_local = self._local_storage.has_local_file(uuid=uuid,
                                                           local_series_folder=cd.local_series_folder,
                                                           content_type=content_type)
            logger.debug("local_storage.has_local_file() returned", uuid=uuid, has_local=has_local)

            if not has_local:
                s3_zip_key = cd.s3_zip_key

                if s3_zip_key is None:
                    # Custom data can still point to local storage after a pod restart
                    # or cache eviction. A completed upload leaves a marker in the
                    # series folder so reads can recover the S3 key without querying
                    # every instance attachment again.
                    marker_path = os.path.join(
                        self._local_storage.get_folder_path(cd.local_series_folder),
                        ".s3-uploaded"
                    )
                    if os.path.exists(marker_path):
                        with open(marker_path, "r") as f:
                            s3_zip_key = f.read().strip()
                        logger.warning(
                            "instance marked as local but file is gone; recovered S3 key from marker",
                            uuid=uuid,
                            s3_zip_key=s3_zip_key,
                            local_series_folder=cd.local_series_folder)
                    else:
                        logger.error(
                            "instance marked as local but file is gone and no S3 key available. "
                            "DATA LOSS: this instance cannot be retrieved.",
                            uuid=uuid,
                            local_series_folder=cd.local_series_folder)
                        return orthanc.ErrorCode.UNKNOWN_RESOURCE, None

                logger.debug("instance not in local cache, retrieving series from S3",
                             uuid=uuid,
                             s3_zip_key=s3_zip_key,
                             local_series_folder=cd.local_series_folder)
                logger.debug("calling zip_manager.retrieve_zip_from_s3()", uuid=uuid, s3_zip_key=s3_zip_key)
                self._zip_manager.retrieve_zip_from_s3(s3_zip_key=s3_zip_key,
                                                       local_series_folder=cd.local_series_folder)
                logger.debug("zip_manager.retrieve_zip_from_s3() returned", uuid=uuid)
                logger.debug("series retrieved from S3 into local cache",
                             uuid=uuid,
                             s3_zip_key=s3_zip_key)
            else:
                logger.debug("instance found in local cache",
                             uuid=uuid,
                             local_series_folder=cd.local_series_folder)

            logger.debug("calling local_storage.read_range()", uuid=uuid, range_start=range_start, size=size)
            result = self._local_storage.read_range(uuid=uuid,
                                                    local_series_folder=cd.local_series_folder,
                                                    content_type=content_type,
                                                    range_start=range_start,
                                                    size=size)
            logger.debug("local_storage.read_range() returned", uuid=uuid)

        error_code, data = result
        logger.debug("storage_read_range completed",
                     uuid=uuid,
                     error_code=str(error_code),
                     bytes_read=len(data) if data else 0)

        return result

    def storage_remove(self,
                       uuid: str,
                       content_type: orthanc.ContentType,
                       custom_data: bytes) -> orthanc.ErrorCode:

        logger.debug("storage_remove entered", uuid=uuid, content_type=str(content_type))

        cd = CustomData.from_binary(custom_data)

        logger.debug("storage_remove called",
                     uuid=uuid,
                     content_type=str(content_type),
                     storage=cd.storage.value,
                     local_series_folder=cd.local_series_folder)

        # always delete from the local storage in case it has been stored there
        logger.debug("calling local_storage.remove()", uuid=uuid)
        self._local_storage.remove(uuid=uuid,
                                   local_series_folder=cd.local_series_folder,
                                   content_type=content_type)
        logger.debug("local_storage.remove() returned", uuid=uuid)

        # TODO: we should probably mark the file as deleted somehow but, how and when do we remove the zip in S3 ?

        logger.debug("storage_remove completed", uuid=uuid)

        return orthanc.ErrorCode.SUCCESS


    def schedule_copy_series_to_s3(self, series_id: str):
        logger.debug("scheduling series copy to S3", series_id=series_id)
        self._zip_manager.schedule_copy_series_to_s3(series_id=series_id)


    def get_series_status(self, series_id: str) -> Optional[SeriesS3Info]:
        logger.debug("retrieving series S3 status", series_id=series_id)
        return self._zip_manager.get_series_info(series_id=series_id)


    def get_s3_zip_stream(self, series_id: str):  # returns a stream
        logger.debug("retrieving a series S3 zip stream", series_id=series_id)
        return self._zip_manager.get_s3_zip_stream(series_id=series_id)
