import orthanc
import json
import zipfile
import os
import random
import tempfile
import threading
import time
from contextlib import ExitStack
from typing import List, Dict, Optional, Tuple
from boto3 import client as S3Client
from local_storage_interface import LocalStorageInterface
from uncommitted_series_handler import UncommittedSeriesHandler
from custom_data import CustomData
from s3zip_logging import get_logger

try:
    from botocore import exceptions as botocore_exceptions
except ImportError:
    botocore_exceptions = None

logger = get_logger(__name__)

DEFAULT_S3_RETRIEVAL_MAX_ATTEMPTS = 3
DEFAULT_S3_RETRIEVAL_RETRY_BASE_DELAY_SECONDS = 0.5
DEFAULT_S3_RETRIEVAL_RETRY_MAX_DELAY_SECONDS = 5.0

_TRANSIENT_CLIENT_ERROR_CODES = {
    "InternalError",
    "InternalFailure",
    "RequestTimeout",
    "RequestTimeoutException",
    "ServiceUnavailable",
    "SlowDown",
    "ThrottledException",
    "Throttling",
    "ThrottlingException",
    "TooManyRequestsException",
}

_PERMANENT_CLIENT_ERROR_CODES = {
    "AccessDenied",
    "InvalidAccessKeyId",
    "InvalidObjectState",
    "NoSuchBucket",
    "NoSuchKey",
    "SignatureDoesNotMatch",
}

_TRANSIENT_HTTP_STATUS_CODES = {408, 429, 500, 502, 503, 504}

if botocore_exceptions is not None:
    ClientError = getattr(botocore_exceptions, "ClientError", None)
    _TRANSIENT_BOTOCORE_EXCEPTIONS = tuple(
        getattr(botocore_exceptions, name)
        for name in (
            "ConnectionClosedError",
            "ConnectTimeoutError",
            "EndpointConnectionError",
            "ProxyConnectionError",
            "ReadTimeoutError",
        )
        if hasattr(botocore_exceptions, name)
    )
else:
    ClientError = None
    _TRANSIENT_BOTOCORE_EXCEPTIONS = ()


class SeriesS3Info:

    series_id: str
    is_stored_in_s3: bool = False
    s3_zip_key: str = None

    def __init__(self, series_id: str):
        self.series_id = series_id


# This class is in charge of compressing and moving series between the local storage
# and S3.
class LocalToS3ZipManager:

    # This class is only used to make sure we do not download twice the same series at the
    # same time.  The ZipRetrieval is destructed at the end of the download phase once the
    # files are stored in the local storage -> the files are not locked in the local storage
    # but they are referenced in a LRU (TODO).
    class ZipRetrieval:

        series_id: str
        _condition: threading.Condition
        _ref_count: int
        _downloaded: bool
        _failed_exception: Optional[BaseException]

        def __init__(self, series_id: str):
            self.series_id = series_id
            self._condition = threading.Condition()
            self._ref_count = 0
            self._downloaded = False
            self._failed_exception = None
            logger.debug("ZipRetrieval created", series_id=series_id)

        def __enter__(self):
            logger.debug("ZipRetrieval entering (acquiring condition)",
                         series_id=self.series_id,
                         ref_count=self._ref_count)
            self._condition.__enter__()
            logger.debug("ZipRetrieval entered (condition acquired)",
                         series_id=self.series_id,
                         ref_count=self._ref_count)
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            logger.debug("ZipRetrieval exiting",
                         series_id=self.series_id,
                         ref_count=self._ref_count)
            self._condition.__exit__(exc_type, exc_val, exc_tb)
            logger.debug("ZipRetrieval exited",
                         series_id=self.series_id,
                         ref_count=self._ref_count)

        @property
        def downloaded(self):
            return self._downloaded

        @property
        def failed_exception(self):
            return self._failed_exception

        def set_downloaded(self):
            logger.debug("ZipRetrieval set_downloaded, notifying waiters",
                         series_id=self.series_id)
            self._downloaded = True
            self._condition.notify_all()

        def set_failed(self, exc: BaseException):
            logger.debug("ZipRetrieval set_failed, notifying waiters",
                         series_id=self.series_id,
                         error_type=type(exc).__name__,
                         error=str(exc))
            self._failed_exception = exc
            self._condition.notify_all()

        def raise_if_failed(self):
            if self._failed_exception is not None:
                raise self._failed_exception

        def wait_downloaded(self):
            logger.debug("ZipRetrieval waiting for download to complete",
                         series_id=self.series_id)
            while not self._downloaded and self._failed_exception is None:
                self._condition.wait()
            self.raise_if_failed()
            logger.debug("ZipRetrieval download wait completed",
                         series_id=self.series_id)

    _s3_client: S3Client
    _local_storage: LocalStorageInterface
    _uncommitted_series_handler: UncommittedSeriesHandler
    _bucket_name: str
    _s3_zip_retrievals: Dict[str, ZipRetrieval]
    _s3_zip_retrievals_lock: threading.Lock
    _copy_thread: threading.Thread
    _threads_should_stop: bool
    _zip_compression: int
    _s3_retrieval_max_attempts: int
    _s3_retrieval_retry_base_delay_sec: float
    _s3_retrieval_retry_max_delay_sec: float

    def __init__(self,
                 s3_client: S3Client,
                 bucket_name: str,
                 local_storage: LocalStorageInterface,
                 enable_compression: bool,
                 uncommitted_series_handler: UncommittedSeriesHandler,
                 key_prefix: str = "",
                 s3_retrieval_max_attempts: int = DEFAULT_S3_RETRIEVAL_MAX_ATTEMPTS,
                 s3_retrieval_retry_base_delay_sec: float = DEFAULT_S3_RETRIEVAL_RETRY_BASE_DELAY_SECONDS,
                 s3_retrieval_retry_max_delay_sec: float = DEFAULT_S3_RETRIEVAL_RETRY_MAX_DELAY_SECONDS):
        self._s3_client = s3_client
        self._bucket_name = bucket_name
        self._local_storage = local_storage
        self._uncommitted_series_handler = uncommitted_series_handler
        self._key_prefix = key_prefix.strip('/')
        self._s3_retrieval_max_attempts = max(1, int(s3_retrieval_max_attempts))
        self._s3_retrieval_retry_base_delay_sec = max(0.0, float(s3_retrieval_retry_base_delay_sec))
        self._s3_retrieval_retry_max_delay_sec = max(0.0, float(s3_retrieval_retry_max_delay_sec))
        if enable_compression:
            self._zip_compression = zipfile.ZIP_DEFLATED
        else:
            self._zip_compression = zipfile.ZIP_STORED
        self._s3_zip_retrievals = {}
        self._s3_zip_retrievals_lock = threading.Lock()
        self._threads_should_stop = False
        self._copy_thread = threading.Thread(target=self._copy_thread_worker)

        compression_name = "ZIP_DEFLATED" if enable_compression else "ZIP_STORED"
        logger.debug("LocalToS3ZipManager initialized",
                     bucket=bucket_name,
                     compression=compression_name,
                     key_prefix=self._key_prefix or "<none>",
                     s3_retrieval_max_attempts=self._s3_retrieval_max_attempts,
                     s3_retrieval_retry_base_delay_sec=self._s3_retrieval_retry_base_delay_sec,
                     s3_retrieval_retry_max_delay_sec=self._s3_retrieval_retry_max_delay_sec)


    def start(self):
        logger.info("S3 copy thread starting")
        self._copy_thread.start()


    def stop(self):
        logger.info("S3 copy thread stopping")
        self._threads_should_stop = True
        self._copy_thread.join()
        logger.info("S3 copy thread stopped")


    def _get_series_s3_key(self, series_id: str) -> str:
        if self._key_prefix:
            return f"{self._key_prefix}/{series_id}.zip"
        return f"{series_id}.zip"


    def schedule_copy_series_to_s3(self, series_id: str):
        logger.debug("enqueuing series for S3 copy", series_id=series_id)
        logger.debug("calling orthanc.EnqueueValue()", series_id=series_id)
        orthanc.EnqueueValue("series-to-copy", series_id.encode('utf-8'))
        logger.debug("orthanc.EnqueueValue() returned", series_id=series_id)
        logger.debug("series enqueued for S3 copy", series_id=series_id)


    def _copy_thread_worker(self):
        orthanc.SetCurrentThreadName("S3-COPY-THREAD")
        logger.info("S3 copy thread started")

        while not self._threads_should_stop:
            logger.debug("calling orthanc.ReserveQueueValue(series-to-copy)")
            bseries_id, value_id = orthanc.ReserveQueueValue("series-to-copy", orthanc.QueueOrigin.FRONT, 600)
            logger.debug("orthanc.ReserveQueueValue() returned",
                         got_item=bseries_id is not None,
                         value_id=str(value_id) if value_id is not None else "<none>")

            if bseries_id is None:
                logger.debug("no series in copy queue, sleeping")
                time.sleep(1)
            else:
                series_id = bseries_id.decode('utf-8')
                logger.debug("dequeued series for S3 copy",
                             series_id=series_id,
                             value_id=str(value_id))
                logger.info("starting copy_series_to_s3", series_id=series_id)
                try:
                    self.copy_series_to_s3(series_id=series_id)
                except Exception as e:
                    logger.warning("failed to copy series to S3, re-enqueuing",
                                   series_id=series_id,
                                   error=str(e))
                    # TODO: identify if this is a "permanent failure".  In this case, no need to repost the message + handle max retries
                    logger.debug("re-enqueuing failed series via orthanc.EnqueueValue()", series_id=series_id)
                    orthanc.EnqueueValue("series-to-copy", bseries_id)
                    logger.debug("orthanc.EnqueueValue() returned after re-enqueue", series_id=series_id)

                logger.debug("calling orthanc.AcknowledgeQueueValue()", series_id=series_id, value_id=str(value_id))
                orthanc.AcknowledgeQueueValue("series-to-copy", value_id)
                logger.debug("orthanc.AcknowledgeQueueValue() returned", series_id=series_id)
                logger.info("copy_series_to_s3 cycle complete", series_id=series_id)

        logger.info("S3 copy thread exiting")


    def copy_series_to_s3(self, series_id: str):
        logger.info("series copy to S3 starting", series_id=series_id)
        t0 = time.monotonic()

        # list all instances attachments
        attachments_uuids = self._get_instances_attachments(series_id=series_id)
        local_series_folder = None

        logger.debug("collected instance attachments for series",
                     series_id=series_id,
                     attachment_count=len(attachments_uuids))

        total_uncompressed_bytes = 0

        # let's zip them in a temp file and upload it to S3.
        with tempfile.NamedTemporaryFile(delete=True, suffix=".zip") as tmp_zip:
            logger.debug("building zip archive",
                         series_id=series_id,
                         tmp_path=tmp_zip.name,
                         attachment_count=len(attachments_uuids))

            with ExitStack() as local_folder_lease:
                with zipfile.ZipFile(tmp_zip.name, "w", compression=self._zip_compression) as zipf:
                    for idx, a_uuid in enumerate(attachments_uuids):
                        if not local_series_folder: # they all share the same folder
                            local_series_folder = CustomData.from_orthanc_attachment(a_uuid).local_series_folder
                            local_folder_lease.enter_context(self._local_storage.lease_folder(local_series_folder))
                            logger.debug("resolved local_series_folder from first attachment",
                                         series_id=series_id,
                                         local_series_folder=local_series_folder)
                        content = self._local_storage.read_file(uuid=a_uuid,
                                                                local_series_folder=local_series_folder)
                        total_uncompressed_bytes += len(content)
                        logger.debug("adding attachment to zip",
                                     series_id=series_id,
                                     uuid=a_uuid,
                                     index=idx,
                                     size_bytes=len(content))
                        zipf.writestr(a_uuid, content)
                        logger.debug("attachment added to zip",
                                     series_id=series_id,
                                     uuid=a_uuid,
                                     index=idx)

                t_zip_done = time.monotonic()
                zip_size_bytes = os.path.getsize(tmp_zip.name)

                logger.info("zip archive built",
                            series_id=series_id,
                            attachment_count=len(attachments_uuids),
                            zip_size_bytes=zip_size_bytes,
                            uncompressed_bytes=total_uncompressed_bytes,
                            zip_build_ms=int((t_zip_done - t0) * 1000))

                # Upload to S3
                s3_key = self._get_series_s3_key(series_id)
                logger.info("uploading zip to S3",
                            series_id=series_id,
                            s3_key=s3_key,
                            bucket=self._bucket_name,
                            zip_size_bytes=zip_size_bytes,
                            uncompressed_bytes=total_uncompressed_bytes)
                logger.debug("calling s3_client.upload_file()",
                             series_id=series_id,
                             s3_key=s3_key,
                             bucket=self._bucket_name)

                self._s3_client.upload_file(tmp_zip.name, self._bucket_name, s3_key)

                t_upload_done = time.monotonic()
                logger.debug("s3_client.upload_file() returned",
                             series_id=series_id,
                             s3_key=s3_key)
                logger.info("zip uploaded to S3",
                            series_id=series_id,
                            s3_key=s3_key,
                            bucket=self._bucket_name,
                            zip_size_bytes=zip_size_bytes,
                            upload_ms=int((t_upload_done - t_zip_done) * 1000))

                # Update the custom data to notify that the file is now stored in a zip in S3
                s3_custom_data = CustomData(storage=CustomData.Storage.S3_ZIP,
                                            local_series_folder=local_series_folder,
                                            s3_zip_key=s3_key).to_binary()

                logger.info("starting SetAttachmentCustomData loop",
                            series_id=series_id,
                            attachment_count=len(attachments_uuids),
                            s3_key=s3_key)
                t_meta_start = time.monotonic()

                for idx, a_uuid in enumerate(attachments_uuids):
                    logger.debug("calling orthanc.SetAttachmentCustomData()",
                                 series_id=series_id,
                                 uuid=a_uuid,
                                 index=idx,
                                 total=len(attachments_uuids))
                    orthanc.SetAttachmentCustomData(a_uuid, s3_custom_data)
                    logger.debug("orthanc.SetAttachmentCustomData() returned",
                                 series_id=series_id,
                                 uuid=a_uuid,
                                 index=idx)

                t_meta_done = time.monotonic()
                logger.info("SetAttachmentCustomData loop complete",
                            series_id=series_id,
                            attachment_count=len(attachments_uuids),
                            s3_key=s3_key,
                            metadata_update_ms=int((t_meta_done - t_meta_start) * 1000))

                # Re-check the attachment set: if a new instance landed for
                # this series after the initial snapshot, the uploaded zip is
                # already incomplete. Skip the marker so eviction cannot purge
                # the folder. The next stable-series event will trigger another
                # copy that includes the new instance(s) and publishes a fresh
                # marker.
                current_attachments: list[str] = self._get_instances_attachments(series_id=series_id)
                attachments_changed: bool = set(current_attachments) != set(attachments_uuids)

                # The marker is the eviction guard's durable signal that the
                # folder contents are recoverable from S3. It is written while
                # the folder lease is active so eviction cannot remove the
                # directory between opening the marker and atomically publishing
                # its final path.
                if local_series_folder and not attachments_changed:
                    self._write_s3_uploaded_marker(
                        local_series_folder=local_series_folder,
                        s3_key=s3_key,
                        series_id=series_id,
                    )
                elif local_series_folder and attachments_changed:
                    new_uuids = sorted(
                        set(current_attachments) - set(attachments_uuids)
                    )
                    dropped_uuids = sorted(
                        set(attachments_uuids) - set(current_attachments)
                    )
                    logger.warning(
                        msg="attachment set changed during S3 copy; skipping marker write (next stable-series event will trigger a fresh copy)",
                        series_id=series_id,
                        s3_key=s3_key,
                        snapshot_count=len(attachments_uuids),
                        current_count=len(current_attachments),
                        new_uuids=new_uuids,
                        dropped_uuids=dropped_uuids,
                    )

        duration_ms = int((time.monotonic() - t0) * 1000)

        self._uncommitted_series_handler.on_committed_series(series_id=series_id)

        logger.info("series stored to S3",
                    series_id=series_id,
                    s3_key=s3_key,
                    bucket=self._bucket_name,
                    attachment_count=len(attachments_uuids),
                    zip_size_bytes=zip_size_bytes,
                    uncompressed_bytes=total_uncompressed_bytes,
                    zip_build_ms=int((t_zip_done - t0) * 1000),
                    upload_ms=int((t_upload_done - t_zip_done) * 1000),
                    metadata_update_ms=int((t_meta_done - t_meta_start) * 1000),
                    duration_ms=duration_ms)

    def _write_s3_uploaded_marker(self, local_series_folder: str, s3_key: str, series_id: str):
        folder_path = self._local_storage.get_folder_path(local_series_folder)
        marker_path = os.path.join(folder_path, ".s3-uploaded")
        tmp_marker_path = os.path.join(
            folder_path,
            f".s3-uploaded.tmp-{os.getpid()}-{threading.get_ident()}"
        )

        try:
            os.makedirs(folder_path, exist_ok=True)
            with open(tmp_marker_path, "w") as f:
                f.write(s3_key)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_marker_path, marker_path)
            self._fsync_directory_if_supported(folder_path)
            logger.debug("wrote S3 upload marker file",
                         series_id=series_id,
                         marker_path=marker_path,
                         s3_key=s3_key)
        except Exception as e:
            try:
                if os.path.exists(tmp_marker_path):
                    os.remove(tmp_marker_path)
            except Exception as cleanup_error:
                logger.warning("failed to remove temporary S3 upload marker",
                               series_id=series_id,
                               tmp_marker_path=tmp_marker_path,
                               error=str(cleanup_error))
            logger.warning("failed to write S3 upload marker file",
                           series_id=series_id,
                           marker_path=marker_path,
                           error=str(e))

    def invalidate_s3_uploaded_marker(self, local_series_folder: str) -> bool:
        """Remove the ``.s3-uploaded`` marker for ``local_series_folder``.

        Called on every storage_create for a series: any new instance landing
        on disk invalidates the marker's invariant ("everything in this folder
        is recoverable from S3"). Best effort -- a missing marker is the
        expected steady state.
        """
        folder_path = self._local_storage.get_folder_path(local_series_folder)
        marker_path = os.path.join(folder_path, ".s3-uploaded")
        try:
            os.remove(marker_path)
            logger.debug("invalidated S3 upload marker",
                         local_series_folder=local_series_folder,
                         marker_path=marker_path)
            return True
        except FileNotFoundError:
            return False
        except OSError as e:
            logger.warning("failed to invalidate S3 upload marker",
                           local_series_folder=local_series_folder,
                           marker_path=marker_path,
                           error=str(e))
            return False

    def _fsync_directory_if_supported(self, folder_path: str):
        if not hasattr(os, "O_DIRECTORY"):
            return

        directory_fd = None
        try:
            directory_fd = os.open(folder_path, os.O_RDONLY | os.O_DIRECTORY)
            os.fsync(directory_fd)
        except OSError as e:
            logger.debug("directory fsync after marker write failed",
                         folder_path=folder_path,
                         error=str(e))
        finally:
            if directory_fd is not None:
                os.close(directory_fd)

    def _acquire_zip_retrieval(self, s3_zip_key: str) -> Tuple[ZipRetrieval, bool]:
        """Return the active retrieval object with a counted live reference.

        Lookup/create and refcount increment share the same lock. A thread must
        not leave this method with an uncounted object: if another thread
        completes the retrieval before this caller enters the condition, the
        active dictionary entry still has to stay alive for this caller.
        """
        with self._s3_zip_retrievals_lock:
            is_new_retrieval = False
            if s3_zip_key not in self._s3_zip_retrievals:
                self._s3_zip_retrievals[s3_zip_key] = LocalToS3ZipManager.ZipRetrieval(s3_zip_key)
                is_new_retrieval = True
            zip_retrieval = self._s3_zip_retrievals[s3_zip_key]
            zip_retrieval._ref_count += 1
            logger.debug("acquired ZipRetrieval",
                         s3_zip_key=s3_zip_key,
                         ref_count=zip_retrieval._ref_count,
                         is_new_retrieval=is_new_retrieval)
            return zip_retrieval, is_new_retrieval


    def _release_zip_retrieval(self, zip_retrieval: ZipRetrieval):
        """Release a counted retrieval reference and discard it when idle."""
        with self._s3_zip_retrievals_lock:
            zip_retrieval._ref_count -= 1
            logger.debug("released ZipRetrieval",
                         s3_zip_key=zip_retrieval.series_id,
                         ref_count=zip_retrieval._ref_count)
            if zip_retrieval._ref_count == 0:
                if self._s3_zip_retrievals.get(zip_retrieval.series_id) is zip_retrieval:
                    del self._s3_zip_retrievals[zip_retrieval.series_id]
                    logger.debug("discarded ZipRetrieval", s3_zip_key=zip_retrieval.series_id)
                else:
                    logger.warning("ZipRetrieval release found a different active retrieval",
                                   s3_zip_key=zip_retrieval.series_id)


    def get_s3_zip_stream(self, series_id: str):  # returns a stream
        logger.info("series zip stream from S3",
                    series_id=series_id)

        s3_zip_key = self._get_series_s3_key(series_id=series_id)

        response =  self._s3_client.get_object(Bucket=self._bucket_name,
                                               Key=s3_zip_key)
        return response['Body']


    def retrieve_zip_from_s3(self, s3_zip_key: str, local_series_folder: str):
        # make sure we do not retrieve the same file multiple times at the same time
        zip_retrieval, is_new_retrieval = self._acquire_zip_retrieval(s3_zip_key)

        logger.debug("retrieve_zip_from_s3 entered",
                     s3_zip_key=s3_zip_key,
                     local_series_folder=local_series_folder,
                     is_new_retrieval=is_new_retrieval)

        try:
            # The zip is extracted as several file writes. A folder may already
            # contain an S3 marker from an earlier upload, so eviction must skip
            # it until the extraction and all waiters have left this retrieval.
            with self._local_storage.lease_folder(local_series_folder):
                with zip_retrieval: # the first thread to get here keeps the condition "locked" during the zip retrieval
                    zip_retrieval.raise_if_failed()
                    if not zip_retrieval.downloaded:
                        logger.debug("this thread will perform the S3 download",
                                     s3_zip_key=s3_zip_key)
                        try:
                            self._retrieve_zip_from_s3(s3_zip_key, local_series_folder)
                        except Exception as e:
                            zip_retrieval.set_failed(e)
                            raise
                        else:
                            zip_retrieval.set_downloaded()
                    else:
                        logger.debug("another thread already downloaded this zip, waiting",
                                     s3_zip_key=s3_zip_key)
                        zip_retrieval.wait_downloaded()
        finally:
            self._release_zip_retrieval(zip_retrieval)


    def _retrieve_zip_from_s3(self, s3_zip_key: str, local_series_folder: str):
        started_at = time.monotonic()
        for attempt in range(1, self._s3_retrieval_max_attempts + 1):
            try:
                return self._retrieve_zip_from_s3_once(
                    s3_zip_key=s3_zip_key,
                    local_series_folder=local_series_folder,
                    attempt=attempt,
                )
            except Exception as e:
                retryable = self._is_retryable_s3_retrieval_exception(e)
                is_last_attempt = attempt >= self._s3_retrieval_max_attempts
                if not retryable or is_last_attempt:
                    logger.error(
                        "series retrieval from S3 failed",
                        s3_zip_key=s3_zip_key,
                        bucket=self._bucket_name,
                        local_series_folder=local_series_folder,
                        attempt=attempt,
                        max_attempts=self._s3_retrieval_max_attempts,
                        retryable=retryable,
                        error_type=type(e).__name__,
                        error=str(e),
                        elapsed_ms=int((time.monotonic() - started_at) * 1000),
                    )
                    raise

                delay_sec = self._get_s3_retrieval_retry_delay_sec(attempt)
                logger.warning(
                    "series retrieval from S3 failed, retrying",
                    s3_zip_key=s3_zip_key,
                    bucket=self._bucket_name,
                    local_series_folder=local_series_folder,
                    attempt=attempt,
                    max_attempts=self._s3_retrieval_max_attempts,
                    retry_delay_ms=int(delay_sec * 1000),
                    error_type=type(e).__name__,
                    error=str(e),
                )
                if delay_sec > 0:
                    time.sleep(delay_sec)

    def _retrieve_zip_from_s3_once(self, s3_zip_key: str, local_series_folder: str, attempt: int):
        logger.info("series retrieval from S3 starting",
                    s3_zip_key=s3_zip_key,
                    bucket=self._bucket_name,
                    local_series_folder=local_series_folder,
                    attempt=attempt,
                    max_attempts=self._s3_retrieval_max_attempts)
        t0 = time.monotonic()

        file_count = 0
        total_bytes = 0

        with tempfile.NamedTemporaryFile(delete=True, suffix=".zip") as tmp_zip:
            logger.debug("downloading zip from S3",
                         s3_zip_key=s3_zip_key,
                         bucket=self._bucket_name,
                         tmp_path=tmp_zip.name)
            logger.debug("calling s3_client.download_file()",
                         s3_zip_key=s3_zip_key,
                         bucket=self._bucket_name,
                         tmp_path=tmp_zip.name)

            self._s3_client.download_file(self._bucket_name,
                                          s3_zip_key,
                                          tmp_zip.name)

            t_download_done = time.monotonic()
            zip_size_bytes = os.path.getsize(tmp_zip.name)
            logger.debug("s3_client.download_file() returned",
                         s3_zip_key=s3_zip_key,
                         zip_size_bytes=zip_size_bytes)
            logger.info("zip downloaded from S3",
                        s3_zip_key=s3_zip_key,
                        bucket=self._bucket_name,
                        zip_size_bytes=zip_size_bytes,
                        download_ms=int((t_download_done - t0) * 1000))

            logger.debug("extracting zip to local storage",
                         s3_zip_key=s3_zip_key,
                         local_series_folder=local_series_folder)

            with zipfile.ZipFile(tmp_zip.name, 'r') as zipf:
                for file_info in zipf.infolist():
                    with zipf.open(file_info) as f:
                        content = f.read()
                        self._local_storage.write_file(uuid=file_info.filename,
                                                       local_series_folder=local_series_folder,
                                                       content=content)
                        file_count += 1
                        total_bytes += len(content)
                        logger.debug("extracted file from zip to local storage",
                                     s3_zip_key=s3_zip_key,
                                     uuid=file_info.filename,
                                     size_bytes=len(content),
                                     index=file_count)

        duration_ms = int((time.monotonic() - t0) * 1000)

        logger.info("series retrieved from S3",
                    s3_zip_key=s3_zip_key,
                    bucket=self._bucket_name,
                    local_series_folder=local_series_folder,
                    attempt=attempt,
                    file_count=file_count,
                    zip_size_bytes=zip_size_bytes,
                    uncompressed_bytes=total_bytes,
                    download_ms=int((t_download_done - t0) * 1000),
                    duration_ms=duration_ms)

    def _get_s3_retrieval_retry_delay_sec(self, failed_attempt: int) -> float:
        if self._s3_retrieval_retry_base_delay_sec <= 0:
            return 0.0

        exponential_delay = self._s3_retrieval_retry_base_delay_sec * (2 ** max(0, failed_attempt - 1))
        capped_delay = min(exponential_delay, self._s3_retrieval_retry_max_delay_sec)
        return random.uniform(0.0, capped_delay)

    def _is_retryable_s3_retrieval_exception(self, exc: BaseException) -> bool:
        if isinstance(exc, zipfile.BadZipFile):
            return False

        if ClientError is not None and isinstance(exc, ClientError):
            response = getattr(exc, "response", {}) or {}
            error = response.get("Error", {}) or {}
            metadata = response.get("ResponseMetadata", {}) or {}
            error_code = error.get("Code")
            http_status_code = metadata.get("HTTPStatusCode")
            if error_code in _PERMANENT_CLIENT_ERROR_CODES:
                return False
            if error_code in _TRANSIENT_CLIENT_ERROR_CODES:
                return True
            if http_status_code in _TRANSIENT_HTTP_STATUS_CODES:
                return True
            return False

        if _TRANSIENT_BOTOCORE_EXCEPTIONS and isinstance(exc, _TRANSIENT_BOTOCORE_EXCEPTIONS):
            return True

        if isinstance(exc, (ConnectionError, TimeoutError)):
            return True

        return False


    def _get_instances_attachments(self, series_id: str) -> List[str]:
        logger.info("querying Orthanc for series instance attachments", series_id=series_id)
        t0 = time.monotonic()

        payload = {
            "Level": "Instance",
            "Query": {},
            "ResponseContent": ["Attachments"],
            "ParentSeries": series_id
        }
        logger.debug("calling orthanc.RestApiPost(/tools/find)", series_id=series_id)
        response_raw = orthanc.RestApiPost("/tools/find", json.dumps(payload).encode('utf-8'))
        logger.debug("orthanc.RestApiPost(/tools/find) returned",
                     series_id=series_id,
                     response_bytes=len(response_raw))

        instances_info = json.loads(response_raw)
        supported_content_types = {
            1,  # ContentType.DICOM
            3,  # ContentType.DICOM_UNTIL_PIXEL_DATA
        }
        attachments_uuids = []
        for i in instances_info:
            for attachment in i["Attachments"]:
                if attachment["ContentType"] in supported_content_types:
                    attachments_uuids.append(attachment["Uuid"])

        duration_ms = int((time.monotonic() - t0) * 1000)
        logger.info("Orthanc returned instance attachments",
                    series_id=series_id,
                    instance_count=len(instances_info),
                    attachment_count=len(attachments_uuids),
                    query_ms=duration_ms)

        return attachments_uuids

    def get_series_info(self, series_id: str) -> Optional[SeriesS3Info]:
        attachments_uuids = self._get_instances_attachments(series_id=series_id)

        if len(attachments_uuids) == 0:
            return None

        status = SeriesS3Info(series_id=series_id)

        # get the custom data of a random attachment (the first one)
        cd = CustomData.from_orthanc_attachment(attachment_uuid=attachments_uuids[0])
        if cd:
            status.is_stored_in_s3 = cd.storage == CustomData.Storage.S3_ZIP
            if status.is_stored_in_s3:
                status.s3_zip_key = cd.s3_zip_key

        return status
