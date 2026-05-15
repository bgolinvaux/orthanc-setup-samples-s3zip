import os
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
import zipfile
from contextlib import contextmanager
from pathlib import Path
from unittest import mock


PLUGIN_DIR = Path(__file__).resolve().parents[1] / "plugin"
sys.path.insert(0, str(PLUGIN_DIR))


class _ContentType:
    DICOM = 1
    DICOM_UNTIL_PIXEL_DATA = 3


class _ErrorCode:
    SUCCESS = "SUCCESS"
    UNKNOWN_RESOURCE = "UNKNOWN_RESOURCE"
    PLUGIN = "PLUGIN"


class _CompressionType:
    NONE = 0


class _DicomInstance:
    pass


orthanc_stub = types.SimpleNamespace(
    ContentType=_ContentType,
    ErrorCode=_ErrorCode,
    CompressionType=_CompressionType,
    DicomInstance=_DicomInstance,
    LogInfo=lambda message: None,
    SetCurrentThreadName=lambda name: None,
    SetAttachmentCustomData=lambda uuid, custom_data: None,
)
sys.modules.setdefault("orthanc", orthanc_stub)
sys.modules.setdefault("boto3", types.SimpleNamespace(client=object))


from custom_data import CustomData
from local_storage import LocalStorage
from local_to_s3_zip_manager import LocalToS3ZipManager
from s3_zip_storage import S3ZipStorage


def _fake_du_for(root: str, folder_name: str = "series", folder_size: int = 10):
    def fake_run(cmd, capture_output, text, check):
        folder = os.path.join(root, folder_name)
        total_size = folder_size if os.path.isdir(folder) else 0
        lines = []
        if os.path.isdir(folder):
            lines.append(f"{folder_size}\t{folder}")
        lines.append(f"{total_size}\t{root}")
        return subprocess.CompletedProcess(cmd, 0, stdout="\n".join(lines), stderr="")

    return fake_run


def _fake_du_walk(root: str):
    """Generic du substitute: walks ``root`` and sums real file sizes per child."""
    def fake_run(cmd, capture_output, text, check):
        lines = []
        total = 0
        if os.path.isdir(root):
            for entry in os.listdir(root):
                child = os.path.join(root, entry)
                if not os.path.isdir(child):
                    continue
                size = 0
                for dirpath, _dirnames, filenames in os.walk(child):
                    for fname in filenames:
                        try:
                            size += os.path.getsize(os.path.join(dirpath, fname))
                        except OSError:
                            pass
                lines.append(f"{size}\t{child}")
                total += size
        lines.append(f"{total}\t{root}")
        return subprocess.CompletedProcess(cmd, 0, stdout="\n".join(lines), stderr="")
    return fake_run


class FolderLeaseTests(unittest.TestCase):
    def test_leased_folder_is_skipped_by_eviction_then_evicted_after_release(self):
        with tempfile.TemporaryDirectory() as root:
            folder = os.path.join(root, "series")
            os.makedirs(folder)
            with open(os.path.join(folder, "instance"), "wb") as f:
                f.write(b"abc")
            with open(os.path.join(folder, ".s3-uploaded"), "w") as f:
                f.write("series.zip")

            with mock.patch("local_storage.subprocess.run", side_effect=_fake_du_for(root)):
                storage = LocalStorage(root=root, max_size_mb=1)
                storage.set_eviction_guard(lambda folder_name: True)

                with storage.lease_folder("series"):
                    result = storage.evict_all_safe()
                    self.assertEqual(result.freed_folders, 0)
                    self.assertEqual(result.skipped_folders, 1)
                    self.assertTrue(os.path.isdir(folder))

                result = storage.evict_all_safe()
                self.assertEqual(result.freed_folders, 1)
                self.assertFalse(os.path.exists(folder))

    def test_make_room_rolls_back_reservation_when_slow_path_fails(self):
        with tempfile.TemporaryDirectory() as root:
            with mock.patch("local_storage.subprocess.run", side_effect=_fake_du_for(root)):
                storage = LocalStorage(root=root, max_size_mb=0)

            storage._available_size = 0
            storage._reserved_bytes = 0

            with mock.patch.object(
                storage,
                "_update_local_storage_stats_with_writes_paused",
                side_effect=RuntimeError("scan failed"),
            ):
                with self.assertRaises(RuntimeError):
                    storage._make_room(10)

            self.assertEqual(storage._reserved_bytes, 0)
            self.assertEqual(storage._available_size, 0)
            self.assertFalse(storage._scan_in_progress)

    def test_eviction_keeps_folder_queued_when_delete_fails(self):
        with tempfile.TemporaryDirectory() as root:
            folder = os.path.join(root, "series")
            os.makedirs(folder)
            with open(os.path.join(folder, "instance"), "wb") as f:
                f.write(b"abc")

            with mock.patch("local_storage.subprocess.run", side_effect=_fake_du_for(root)):
                storage = LocalStorage(root=root, max_size_mb=1)
                storage.set_eviction_guard(lambda folder_name: True)

                with mock.patch("local_storage.shutil.rmtree", side_effect=OSError("locked")):
                    result = storage.evict_all_safe()

            self.assertEqual(result.freed_folders, 0)
            self.assertEqual(result.skipped_folders, 1)
            self.assertTrue(os.path.isdir(folder))

    def test_pause_writes_for_scan_clears_flag_when_wait_is_interrupted(self):
        # Regression: if `wait()` raises after `_scan_in_progress` is set, the
        # flag must be cleared and waiters notified. Otherwise every future
        # write/scan deadlocks at `_enter_write` / `_pause_writes_for_scan`.
        with tempfile.TemporaryDirectory() as root:
            with mock.patch("local_storage.subprocess.run", side_effect=_fake_du_for(root)):
                storage = LocalStorage(root=root, max_size_mb=1)

            # Make sure the active-writers wait loop is taken.
            storage._active_writers = 1

            with mock.patch.object(
                storage._io_condition,
                "wait",
                side_effect=KeyboardInterrupt("simulated signal"),
            ):
                with self.assertRaises(KeyboardInterrupt):
                    storage._pause_writes_for_scan()

            self.assertFalse(storage._scan_in_progress)

            # And a fresh scan can still acquire the slot afterwards.
            storage._active_writers = 0
            with mock.patch("local_storage.subprocess.run", side_effect=_fake_du_for(root)):
                storage._update_local_storage_stats()
            self.assertFalse(storage._scan_in_progress)

    def test_folder_marker_critical_section_serializes_same_folder(self):
        # Two threads asking for the SAME folder's CS must serialize. Two
        # threads asking for DIFFERENT folders must run in parallel.
        with tempfile.TemporaryDirectory() as root:
            with mock.patch("local_storage.subprocess.run", side_effect=_fake_du_for(root)):
                storage = LocalStorage(root=root, max_size_mb=1)

            inside_same = threading.Event()
            release_same = threading.Event()
            second_acquired_same = threading.Event()

            def first_same():
                with storage.folder_marker_critical_section("series"):
                    inside_same.set()
                    self.assertTrue(release_same.wait(timeout=2))

            def second_same():
                self.assertTrue(inside_same.wait(timeout=2))
                # Should block until release_same fires.
                with storage.folder_marker_critical_section("series"):
                    second_acquired_same.set()

            t1 = threading.Thread(target=first_same)
            t2 = threading.Thread(target=second_same)
            t1.start(); t2.start()
            self.assertTrue(inside_same.wait(timeout=2))
            self.assertFalse(
                second_acquired_same.wait(timeout=0.1),
                "second caller for same folder must wait while first holds the section",
            )
            release_same.set()
            t1.join(timeout=2); t2.join(timeout=2)
            self.assertTrue(second_acquired_same.is_set())
            self.assertEqual(storage._folder_marker_cs_locks, {})

            # Different folder: must NOT block.
            inside_a = threading.Event()
            release_a = threading.Event()
            inside_b = threading.Event()

            def folder_a():
                with storage.folder_marker_critical_section("a"):
                    inside_a.set()
                    self.assertTrue(release_a.wait(timeout=2))

            def folder_b():
                self.assertTrue(inside_a.wait(timeout=2))
                with storage.folder_marker_critical_section("b"):
                    inside_b.set()

            ta = threading.Thread(target=folder_a)
            tb = threading.Thread(target=folder_b)
            ta.start(); tb.start()
            self.assertTrue(
                inside_b.wait(timeout=2),
                "different-folder caller must not be blocked by holder of another folder",
            )
            release_a.set()
            ta.join(timeout=2); tb.join(timeout=2)
            self.assertEqual(storage._folder_marker_cs_locks, {})

    def test_marker_critical_section_prevents_stale_marker_in_race_window(self):
        # The classic interleaving the mutex is here to forbid:
        #   copy:   takes CS, runs recheck (snapshot match) -> writes marker
        #   create: takes CS afterwards, deletes marker
        # End state must be NO marker. Without the mutex, create's invalidate
        # could land between copy's recheck and copy's marker write, leaving
        # a stale marker that hides an un-uploaded file.
        with tempfile.TemporaryDirectory() as root:
            with mock.patch("local_storage.subprocess.run", side_effect=_fake_du_for(root)):
                storage = LocalStorage(root=root, max_size_mb=1)

            folder = os.path.join(root, "series")
            os.makedirs(folder)
            marker_path = os.path.join(folder, ".s3-uploaded")

            copy_inside_cs = threading.Event()
            copy_finished_recheck = threading.Event()
            create_done = threading.Event()

            def copy_side():
                with storage.folder_marker_critical_section("series"):
                    copy_inside_cs.set()
                    # Let create_side try (and block) before we publish.
                    time.sleep(0.05)
                    copy_finished_recheck.set()
                    with open(marker_path, "w") as f:
                        _ = f.write("series.zip")

            def create_side():
                self.assertTrue(copy_inside_cs.wait(timeout=2))
                # This must block on the mutex until copy_side releases.
                with storage.folder_marker_critical_section("series"):
                    self.assertTrue(
                        copy_finished_recheck.is_set(),
                        "create entered CS before copy finished its recheck-and-publish window",
                    )
                    try:
                        os.remove(marker_path)
                    except FileNotFoundError:
                        pass
                    create_done.set()

            t_copy = threading.Thread(target=copy_side)
            t_create = threading.Thread(target=create_side)
            t_copy.start(); t_create.start()
            t_copy.join(timeout=2); t_create.join(timeout=2)

            self.assertTrue(create_done.is_set())
            self.assertFalse(
                os.path.exists(marker_path),
                "create's invalidate must win the end state; mutex orders the two",
            )
            self.assertEqual(storage._folder_marker_cs_locks, {})

    def test_remove_swallows_filenotfound_race_with_eviction(self):
        # Eviction can rmtree the parent folder between `os.path.exists` and
        # `os.remove`. The remove path must absorb that race instead of letting
        # FileNotFoundError leak back to Orthanc's storage callback.
        with tempfile.TemporaryDirectory() as root:
            with mock.patch("local_storage.subprocess.run", side_effect=_fake_du_for(root)):
                storage = LocalStorage(root=root, max_size_mb=1)

            with mock.patch("local_storage.os.remove", side_effect=FileNotFoundError("gone")):
                # Must not raise.
                storage.remove(
                    uuid="instance",
                    local_series_folder="series",
                    content_type=orthanc_stub.ContentType.DICOM,
                )


class ConcurrentStressTests(unittest.TestCase):
    """Best-effort multi-thread stress.

    Spins up a real ``LocalStorage`` against a real temp directory and runs
    writers, readers, removers and evictors against it concurrently. Each
    worker uses the public storage API just like Orthanc would. The asserts
    only check global invariants -- no exceptions surface, accounting
    stays consistent, and no scan slot or write count is left dangling.
    """

    def test_writers_readers_evictor_keep_state_consistent(self):
        import random

        with tempfile.TemporaryDirectory() as root:
            with mock.patch("local_storage.subprocess.run", side_effect=_fake_du_walk(root)):
                # Generous budget so the fast path is usually taken, but small
                # enough that some calls cross the slow path during the run.
                storage = LocalStorage(root=root, max_size_mb=4)

                # Treat every folder as safe to evict so eviction actually fires.
                storage.set_eviction_guard(lambda folder_name: True)

                folders = [f"series-{i}" for i in range(6)]
                stop = threading.Event()
                errors: list[BaseException] = []
                errors_lock = threading.Lock()

                def record(exc: BaseException) -> None:
                    with errors_lock:
                        errors.append(exc)

                def writer(seed: int) -> None:
                    rng = random.Random(seed)
                    with mock.patch("local_storage.subprocess.run", side_effect=_fake_du_walk(root)):
                        for _ in range(60):
                            if stop.is_set():
                                return
                            folder = rng.choice(folders)
                            uuid = f"u-{rng.randint(0, 999)}-{seed}-{_}"
                            content = os.urandom(rng.randint(64, 4096))
                            try:
                                storage.write_file(local_series_folder=folder, uuid=uuid, content=content)
                            except Exception as e:
                                record(e)
                                return

                def reader(seed: int) -> None:
                    rng = random.Random(seed + 1000)
                    for _ in range(80):
                        if stop.is_set():
                            return
                        folder = rng.choice(folders)
                        uuid = f"u-{rng.randint(0, 999)}-{seed}-{_}"
                        try:
                            with storage.lease_folder(folder):
                                if storage.has_local_file(
                                    uuid=uuid,
                                    local_series_folder=folder,
                                    content_type=orthanc_stub.ContentType.DICOM,
                                ):
                                    storage.read_file(uuid=uuid, local_series_folder=folder)
                        except FileNotFoundError:
                            # Acceptable: file was evicted/removed between
                            # has_local_file and read; the lease only protects
                            # the folder, not individual files post-eviction.
                            pass
                        except Exception as e:
                            record(e)
                            return

                def remover(seed: int) -> None:
                    rng = random.Random(seed + 2000)
                    for _ in range(40):
                        if stop.is_set():
                            return
                        folder = rng.choice(folders)
                        uuid = f"u-{rng.randint(0, 999)}-{seed}-{_}"
                        try:
                            storage.remove(
                                uuid=uuid,
                                local_series_folder=folder,
                                content_type=orthanc_stub.ContentType.DICOM,
                            )
                        except Exception as e:
                            record(e)
                            return

                def evictor() -> None:
                    with mock.patch("local_storage.subprocess.run", side_effect=_fake_du_walk(root)):
                        for _ in range(15):
                            if stop.is_set():
                                return
                            try:
                                storage.evict_all_safe()
                            except Exception as e:
                                record(e)
                                return

                threads: list[threading.Thread] = []
                for i in range(4):
                    threads.append(threading.Thread(target=writer, args=(i,)))
                for i in range(4):
                    threads.append(threading.Thread(target=reader, args=(i,)))
                for i in range(2):
                    threads.append(threading.Thread(target=remover, args=(i,)))
                threads.append(threading.Thread(target=evictor))

                for t in threads:
                    t.start()

                deadline = time.monotonic() + 8
                for t in threads:
                    remaining = max(0.1, deadline - time.monotonic())
                    t.join(timeout=remaining)
                stop.set()

                for t in threads:
                    if t.is_alive():
                        self.fail(f"thread did not finish in time: {t.name}")

                if errors:
                    self.fail(f"workers raised: {[type(e).__name__ + ': ' + str(e) for e in errors]}")

                # Drain any final state with one more refresh so accounting
                # reflects what is actually on disk.
                with mock.patch("local_storage.subprocess.run", side_effect=_fake_du_walk(root)):
                    storage._update_local_storage_stats()

                # Global invariants after the storm.
                self.assertEqual(storage._active_writers, 0)
                self.assertFalse(storage._scan_in_progress)
                self.assertEqual(storage._folder_lease_counts, {})
                self.assertEqual(storage._reserved_bytes, 0)
                # Available + apparent disk usage must equal max_size after
                # a fresh rescan (no reservations leaked).
                used_estimate = storage._max_size - storage._available_size
                self.assertGreaterEqual(used_estimate, 0)
                self.assertLessEqual(used_estimate, storage._max_size)


class _ReadPathLocalStorage:
    def __init__(self):
        self.lease_depth = 0
        self.read_saw_lease = False

    @contextmanager
    def lease_folder(self, local_series_folder):
        self.lease_depth += 1
        try:
            yield
        finally:
            self.lease_depth -= 1

    def has_local_file(self, uuid, local_series_folder, content_type):
        if self.lease_depth <= 0:
            raise AssertionError("has_local_file called without a folder lease")
        return True

    def read_range(self, uuid, local_series_folder, content_type, range_start, size):
        if self.lease_depth <= 0:
            raise AssertionError("read_range called without a folder lease")
        self.read_saw_lease = True
        return orthanc_stub.ErrorCode.SUCCESS, b"dicom"


class _UnusedZipManager:
    def retrieve_zip_from_s3(self, s3_zip_key, local_series_folder):
        raise AssertionError("retrieve_zip_from_s3 should not be called for a local hit")


class S3ZipStorageReadTests(unittest.TestCase):
    def test_local_hit_keeps_folder_leased_from_check_through_read(self):
        local_storage = _ReadPathLocalStorage()
        storage = S3ZipStorage.__new__(S3ZipStorage)
        storage._local_storage = local_storage
        storage._zip_manager = _UnusedZipManager()

        custom_data = CustomData(
            storage=CustomData.Storage.S3_ZIP,
            local_series_folder="series",
            s3_zip_key="series.zip",
        ).to_binary()

        error_code, data = storage.storage_read_range(
            uuid="instance",
            content_type=orthanc_stub.ContentType.DICOM,
            range_start=0,
            size=0,
            custom_data=custom_data,
        )

        self.assertEqual(error_code, orthanc_stub.ErrorCode.SUCCESS)
        self.assertEqual(data, b"dicom")
        self.assertTrue(local_storage.read_saw_lease)
        self.assertEqual(local_storage.lease_depth, 0)


class _RetrievalLocalStorage:
    def __init__(self):
        self.lease_depth = 0
        self.max_lease_depth = 0
        self.writes = []

    @contextmanager
    def lease_folder(self, local_series_folder):
        self.lease_depth += 1
        self.max_lease_depth = max(self.max_lease_depth, self.lease_depth)
        try:
            yield
        finally:
            self.lease_depth -= 1

    def write_file(self, local_series_folder, uuid, content):
        if self.lease_depth <= 0:
            raise AssertionError("write_file called without a folder lease")
        self.writes.append((local_series_folder, uuid, content))

    def read_file(self, local_series_folder, uuid):
        raise AssertionError("read_file is not used by retrieval")


class _ZipS3Client:
    def download_file(self, bucket_name, s3_zip_key, destination_path):
        with zipfile.ZipFile(destination_path, "w") as zipf:
            zipf.writestr("a", b"A")
            zipf.writestr("b", b"B")


class _FlakyZipS3Client:
    def __init__(self, failures_before_success):
        self.failures_before_success = failures_before_success
        self.download_attempts = 0

    def download_file(self, bucket_name, s3_zip_key, destination_path):
        self.download_attempts += 1
        if self.download_attempts <= self.failures_before_success:
            raise ConnectionError("temporary S3 connection glitch")
        with zipfile.ZipFile(destination_path, "w") as zipf:
            zipf.writestr("a", b"A")


class _BadZipS3Client:
    def __init__(self):
        self.download_attempts = 0

    def download_file(self, bucket_name, s3_zip_key, destination_path):
        self.download_attempts += 1
        with open(destination_path, "wb") as f:
            f.write(b"this is not a zip")


class _BlockingFailS3Client:
    def __init__(self):
        self.download_started = threading.Event()
        self.release_failure = threading.Event()
        self.download_attempts = 0

    def download_file(self, bucket_name, s3_zip_key, destination_path):
        self.download_attempts += 1
        self.download_started.set()
        self.release_failure.wait(timeout=5)
        raise ConnectionError("shared retrieval failure")


class ZipRetrievalTests(unittest.TestCase):
    def _new_manager(self, local_storage, s3_client=None, max_attempts=3):
        return LocalToS3ZipManager(
            s3_client=s3_client or _ZipS3Client(),
            bucket_name="bucket",
            local_storage=local_storage,
            enable_compression=False,
            uncommitted_series_handler=object(),
            s3_retrieval_max_attempts=max_attempts,
            s3_retrieval_retry_base_delay_sec=0,
            s3_retrieval_retry_max_delay_sec=0,
        )

    def test_retrieval_refcount_is_owned_by_manager_lock(self):
        manager = self._new_manager(_RetrievalLocalStorage())

        first, first_is_new = manager._acquire_zip_retrieval("series.zip")
        second, second_is_new = manager._acquire_zip_retrieval("series.zip")

        self.assertTrue(first_is_new)
        self.assertFalse(second_is_new)
        self.assertIs(first, second)
        self.assertEqual(first._ref_count, 2)

        manager._release_zip_retrieval(first)
        self.assertIs(manager._s3_zip_retrievals["series.zip"], first)
        self.assertEqual(first._ref_count, 1)

        manager._release_zip_retrieval(second)
        self.assertNotIn("series.zip", manager._s3_zip_retrievals)
        self.assertEqual(first._ref_count, 0)

    def test_retrieve_zip_from_s3_holds_folder_lease_while_extracting(self):
        local_storage = _RetrievalLocalStorage()
        manager = self._new_manager(local_storage)

        manager.retrieve_zip_from_s3(
            s3_zip_key="series.zip",
            local_series_folder="series",
        )

        self.assertEqual(
            local_storage.writes,
            [("series", "a", b"A"), ("series", "b", b"B")],
        )
        self.assertGreaterEqual(local_storage.max_lease_depth, 1)
        self.assertEqual(local_storage.lease_depth, 0)
        self.assertEqual(manager._s3_zip_retrievals, {})

    def test_retrieve_zip_from_s3_retries_transient_download_failure(self):
        local_storage = _RetrievalLocalStorage()
        s3_client = _FlakyZipS3Client(failures_before_success=2)
        manager = self._new_manager(local_storage, s3_client=s3_client, max_attempts=3)

        manager.retrieve_zip_from_s3(
            s3_zip_key="series.zip",
            local_series_folder="series",
        )

        self.assertEqual(s3_client.download_attempts, 3)
        self.assertEqual(local_storage.writes, [("series", "a", b"A")])
        self.assertEqual(manager._s3_zip_retrievals, {})

    def test_retrieve_zip_from_s3_does_not_retry_bad_zip(self):
        local_storage = _RetrievalLocalStorage()
        s3_client = _BadZipS3Client()
        manager = self._new_manager(local_storage, s3_client=s3_client, max_attempts=3)

        with self.assertRaises(zipfile.BadZipFile):
            manager.retrieve_zip_from_s3(
                s3_zip_key="series.zip",
                local_series_folder="series",
            )

        self.assertEqual(s3_client.download_attempts, 1)
        self.assertEqual(local_storage.writes, [])
        self.assertEqual(manager._s3_zip_retrievals, {})

    def test_waiting_retrieval_callers_share_terminal_failure(self):
        local_storage = _RetrievalLocalStorage()
        s3_client = _BlockingFailS3Client()
        manager = self._new_manager(local_storage, s3_client=s3_client, max_attempts=1)
        errors = []

        def retrieve():
            try:
                manager.retrieve_zip_from_s3(
                    s3_zip_key="series.zip",
                    local_series_folder="series",
                )
            except Exception as e:
                errors.append(e)

        first = threading.Thread(target=retrieve)
        second = threading.Thread(target=retrieve)
        first.start()
        self.assertTrue(s3_client.download_started.wait(timeout=5))
        second.start()

        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with manager._s3_zip_retrievals_lock:
                retrieval = manager._s3_zip_retrievals.get("series.zip")
                ref_count = retrieval._ref_count if retrieval is not None else 0
            if ref_count >= 2:
                break
            time.sleep(0.01)
        else:
            self.fail("second retrieval caller did not acquire the shared ZipRetrieval")

        s3_client.release_failure.set()
        first.join(timeout=5)
        second.join(timeout=5)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(len(errors), 2)
        self.assertTrue(all(isinstance(e, ConnectionError) for e in errors))
        self.assertEqual(s3_client.download_attempts, 1)
        self.assertEqual(manager._s3_zip_retrievals, {})


class _CopyLocalStorage:
    def __init__(self, root):
        self.root = root
        self.lease_depth = 0
        self.max_lease_depth = 0
        self.marker_cs_depth = 0
        self.max_marker_cs_depth = 0
        self.reads = []

    @contextmanager
    def lease_folder(self, local_series_folder):
        self.lease_depth += 1
        self.max_lease_depth = max(self.max_lease_depth, self.lease_depth)
        try:
            yield
        finally:
            self.lease_depth -= 1

    @contextmanager
    def folder_marker_critical_section(self, local_series_folder):
        self.marker_cs_depth += 1
        self.max_marker_cs_depth = max(self.max_marker_cs_depth, self.marker_cs_depth)
        try:
            yield
        finally:
            self.marker_cs_depth -= 1

    def read_file(self, uuid, local_series_folder):
        if self.lease_depth <= 0:
            raise AssertionError("read_file called without a folder lease")
        self.reads.append((uuid, local_series_folder))
        return f"content-{uuid}".encode("ascii")

    def write_file(self, local_series_folder, uuid, content):
        raise AssertionError("write_file is not used by copy_series_to_s3")

    def get_folder_path(self, local_series_folder):
        return os.path.join(self.root, local_series_folder)


class _UploadS3Client:
    def __init__(self):
        self.uploads = []
        self.uploaded_zip_entries = []

    def upload_file(self, source_path, bucket_name, s3_key):
        self.uploads.append((bucket_name, s3_key))
        with zipfile.ZipFile(source_path, "r") as zipf:
            self.uploaded_zip_entries = sorted(zipf.namelist())


class _UncommittedHandler:
    def __init__(self):
        self.committed = []

    def on_committed_series(self, series_id):
        self.committed.append(series_id)


class CopySeriesToS3Tests(unittest.TestCase):
    def _make_manager(self, local_storage, s3_client=None, uncommitted_handler=None):
        return LocalToS3ZipManager(
            s3_client=s3_client or _UploadS3Client(),
            bucket_name="bucket",
            local_storage=local_storage,
            enable_compression=False,
            uncommitted_series_handler=uncommitted_handler or _UncommittedHandler(),
            s3_retrieval_retry_base_delay_sec=0,
            s3_retrieval_retry_max_delay_sec=0,
        )

    def test_copy_series_to_s3_leases_source_folder_and_writes_marker_atomically(self):
        with tempfile.TemporaryDirectory() as root:
            local_storage = _CopyLocalStorage(root)
            s3_client = _UploadS3Client()
            uncommitted_handler = _UncommittedHandler()
            manager = self._make_manager(local_storage, s3_client, uncommitted_handler)

            custom_data = CustomData(
                storage=CustomData.Storage.LOCAL,
                local_series_folder="series",
            )
            set_custom_data_calls = []

            with mock.patch.object(manager, "_get_instances_attachments", return_value=["a", "b"]):
                with mock.patch.object(CustomData, "from_orthanc_attachment", return_value=custom_data):
                    with mock.patch.object(
                        orthanc_stub,
                        "SetAttachmentCustomData",
                        side_effect=lambda uuid, data: set_custom_data_calls.append((uuid, data)),
                    ):
                        manager.copy_series_to_s3("orthanc-series")

            self.assertEqual(local_storage.reads, [("a", "series"), ("b", "series")])
            self.assertGreaterEqual(local_storage.max_lease_depth, 1)
            self.assertEqual(local_storage.lease_depth, 0)
            self.assertEqual(s3_client.uploads, [("bucket", "orthanc-series.zip")])
            self.assertEqual(s3_client.uploaded_zip_entries, ["a", "b"])
            self.assertEqual([uuid for uuid, _ in set_custom_data_calls], ["a", "b"])
            self.assertEqual(uncommitted_handler.committed, ["orthanc-series"])
            marker_path = os.path.join(root, "series", ".s3-uploaded")
            with open(marker_path, "r") as f:
                self.assertEqual(f.read(), "orthanc-series.zip")

    def test_copy_skips_marker_when_new_instance_arrives_during_copy(self):
        # Recheck-before-marker: if a new attachment appears between the
        # initial snapshot and the marker write, the uploaded zip is already
        # incomplete and the marker must NOT be published. The next stable
        # event will trigger a fresh copy that captures the new instance.
        with tempfile.TemporaryDirectory() as root:
            local_storage = _CopyLocalStorage(root)
            uncommitted_handler = _UncommittedHandler()
            manager = self._make_manager(local_storage, uncommitted_handler=uncommitted_handler)

            custom_data = CustomData(
                storage=CustomData.Storage.LOCAL,
                local_series_folder="series",
            )

            # First call: initial snapshot. Second call (the recheck): a third
            # attachment has appeared.
            attachment_calls = [["a", "b"], ["a", "b", "c"]]

            with mock.patch.object(manager, "_get_instances_attachments", side_effect=attachment_calls):
                with mock.patch.object(CustomData, "from_orthanc_attachment", return_value=custom_data):
                    manager.copy_series_to_s3("orthanc-series")

            # Snapshot's instances were still uploaded + custom-data'd: the
            # snapshot's own data is valid in S3. Only the marker is withheld.
            self.assertEqual(local_storage.reads, [("a", "series"), ("b", "series")])
            self.assertFalse(
                os.path.exists(os.path.join(root, "series", ".s3-uploaded")),
                "marker must not be written when attachment set changed during copy",
            )
            # We still commit -- the data we did upload is on S3 and the next
            # stable-series event will re-fire copy_series_to_s3 to cover the
            # new instance.
            self.assertEqual(uncommitted_handler.committed, ["orthanc-series"])

    def test_invalidate_s3_uploaded_marker_removes_existing_marker(self):
        with tempfile.TemporaryDirectory() as root:
            local_storage = _CopyLocalStorage(root)
            manager = self._make_manager(local_storage)

            folder = os.path.join(root, "series")
            os.makedirs(folder)
            marker_path = os.path.join(folder, ".s3-uploaded")
            with open(marker_path, "w") as f:
                _ = f.write("series.zip")

            self.assertTrue(manager.invalidate_s3_uploaded_marker("series"))
            self.assertFalse(os.path.exists(marker_path))

            # Idempotent on missing marker.
            self.assertFalse(manager.invalidate_s3_uploaded_marker("series"))


if __name__ == "__main__":
    unittest.main()
