import os
import subprocess
import sys
import tempfile
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


class ZipRetrievalTests(unittest.TestCase):
    def _new_manager(self, local_storage):
        return LocalToS3ZipManager(
            s3_client=_ZipS3Client(),
            bucket_name="bucket",
            local_storage=local_storage,
            enable_compression=False,
            uncommitted_series_handler=object(),
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


if __name__ == "__main__":
    unittest.main()
