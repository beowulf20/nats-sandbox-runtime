"""Run with python3 -m unittest discover -s scripts/python-bench."""
import hashlib
from pathlib import Path
import tempfile
import threading
import time
import unittest

from io_backend import BenchIO


class BenchIOTest(unittest.TestCase):
    def test_thread_pool_is_bounded(self):
        engine = BenchIO('threads')
        lock = threading.Lock()
        active = peak = 0
        def measured(path):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            try:
                time.sleep(0.02)
                return 'hash'
            finally:
                with lock:
                    active -= 1
        engine._hash_sync = measured
        try:
            self.assertEqual(len(engine.hash_files([Path(str(i)) for i in range(12)])), 12)
            self.assertEqual(peak, 4)
            self.assertEqual(active, 0)
        finally:
            self.assertEqual(engine.close(), 0)

    def test_streamed_content_and_failure_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = bytes(range(256)) * 1027  # Multiple chunks, uneven final chunk.
            paths = [root / str(i) for i in range(9)]
            for path in paths:
                path.write_bytes(data)
            expected = {path.name: hashlib.sha256(data).hexdigest() for path in paths}
            for kind in ('sync', 'threads'):
                with self.subTest(kind=kind):
                    engine = BenchIO(kind)
                    try:
                        self.assertEqual(engine.hash_files(paths), expected)
                        output = root / 'output'
                        output.write_bytes(b'x' * (len(data) * 4))
                        engine.write(output, data, 3)
                        self.assertEqual(output.read_bytes(), data * 3)
                        with self.assertRaises(FileNotFoundError):
                            engine.hash_files([root / 'missing', *paths])
                    finally:
                        self.assertEqual(engine.close(), 0)


if __name__ == '__main__':
    unittest.main()
