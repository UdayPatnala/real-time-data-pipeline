import os
import shutil
import sys
import tempfile
import threading
import time
import unittest
import pandas as pd

# Add root directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pipeline.ingestor import WeatherIngestor
from pipeline.processor import StreamProcessor


class StressTestProcessor(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.raw_path = os.path.join(self.test_dir, "raw_weather_stress.csv")
        self.processed_path = os.path.join(self.test_dir, "processed_weather_stress.csv")
        self.json_path = os.path.join(self.test_dir, "stream_weather_stress.json")
        self.ingestor = WeatherIngestor(output_path=self.raw_path)
        self.processor = StreamProcessor(raw_path=self.raw_path, processed_path=self.processed_path)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_high_concurrency_ingest_and_processor_stress(self):
        """Stress-test processor with 20 concurrent ingest threads and 10 processor threads."""
        num_ingest_threads = 20
        records_per_thread = 25
        num_process_threads = 10
        process_cycles = 15

        errors = []
        stop_event = threading.Event()

        def ingest_worker(thread_id):
            try:
                for i in range(records_per_thread):
                    if stop_event.is_set():
                        break
                    rec = {
                        "timestamp_utc": f"2026-07-23T10:{thread_id:02d}:{i:02d}Z",
                        "city": f"City_{thread_id}",
                        "temperature_c": 15.0 + (i % 15) + (thread_id * 0.1),
                        "humidity_percent": 40 + (i % 50),
                        "wind_speed_kmh": 5.0 + (i % 20),
                    }
                    self.ingestor.save_data(rec)
                    time.sleep(0.001)
            except Exception as e:
                errors.append(f"Ingest thread {thread_id} failed: {e}")

        def process_worker(thread_id):
            try:
                for _ in range(process_cycles):
                    if stop_event.is_set():
                        break
                    res = self.processor.process()
                    time.sleep(0.002)
            except Exception as e:
                errors.append(f"Process thread {thread_id} failed: {e}")

        threads = []
        for i in range(num_ingest_threads):
            t = threading.Thread(target=ingest_worker, args=(i,))
            threads.append(t)

        for i in range(num_process_threads):
            t = threading.Thread(target=process_worker, args=(i,))
            threads.append(t)

        start_time = time.time()
        for t in threads:
            t.start()

        for t in threads:
            t.join()
        duration = time.time() - start_time

        self.assertEqual(errors, [], f"Encountered errors during multi-threaded stress: {errors}")

        # Final processor run to verify consistency
        final_df = self.processor.process()
        self.assertIsNotNone(final_df, "Final processed DataFrame should not be None")
        expected_total = num_ingest_threads * records_per_thread
        self.assertEqual(
            len(final_df),
            expected_total,
            f"Expected {expected_total} records, found {len(final_df)} in final processed CSV",
        )
        self.assertTrue(os.path.exists(self.processed_path))

        # Verify rolling average calculation on full dataset
        self.assertIn("temp_rolling_avg", final_df.columns)
        self.assertIn("humidity_rolling_avg", final_df.columns)
        self.assertIn("wind_rolling_avg", final_df.columns)
        print(f"[SUCCESS] Multi-threaded ingest & processor stress passed ({expected_total} records processed in {duration:.3f}s)")

    def test_concurrent_reader_file_lock_contention(self):
        """Simulate file contention where reader threads open processed_path while processor executes os.replace."""
        errors = []
        replace_failures = []
        num_readers = 5
        duration_sec = 2.0
        stop_flag = False

        # Pre-seed data
        for i in range(10):
            self.ingestor.save_data({
                "timestamp_utc": f"2026-07-23T00:00:{i:02d}Z",
                "city": "Bengaluru",
                "temperature_c": 25.0,
                "humidity_percent": 60,
                "wind_speed_kmh": 10.0,
            })
        self.processor.process()

        def reader_worker():
            nonlocal stop_flag
            while not stop_flag:
                try:
                    if os.path.exists(self.processed_path):
                        with open(self.processed_path, "r", encoding="utf-8") as f:
                            _ = f.read()
                except Exception as e:
                    # Reader caught file lock/replace transition
                    pass
                time.sleep(0.0005)

        def processor_worker():
            nonlocal stop_flag
            count = 0
            start = time.time()
            while time.time() - start < duration_sec:
                res = self.processor.process()
                if res is None:
                    replace_failures.append("process() returned None during lock contention")
                count += 1
                time.sleep(0.001)

        reader_threads = [threading.Thread(target=reader_worker) for _ in range(num_readers)]
        proc_thread = threading.Thread(target=processor_worker)

        for t in reader_threads:
            t.start()
        proc_thread.start()

        proc_thread.join()
        stop_flag = True
        for t in reader_threads:
            t.join()

        print(f"[INFO] Reader contention test completed. Replace failures caught = {len(replace_failures)}")

    def test_concurrent_json_stream_high_volume(self):
        """Stress-test atomic JSON stream write with 15 concurrent threads writing 20 records each."""
        num_threads = 15
        recs_per_thread = 20
        errors = []

        def json_worker(t_id):
            try:
                for i in range(recs_per_thread):
                    rec = {
                        "ts": time.time(),
                        "thread_id": t_id,
                        "seq": i,
                        "payload": "x" * 100,
                    }
                    self.ingestor.save_data_json(rec, json_path=self.json_path)
            except Exception as e:
                errors.append(f"JSON thread {t_id} error: {e}")

        threads = [threading.Thread(target=json_worker, args=(i,)) for i in range(num_threads)]
        start_t = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        duration = time.time() - start_t

        self.assertEqual(errors, [], f"JSON stream write errors: {errors}")
        self.assertTrue(os.path.exists(self.json_path))

        # Read back JSON stream file to verify structure and record count
        import json
        with open(self.json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        expected_recs = num_threads * recs_per_thread
        self.assertEqual(len(data), expected_recs, f"JSON stream lost records: expected {expected_recs}, got {len(data)}")
        print(f"[SUCCESS] Concurrent JSON stream test passed ({expected_recs} records appended in {duration:.3f}s)")


if __name__ == "__main__":
    unittest.main()
