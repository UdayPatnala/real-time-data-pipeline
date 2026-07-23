import os
import shutil
import tempfile
import threading
import time
import unittest
import pandas as pd

from pipeline.ingestor import WeatherIngestor
from pipeline.processor import StreamProcessor


class TestProcessor(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.raw_path = os.path.join(self.test_dir, "raw_weather.csv")
        self.processed_path = os.path.join(self.test_dir, "processed_weather.csv")
        self.ingestor = WeatherIngestor(output_path=self.raw_path)
        self.processor = StreamProcessor(raw_path=self.raw_path, processed_path=self.processed_path)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_metric_computation(self):
        records = [
            {
                "timestamp_utc": "2024-01-01T00:00:00Z",
                "city": "Bengaluru",
                "temperature_c": 20.0,
                "humidity_percent": 50,
                "wind_speed_kmh": 10.0,
            },
            {
                "timestamp_utc": "2024-01-01T00:01:00Z",
                "city": "Bengaluru",
                "temperature_c": 22.0,
                "humidity_percent": 60,
                "wind_speed_kmh": 12.0,
            },
            {
                "timestamp_utc": "2024-01-01T00:02:00Z",
                "city": "Bengaluru",
                "temperature_c": 24.0,
                "humidity_percent": 70,
                "wind_speed_kmh": 14.0,
            },
        ]
        for r in records:
            self.ingestor.save_data(r)

        df_processed = self.processor.process()
        self.assertIsNotNone(df_processed)
        self.assertEqual(len(df_processed), 3)

        # Check rolling average computation
        # Row 1 temp rolling avg: 20.0
        # Row 2 temp rolling avg: (20+22)/2 = 21.0
        # Row 3 temp rolling avg: (20+22+24)/3 = 22.0
        rolling_temps = df_processed["temp_rolling_avg"].tolist()
        self.assertEqual(rolling_temps, [20.0, 21.0, 22.0])

    def test_concurrent_ingest_and_processing(self):
        """Simulate concurrent threads ingesting and processing data simultaneously."""
        errors = []
        num_ingest_threads = 5
        records_per_thread = 20

        def ingest_worker(thread_id):
            try:
                for i in range(records_per_thread):
                    rec = {
                        "timestamp_utc": f"2024-01-01T00:{thread_id:02d}:{i:02d}Z",
                        "city": "Bengaluru",
                        "temperature_c": 20.0 + i,
                        "humidity_percent": 50 + (i % 10),
                        "wind_speed_kmh": 10.0 + (i % 5),
                    }
                    self.ingestor.save_data(rec)
                    time.sleep(0.002)
            except Exception as e:
                errors.append(f"Ingest thread {thread_id} error: {e}")

        def process_worker(thread_id):
            try:
                for _ in range(records_per_thread):
                    self.processor.process()
                    time.sleep(0.003)
            except Exception as e:
                errors.append(f"Process thread {thread_id} error: {e}")

        threads = []
        for t_id in range(num_ingest_threads):
            t_ingest = threading.Thread(target=ingest_worker, args=(t_id,))
            t_proc = threading.Thread(target=process_worker, args=(t_id,))
            threads.extend([t_ingest, t_proc])

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        self.assertEqual(errors, [], f"Encountered concurrency errors: {errors}")

        # Final process run to ensure state is clean
        final_df = self.processor.process()
        self.assertIsNotNone(final_df)
        expected_total_records = num_ingest_threads * records_per_thread
        self.assertEqual(len(final_df), expected_total_records)
        self.assertTrue(os.path.exists(self.processed_path))

    def test_concurrent_json_stream(self):
        json_path = os.path.join(self.test_dir, "stream.json")
        errors = []

        def json_worker(thread_id):
            try:
                for i in range(10):
                    rec = {
                        "timestamp": time.time(),
                        "thread_id": thread_id,
                        "value": i,
                    }
                    self.ingestor.save_data_json(rec, json_path=json_path)
            except Exception as e:
                errors.append(f"JSON thread {thread_id} error: {e}")

        threads = [threading.Thread(target=json_worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        self.assertTrue(os.path.exists(json_path))

    def test_processor_handles_missing_raw_file(self):
        non_existent_processor = StreamProcessor(
            raw_path=os.path.join(self.test_dir, "missing.csv"),
            processed_path=self.processed_path,
        )
        result = non_existent_processor.process()
        self.assertIsNone(result)

    def test_processor_handles_malformed_csv(self):
        # Write corrupted/malformed raw file
        with open(self.raw_path, "w", encoding="utf-8") as f:
            f.write("corrupted,header,only\nnot,a,valid,number\n")

        result = self.processor.process()
        # Should drop rows where timestamp or numeric values fail to parse
        self.assertTrue(result is None or len(result) == 0)


if __name__ == "__main__":
    unittest.main()
