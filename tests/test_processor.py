import unittest
import pandas as pd
from pipeline.processor import StreamProcessor

class TestProcessor(unittest.TestCase):
    def setUp(self):
        self.processor = StreamProcessor()

    def test_metric_computation(self):
        # Mock data
        data = {
            "timestamp_utc": ["2024-01-01T00:00:00", "2024-01-01T00:01:00", "2024-01-01T00:02:00"],
            "temperature_c": [20.0, 21.0, 22.0],
            "humidity_percent": [50, 55, 60],
            "wind_speed_kmh": [10.0, 11.0, 12.0]
        }
        df = pd.DataFrame(data)
        
        # Test cleaning logic
        # Note: In a real scenario, we'd mock the file system, 
        # but for this portfolio piece, we'll test the logic directly if accessible.
        pass

if __name__ == "__main__":
    unittest.main()
