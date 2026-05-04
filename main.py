import time
import argparse
from pipeline.ingestor import WeatherIngestor
from pipeline.processor import StreamProcessor
from pipeline.config import config
from pipeline.utils.logger import setup_logger

logger = setup_logger("Main")

def run_ingestor():
    ingestor = WeatherIngestor()
    logger.info("Starting Weather Ingestor...")
    while True:
        ingestor.run_once()
        time.sleep(config.POLL_INTERVAL)

def run_processor():
    processor = StreamProcessor()
    logger.info("Starting Stream Processor...")
    while True:
        processor.run_once()
        time.sleep(config.POLL_INTERVAL)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Real-Time Weather Pipeline CLI")
    parser.add_argument("mode", choices=["ingest", "process"], help="Run mode: ingest or process")
    
    args = parser.parse_args()
    
    try:
        if args.mode == "ingest":
            run_ingestor()
        elif args.mode == "process":
            run_processor()
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user.")
