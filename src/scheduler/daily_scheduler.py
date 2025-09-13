"""
Daily scheduler for automated ship lineup data collection and processing
"""

import time
from datetime import datetime, timedelta
from typing import Any, Dict

import pandas as pd
import schedule
from loguru import logger

from config import Config
from src.data_collectors.paranagua_collector import ParanaguaCollector
from src.data_collectors.santos_collector import SantosCollector
from src.database.database_manager import DatabaseManager
from src.etl.medallion_pipeline import MedallionPipeline


class DailyScheduler:
    """Manages daily automated data collection and processing"""

    def __init__(self):
        self.paranagua_collector = ParanaguaCollector()
        self.santos_collector = SantosCollector()
        self.pipeline = MedallionPipeline()
        self.db_manager = DatabaseManager()

        # Setup logging
        logger.add(
            Config.LOG_FILE,
            rotation="1 day",
            retention="30 days",
            level=Config.LOG_LEVEL,
        )

    def run_daily_collection(self):
        """Run daily data collection and processing"""
        logger.info("Starting daily data collection process")

        try:
            # Calculate date range (last 7 days to ensure we don't miss any data)
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=7)

            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")
            collection_date = end_date.strftime("%Y-%m-%d")

            # Collect data from both sources
            results = {}

            # Paranaguá
            logger.info("Collecting data from Paranaguá...")
            paranagua_data = self.paranagua_collector.collect_data(
                start_date_str, end_date_str
            )
            if not paranagua_data.empty:
                results["paranagua"] = self._process_source_data(
                    paranagua_data, "paranagua", collection_date
                )
            else:
                logger.warning("No data collected from Paranaguá")

            # Santos
            logger.info("Collecting data from Santos...")
            santos_data = self.santos_collector.collect_data(
                start_date_str, end_date_str
            )
            if not santos_data.empty:
                results["santos"] = self._process_source_data(
                    santos_data, "santos", collection_date
                )
            else:
                logger.warning("No data collected from Santos")

            # Generate summary report
            self._generate_daily_report(results, collection_date)

            logger.info("Daily data collection process completed successfully")

        except Exception as e:
            logger.error(f"Error in daily collection process: {e}")
            raise

    def _process_source_data(
        self, df, source: str, collection_date: str
    ) -> Dict[str, Any]:
        """Process data for a single source through all medallion layers"""
        logger.info(f"Processing {len(df)} records from {source}")

        try:
            # Bronze layer
            bronze_file = self.pipeline.process_bronze_layer(
                df, source, collection_date
            )
            bronze_count = self.db_manager.insert_bronze_data(df)

            # Silver layer
            silver_file = self.pipeline.process_silver_layer(bronze_file)
            silver_df = pd.read_parquet(silver_file)
            silver_count = self.db_manager.insert_silver_data(silver_df)

            # Gold layer
            gold_file = self.pipeline.process_gold_layer(silver_file)
            gold_df = pd.read_parquet(gold_file)
            gold_count = self.db_manager.insert_gold_data(gold_df)

            return {
                "source": source,
                "bronze_records": bronze_count,
                "silver_records": silver_count,
                "gold_records": gold_count,
                "bronze_file": bronze_file,
                "silver_file": silver_file,
                "gold_file": gold_file,
            }

        except Exception as e:
            logger.error(f"Error processing data from {source}: {e}")
            return {
                "source": source,
                "error": str(e),
                "bronze_records": 0,
                "silver_records": 0,
                "gold_records": 0,
            }

    def _generate_daily_report(self, results: Dict[str, Any], collection_date: str):
        """Generate daily processing report"""
        logger.info("Generating daily report...")

        total_bronze = sum(r.get("bronze_records", 0) for r in results.values())
        total_silver = sum(r.get("silver_records", 0) for r in results.values())
        total_gold = sum(r.get("gold_records", 0) for r in results.values())

        report = {
            "collection_date": collection_date,
            "total_bronze_records": total_bronze,
            "total_silver_records": total_silver,
            "total_gold_records": total_gold,
            "sources_processed": len(results),
            "processing_timestamp": datetime.now().isoformat(),
            "results_by_source": results,
        }

        # Log summary
        logger.info(f"Daily Report - {collection_date}")
        logger.info(f"Total Bronze Records: {total_bronze}")
        logger.info(f"Total Silver Records: {total_silver}")
        logger.info(f"Total Gold Records: {total_gold}")
        logger.info(f"Sources Processed: {len(results)}")

        # Save report to file
        report_file = Config.BASE_DATA_PATH / f"daily_report_{collection_date}.json"
        import json

        with open(report_file, "w") as f:
            json.dump(report, f, indent=2, default=str)

        logger.info(f"Daily report saved to {report_file}")

    def run_incremental_update(self):
        """Run incremental update for new data only"""
        logger.info("Starting incremental update process")

        try:
            # Get latest collection dates for each source
            paranagua_latest = self.db_manager.get_latest_collection_date("paranagua")
            santos_latest = self.db_manager.get_latest_collection_date("santos")

            # Calculate start dates (next day after latest collection)
            today = datetime.now().date()

            if paranagua_latest:
                paranagua_start = paranagua_latest.date() + timedelta(days=1)
            else:
                paranagua_start = today - timedelta(days=7)  # Fallback to last week

            if santos_latest:
                santos_start = santos_latest.date() + timedelta(days=1)
            else:
                santos_start = today - timedelta(days=7)  # Fallback to last week

            # Only collect if there's new data to collect
            if paranagua_start <= today:
                logger.info(
                    f"Collecting incremental data from Paranaguá from {paranagua_start}"
                )
                paranagua_data = self.paranagua_collector.collect_data(
                    paranagua_start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")
                )
                if not paranagua_data.empty:
                    self._process_source_data(
                        paranagua_data, "paranagua", today.strftime("%Y-%m-%d")
                    )

            if santos_start <= today:
                logger.info(
                    f"Collecting incremental data from Santos from {santos_start}"
                )
                santos_data = self.santos_collector.collect_data(
                    santos_start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")
                )
                if not santos_data.empty:
                    self._process_source_data(
                        santos_data, "santos", today.strftime("%Y-%m-%d")
                    )

            logger.info("Incremental update completed")

        except Exception as e:
            logger.error(f"Error in incremental update: {e}")
            raise

    def run_data_cleanup(self):
        """Run data cleanup tasks"""
        logger.info("Running data cleanup tasks")

        try:
            # Clean up old bronze data (keep last 90 days)
            self.db_manager.cleanup_old_data(days_to_keep=90)

            # Generate data quality report
            quality_report = self.db_manager.get_data_quality_report()
            if not quality_report.empty:
                quality_file = (
                    Config.BASE_DATA_PATH
                    / f"quality_report_{datetime.now().strftime('%Y%m%d')}.csv"
                )
                quality_report.to_csv(quality_file, index=False)
                logger.info(f"Data quality report saved to {quality_file}")

            logger.info("Data cleanup tasks completed")

        except Exception as e:
            logger.error(f"Error in data cleanup: {e}")

    def setup_schedule(self):
        """Setup automated schedule"""
        logger.info("Setting up automated schedule")

        # Daily collection at 6:00 AM
        schedule.every().day.at("06:00").do(self.run_daily_collection)

        # Incremental update every 4 hours during business hours
        schedule.every().day.at("10:00").do(self.run_incremental_update)
        schedule.every().day.at("14:00").do(self.run_incremental_update)
        schedule.every().day.at("18:00").do(self.run_incremental_update)

        # Weekly cleanup on Sundays at 2:00 AM
        schedule.every().sunday.at("02:00").do(self.run_data_cleanup)

        logger.info("Schedule setup completed")
        logger.info("Daily collection: 06:00")
        logger.info("Incremental updates: 10:00, 14:00, 18:00")
        logger.info("Weekly cleanup: Sunday 02:00")

    def run_scheduler(self):
        """Run the scheduler continuously"""
        logger.info("Starting scheduler...")

        self.setup_schedule()

        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            except KeyboardInterrupt:
                logger.info("Scheduler stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in scheduler: {e}")
                time.sleep(300)  # Wait 5 minutes before retrying

    def run_manual_collection(self, start_date: str = None, end_date: str = None):
        """Run manual data collection for specific date range"""
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"Running manual collection from {start_date} to {end_date}")

        try:
            # Collect from both sources
            paranagua_data = self.paranagua_collector.collect_data(start_date, end_date)
            santos_data = self.santos_collector.collect_data(start_date, end_date)

            results = {}

            if not paranagua_data.empty:
                results["paranagua"] = self._process_source_data(
                    paranagua_data, "paranagua", end_date
                )

            if not santos_data.empty:
                results["santos"] = self._process_source_data(
                    santos_data, "santos", end_date
                )

            self._generate_daily_report(results, end_date)

            logger.info("Manual collection completed successfully")
            return results

        except Exception as e:
            logger.error(f"Error in manual collection: {e}")
            raise
