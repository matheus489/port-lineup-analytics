"""
Main entry point for Ship Lineup Data Pipeline
"""
import argparse
import sys
from datetime import datetime, timedelta
from loguru import logger
from config import Config
from src.scheduler.daily_scheduler import DailyScheduler


def setup_logging():
    """Setup logging configuration"""
    logger.remove()  # Remove default handler
    
    # Add console handler
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=Config.LOG_LEVEL
    )
    
    # Add file handler
    logger.add(
        Config.LOG_FILE,
        rotation="1 day",
        retention="30 days",
        level=Config.LOG_LEVEL,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
    )


def run_daily_collection():
    """Run daily data collection"""
    logger.info("Starting daily data collection...")
    scheduler = DailyScheduler()
    scheduler.run_daily_collection()


def run_incremental_update():
    """Run incremental data update"""
    logger.info("Starting incremental update...")
    scheduler = DailyScheduler()
    scheduler.run_incremental_update()


def run_manual_collection(start_date: str, end_date: str):
    """Run manual data collection for specific date range"""
    logger.info(f"Starting manual collection from {start_date} to {end_date}")
    scheduler = DailyScheduler()
    return scheduler.run_manual_collection(start_date, end_date)


def run_scheduler():
    """Run the automated scheduler"""
    logger.info("Starting automated scheduler...")
    scheduler = DailyScheduler()
    scheduler.run_scheduler()


def run_data_cleanup():
    """Run data cleanup tasks"""
    logger.info("Starting data cleanup...")
    scheduler = DailyScheduler()
    scheduler.run_data_cleanup()


def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(description="Ship Lineup Data Pipeline")
    parser.add_argument(
        "command",
        choices=[
            "daily", "incremental", "manual", "scheduler", "cleanup", "test"
        ],
        help="Command to execute"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date for manual collection (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date for manual collection (YYYY-MM-DD)"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    
    # Create necessary directories
    Config.create_directories()
    
    try:
        if args.command == "daily":
            run_daily_collection()
            
        elif args.command == "incremental":
            run_incremental_update()
            
        elif args.command == "manual":
            if not args.start_date or not args.end_date:
                logger.error("Manual collection requires --start-date and --end-date")
                sys.exit(1)
            run_manual_collection(args.start_date, args.end_date)
            
        elif args.command == "scheduler":
            run_scheduler()
            
        elif args.command == "cleanup":
            run_data_cleanup()
            
        elif args.command == "test":
            # Test data collection for last 3 days
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
            logger.info(f"Running test collection from {start_date} to {end_date}")
            run_manual_collection(start_date, end_date)
            
    except Exception as e:
        logger.error(f"Error executing command '{args.command}': {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

