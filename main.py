#!/usr/bin/env python3
"""
Main orchestration script for the 2026 Summer Internship pipeline.

This script coordinates the entire pipeline from fetching data to generating reports.
"""

import logging
import yaml
import argparse
import os
from datetime import datetime, timedelta
from pathlib import Path

from agents import FetcherAgent, CleanerAgent, ReporterAgent
from tools import JobBoardAPIClient, DatabaseManager, DeduplicationManager, CompanyEnricher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/pipeline.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class InternshipPipeline:
    """Main pipeline orchestrator."""
    
    def __init__(self, config_path: str = "configs/settings.yaml"):
        self.config = self._load_config(config_path)
        self._setup_components()
    
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {str(e)}")
            raise
    
    def _setup_components(self):
        """Initialize all pipeline components."""
        try:
            # Database
            db_config = self.config['database']
            if db_config['type'] == 'sqlite':
                self.db_manager = DatabaseManager(db_path=db_config['sqlite']['path'])
            else:
                raise NotImplementedError("PostgreSQL not implemented yet")
            
            # API Client
            api_config = self.config['api']['fantastic_jobs']
            api_key = api_config['api_key']
            
            # Handle environment variable if the key looks like a placeholder
            if api_key.startswith('os.getenv'):
                # Extract the environment variable name
                env_var = api_key.split('"')[1] if '"' in api_key else 'RAPID_API_KEY'
                api_key = os.getenv(env_var, api_key)
            
            self.api_client = JobBoardAPIClient(
                api_key=api_key,
                base_url=api_config['base_url']
            )
            
            # Deduplication
            dedup_config = self.config['processing']['deduplication']
            self.dedup_manager = DeduplicationManager(
                similarity_threshold=dedup_config['similarity_threshold']
            )
            
            # Company Enricher
            enrich_config = self.config['processing']['enrichment']
            self.enricher = CompanyEnricher()
            
            # Agents
            self.fetcher = FetcherAgent(self.api_client, self.db_manager)
            self.cleaner = CleanerAgent(self.db_manager, self.dedup_manager, self.enricher)
            self.reporter = ReporterAgent(self.db_manager)
            
            logger.info("Pipeline components initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to setup components: {str(e)}")
            raise
    
    def run_daily_pipeline(self, keywords: list = None, location: str = None):
        """Run the daily internship pipeline."""
        try:
            logger.info("Starting daily pipeline")
            
            # Use config keywords if none provided
            if not keywords:
                keywords = self.config['search']['keywords']
            
            # Step 1: Fetch data
            logger.info("Step 1: Fetching internship data")
            fetch_result = self.fetcher.fetch_internships(
                keywords=keywords,
                location=location,
                limit=100
            )
            
            if fetch_result['status'] != 'success':
                logger.error(f"Fetch failed: {fetch_result}")
                return False
            
            # Step 2: Clean data
            logger.info("Step 2: Cleaning and processing data")
            # Get the latest raw data ID (this would need to be implemented in db_manager)
            raw_data_id = fetch_result.get('raw_data_id', 1)  # Simplified
            
            clean_result = self.cleaner.clean_data(raw_data_id)
            
            if clean_result['status'] != 'success':
                logger.error(f"Cleaning failed: {clean_result}")
                return False
            
            # Step 3: Generate report
            logger.info("Step 3: Generating daily report")
            report_result = self.reporter.generate_daily_report()
            
            if report_result['status'] != 'success':
                logger.error(f"Report generation failed: {report_result}")
                return False
            
            logger.info("Daily pipeline completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Daily pipeline failed: {str(e)}")
            return False
    
    def run_weekly_analysis(self):
        """Run weekly analysis and summary."""
        try:
            logger.info("Starting weekly analysis")
            
            # Calculate date range
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            
            # Generate weekly summary
            summary_result = self.reporter.generate_weekly_summary(start_date, end_date)
            
            if summary_result['status'] != 'success':
                logger.error(f"Weekly analysis failed: {summary_result}")
                return False
            
            logger.info("Weekly analysis completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Weekly analysis failed: {str(e)}")
            return False
    
    def run_custom_search(self, keywords: list, location: str = None, limit: int = 50):
        """Run a custom search with specific parameters."""
        try:
            logger.info(f"Running custom search: {keywords}")
            
            # Fetch data
            fetch_result = self.fetcher.fetch_internships(
                keywords=keywords,
                location=location,
                limit=limit
            )
            
            if fetch_result['status'] != 'success':
                logger.error(f"Custom search failed: {fetch_result}")
                return False
            
            # Clean data
            raw_data_id = fetch_result.get('raw_data_id', 1)
            clean_result = self.cleaner.clean_data(raw_data_id)
            
            if clean_result['status'] != 'success':
                logger.error(f"Data cleaning failed: {clean_result}")
                return False
            
            logger.info("Custom search completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Custom search failed: {str(e)}")
            return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='2026 Summer Internship Pipeline')
    parser.add_argument('--mode', choices=['daily', 'weekly', 'custom'], 
                       default='daily', help='Pipeline mode to run')
    parser.add_argument('--keywords', nargs='+', help='Search keywords for custom mode')
    parser.add_argument('--location', help='Search location for custom mode')
    parser.add_argument('--limit', type=int, default=50, help='Limit for custom search')
    parser.add_argument('--config', default='configs/settings.yaml', 
                       help='Path to configuration file')
    
    args = parser.parse_args()
    
    # Create logs directory
    Path('logs').mkdir(exist_ok=True)
    
    try:
        # Initialize pipeline
        pipeline = InternshipPipeline(args.config)
        
        # Run based on mode
        if args.mode == 'daily':
            success = pipeline.run_daily_pipeline()
        elif args.mode == 'weekly':
            success = pipeline.run_weekly_analysis()
        elif args.mode == 'custom':
            if not args.keywords:
                logger.error("Keywords required for custom mode")
                return 1
            success = pipeline.run_custom_search(
                keywords=args.keywords,
                location=args.location,
                limit=args.limit
            )
        
        if success:
            logger.info("Pipeline completed successfully")
            return 0
        else:
            logger.error("Pipeline failed")
            return 1
            
    except Exception as e:
        logger.error(f"Pipeline initialization failed: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main())
