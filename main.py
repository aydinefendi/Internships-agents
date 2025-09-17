#!/usr/bin/env python3
"""
Main orchestration script for the 2026 Summer Internship pipeline.

This script coordinates the entire pipeline from fetching data to generating reports.
"""

import logging
import yaml
import argparse
import os
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

from agents import FetcherAgent, CleanerAgent
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
            
            logger.info("Pipeline components initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to setup components: {str(e)}")
            raise
    
    def run_daily_pipeline(self, filters: dict = None, location: str = None):
        """Run the daily internship pipeline."""
        try:
            logger.info("Starting daily pipeline")
            
            # Use config filters if none provided
            if not filters:
                filters = {
                    'internship_indicators': self.config['search']['internship_indicators'],
                    'security_indicators': self.config['search']['security_indicators']
                }
            
            # Step 1: Fetch data
            logger.info("Step 1: Fetching internship data")
            fetch_result = self.fetcher.fetch_internships(
                filters=filters,
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
            
            # Step 3: Retrieve and display data
            logger.info("Step 3: Retrieving processed data")
            data_summary = self.get_data_summary()
            
            logger.info("Daily pipeline completed successfully")
            logger.info(f"Data summary: {data_summary}")
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
            
            # Get weekly data summary
            weekly_summary = self.get_weekly_summary(start_date, end_date)
            
            logger.info("Weekly analysis completed successfully")
            logger.info(f"Weekly summary: {weekly_summary}")
            return True
            
        except Exception as e:
            logger.error(f"Weekly analysis failed: {str(e)}")
            return False
    
    def run_custom_search(self, filters: dict, location: str = None, limit: int = 50):
        """Run a custom search with specific parameters."""
        try:
            logger.info(f"Running custom search with filters: {filters}")
            
            # Fetch data
            fetch_result = self.fetcher.fetch_internships(
                filters=filters,
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
    
    def get_data_summary(self) -> dict:
        """Get a summary of all processed data using pandas."""
        try:
            # Get all jobs from database
            jobs_df = self.get_jobs_dataframe()
            
            if jobs_df.empty:
                return {"message": "No data available"}
            
            summary = {
                "total_jobs": len(jobs_df),
                "unique_organizations": jobs_df['organization'].nunique(),
                "unique_locations": jobs_df['address_locality'].nunique(),
                "job_types": jobs_df['job_type'].value_counts().to_dict(),
                "top_organizations": jobs_df['organization'].value_counts().head(10).to_dict(),
                "location_distribution": jobs_df['address_locality'].value_counts().head(10).to_dict(),
                "linkedin_industries": jobs_df['linkedin_org_industry'].value_counts().head(10).to_dict(),
                "linkedin_org_sizes": jobs_df['linkedin_org_size'].value_counts().to_dict(),
                "remote_jobs": jobs_df['remote_derived'].sum() if 'remote_derived' in jobs_df.columns else 0
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting data summary: {str(e)}")
            return {"error": str(e)}
    
    def get_weekly_summary(self, start_date: str, end_date: str) -> dict:
        """Get weekly summary of processed data."""
        try:
            # Get jobs from date range
            jobs_df = self.get_jobs_dataframe(start_date, end_date)
            
            if jobs_df.empty:
                return {"message": f"No data available for {start_date} to {end_date}"}
            
            summary = {
                "period": f"{start_date} to {end_date}",
                "total_jobs": len(jobs_df),
                "unique_organizations": jobs_df['organization'].nunique(),
                "unique_locations": jobs_df['address_locality'].nunique(),
                "daily_breakdown": self._get_daily_breakdown(jobs_df),
                "top_organizations": jobs_df['organization'].value_counts().head(10).to_dict(),
                "location_distribution": jobs_df['address_locality'].value_counts().head(10).to_dict(),
                "industry_distribution": jobs_df['linkedin_org_industry'].value_counts().head(10).to_dict()
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting weekly summary: {str(e)}")
            return {"error": str(e)}
    
    def get_jobs_dataframe(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """Get jobs data as a pandas DataFrame."""
        try:
            # Query jobs from database
            cursor = self.db_manager.connection.cursor()
            
            query = "SELECT * FROM jobs"
            params = []
            
            if start_date and end_date:
                query += " WHERE DATE(created_at) BETWEEN ? AND ?"
                params.extend([start_date, end_date])
            elif start_date:
                query += " WHERE DATE(created_at) >= ?"
                params.append(start_date)
            elif end_date:
                query += " WHERE DATE(created_at) <= ?"
                params.append(end_date)
            
            query += " ORDER BY created_at DESC"
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            if not rows:
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame([dict(row) for row in rows])
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting jobs dataframe: {str(e)}")
            return pd.DataFrame()
    
    def _get_daily_breakdown(self, jobs_df: pd.DataFrame) -> dict:
        """Get daily breakdown of job counts."""
        try:
            jobs_df['date'] = pd.to_datetime(jobs_df['created_at']).dt.date
            daily_counts = jobs_df.groupby('date').size().to_dict()
            
            # Convert dates to strings for JSON serialization
            return {str(date): count for date, count in daily_counts.items()}
            
        except Exception as e:
            logger.error(f"Error getting daily breakdown: {str(e)}")
            return {}
    
    def export_to_csv(self, filename: str = None, start_date: str = None, end_date: str = None) -> str:
        """Export jobs data to CSV file."""
        try:
            if not filename:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"internships_export_{timestamp}.csv"
            
            # Get data
            df = self.get_jobs_dataframe(start_date, end_date)
            
            if df.empty:
                logger.warning("No data to export")
                return None
            
            # Ensure data directory exists
            data_dir = Path("data/exports")
            data_dir.mkdir(parents=True, exist_ok=True)
            
            filepath = data_dir / filename
            df.to_csv(filepath, index=False)
            
            logger.info(f"Data exported to {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"Error exporting to CSV: {str(e)}")
            return None


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='2026 Summer Internship Pipeline')
    parser.add_argument('--mode', choices=['daily', 'weekly', 'custom', 'export', 'summary'], 
                       default='daily', help='Pipeline mode to run')
    parser.add_argument('--internship-keywords', nargs='+', help='Internship keywords for custom mode')
    parser.add_argument('--security-keywords', nargs='+', help='Security keywords for custom mode')
    parser.add_argument('--location', help='Search location for custom mode')
    parser.add_argument('--limit', type=int, default=50, help='Limit for custom search')
    parser.add_argument('--export-filename', help='Filename for CSV export')
    parser.add_argument('--start-date', help='Start date for data filtering (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date for data filtering (YYYY-MM-DD)')
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
            if not args.internship_keywords and not args.security_keywords:
                logger.error("At least one keyword type required for custom mode")
                return 1
            
            filters = {}
            if args.internship_keywords:
                filters['internship_indicators'] = args.internship_keywords
            if args.security_keywords:
                filters['security_indicators'] = args.security_keywords
                
            success = pipeline.run_custom_search(
                filters=filters,
                location=args.location,
                limit=args.limit
            )
        elif args.mode == 'export':
            # Export data to CSV
            filepath = pipeline.export_to_csv(
                filename=args.export_filename,
                start_date=args.start_date,
                end_date=args.end_date
            )
            if filepath:
                logger.info(f"Data exported to {filepath}")
                success = True
            else:
                logger.error("Export failed")
                success = False
        elif args.mode == 'summary':
            # Show data summary
            summary = pipeline.get_data_summary()
            print("\n" + "="*50)
            print("DATA SUMMARY")
            print("="*50)
            for key, value in summary.items():
                print(f"{key}: {value}")
            print("="*50)
            success = True
        
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
