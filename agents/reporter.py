"""
Reporter Agent - Creates digest reports from processed internship data.

This agent is responsible for:
- Generating daily/weekly digest reports
- Creating CSV exports for analysis
- Producing markdown summaries
- Formatting data for different audiences
"""

from crewai import Agent
from tools.db import DatabaseManager
import logging
import pandas as pd
from datetime import datetime
import os

logger = logging.getLogger(__name__)


class ReporterAgent:
    """Agent responsible for creating reports and summaries."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        
        self.agent = Agent(
            role="Report Generator",
            goal="Create comprehensive reports and summaries of internship data",
            backstory="You are an expert report generator with experience in data visualization "
                     "and business intelligence. You excel at creating clear, actionable reports "
                     "from complex datasets.",
            verbose=True,
            allow_delegation=False
        )
    
    def generate_daily_report(self, date: str = None):
        """
        Generate a daily digest report.
        
        Args:
            date: Date for the report (defaults to today)
            
        Returns:
            dict: Report generation summary
        """
        try:
            if not date:
                date = datetime.now().strftime('%Y-%m-%d')
            
            logger.info(f"Generating daily report for {date}")
            
            # Get processed data for the date
            processed_data = self.db_manager.get_processed_data_by_date(date)
            if not processed_data:
                logger.warning(f"No processed data found for {date}")
                return {'status': 'no_data', 'message': f'No data available for {date}'}
            
            # Generate different report formats
            csv_path = self._generate_csv_report(processed_data, date)
            md_path = self._generate_markdown_report(processed_data, date)
            
            logger.info(f"Daily report generated successfully")
            
            return {
                'status': 'success',
                'date': date,
                'csv_path': csv_path,
                'markdown_path': md_path,
                'job_count': len(processed_data.get('jobs', []))
            }
            
        except Exception as e:
            logger.error(f"Error generating daily report: {str(e)}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def generate_weekly_summary(self, start_date: str, end_date: str):
        """
        Generate a weekly summary report.
        
        Args:
            start_date: Start date for the summary
            end_date: End date for the summary
            
        Returns:
            dict: Summary generation result
        """
        try:
            logger.info(f"Generating weekly summary from {start_date} to {end_date}")
            
            # Get all processed data in date range
            weekly_data = self.db_manager.get_processed_data_by_date_range(start_date, end_date)
            
            if not weekly_data:
                return {'status': 'no_data', 'message': 'No data available for date range'}
            
            # Aggregate statistics
            total_jobs = sum(len(data.get('jobs', [])) for data in weekly_data)
            companies = set()
            locations = set()
            
            for data in weekly_data:
                for job in data.get('jobs', []):
                    if job.get('company'):
                        companies.add(job['company'])
                    if job.get('location'):
                        locations.add(job['location'])
            
            # Generate summary report
            summary = {
                'period': f"{start_date} to {end_date}",
                'total_jobs': total_jobs,
                'unique_companies': len(companies),
                'unique_locations': len(locations),
                'daily_breakdown': self._get_daily_breakdown(weekly_data)
            }
            
            # Save summary
            summary_path = self._save_weekly_summary(summary, start_date, end_date)
            
            return {
                'status': 'success',
                'summary_path': summary_path,
                'summary': summary
            }
            
        except Exception as e:
            logger.error(f"Error generating weekly summary: {str(e)}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def _generate_csv_report(self, data: dict, date: str) -> str:
        """Generate CSV report from processed data."""
        jobs = data.get('jobs', [])
        if not jobs:
            return None
        
        # Convert to DataFrame
        df = pd.DataFrame(jobs)
        
        # Create reports directory if it doesn't exist
        reports_dir = '/Users/ae/Desktop/2026-summer-is-going-to-change-your-life/data/reports'
        os.makedirs(reports_dir, exist_ok=True)
        
        # Save CSV
        csv_path = os.path.join(reports_dir, f'daily_report_{date}.csv')
        df.to_csv(csv_path, index=False)
        
        return csv_path
    
    def _generate_markdown_report(self, data: dict, date: str) -> str:
        """Generate markdown report from processed data."""
        jobs = data.get('jobs', [])
        if not jobs:
            return None
        
        # Create reports directory if it doesn't exist
        reports_dir = '/Users/ae/Desktop/2026-summer-is-going-to-change-your-life/data/reports'
        os.makedirs(reports_dir, exist_ok=True)
        
        # Generate markdown content
        md_content = f"# Daily Internship Report - {date}\n\n"
        md_content += f"**Total Jobs Found:** {len(jobs)}\n\n"
        
        # Group by company
        companies = {}
        for job in jobs:
            company = job.get('company', 'Unknown')
            if company not in companies:
                companies[company] = []
            companies[company].append(job)
        
        md_content += f"**Unique Companies:** {len(companies)}\n\n"
        
        # List jobs by company
        for company, company_jobs in companies.items():
            md_content += f"## {company}\n\n"
            for job in company_jobs:
                md_content += f"- **{job.get('title', 'N/A')}**\n"
                md_content += f"  - Location: {job.get('location', 'N/A')}\n"
                md_content += f"  - Salary: {job.get('salary', 'N/A')}\n"
                md_content += f"  - URL: {job.get('url', 'N/A')}\n\n"
        
        # Save markdown
        md_path = os.path.join(reports_dir, f'daily_report_{date}.md')
        with open(md_path, 'w') as f:
            f.write(md_content)
        
        return md_path
    
    def _get_daily_breakdown(self, weekly_data: list) -> dict:
        """Get daily breakdown of job counts."""
        daily_counts = {}
        for data in weekly_data:
            date = data.get('metadata', {}).get('processed_at', 'Unknown')
            job_count = len(data.get('jobs', []))
            daily_counts[date] = job_count
        return daily_counts
    
    def _save_weekly_summary(self, summary: dict, start_date: str, end_date: str) -> str:
        """Save weekly summary to file."""
        reports_dir = '/Users/ae/Desktop/2026-summer-is-going-to-change-your-life/data/reports'
        os.makedirs(reports_dir, exist_ok=True)
        
        summary_path = os.path.join(reports_dir, f'weekly_summary_{start_date}_to_{end_date}.json')
        
        import json
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        return summary_path
