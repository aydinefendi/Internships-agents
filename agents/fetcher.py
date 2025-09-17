"""
Fetcher Agent - Retrieves internship data from Job Board API.

This agent is responsible for:
- Fetching raw internship data from Fantastic.jobs API
- Handling API rate limiting and pagination
- Storing raw data with timestamps
"""

from crewai import Agent
from tools.api_client import JobBoardAPIClient
from tools.db import DatabaseManager
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class FetcherAgent:
    """Agent responsible for fetching internship data from external APIs."""
    
    def __init__(self, api_client: JobBoardAPIClient, db_manager: DatabaseManager):
        self.api_client = api_client
        self.db_manager = db_manager
        
        self.agent = Agent(
            role="Data Fetcher",
            goal="Fetch internship data from job board APIs",
            backstory="You are an expert data fetcher specializing in job board APIs. "
                      "You understand rate limiting, pagination, and data extraction patterns. ",
            verbose=True,
            allow_delegation=False,
            tools=[self.api_client]
        )
    
    def fetch_internships(self, filters: dict, location: str = None, limit: int = 100):
        """
        Fetch internships based on filters and location.
        
        Args:
            filters: Dictionary containing internship_indicators and security_indicators
            location: Optional location filter
            limit: Maximum number of jobs to fetch
            
        Returns:
            dict: Summary of fetch operation
        """
        try:
            logger.info(f"Starting fetch with filters: {filters}")
            
            # Fetch data from API
            raw_data = self.api_client.search_jobs(
                filters=filters,
                location=location,
                limit=limit
            )
            
            # Store raw data with timestamp
            timestamp = datetime.now().isoformat()
            raw_data['metadata'] = {
                'fetched_at': timestamp,
                'filters': filters,
                'location': location,
                'total_count': len(raw_data.get('jobs', []))
            }
            
            # Save to database
            self.db_manager.store_raw_data(raw_data)
            
            logger.info(f"Successfully fetched {len(raw_data.get('jobs', []))} jobs")
            
            return {
                'status': 'success',
                'jobs_fetched': len(raw_data.get('jobs', [])),
                'timestamp': timestamp
            }
            
        except Exception as e:
            logger.error(f"Error fetching internships: {str(e)}")
            return {
                'status': 'error',
                'error': str(e)
            }
