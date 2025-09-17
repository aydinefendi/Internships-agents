"""
Cleaner Agent - Deduplicates, filters, and detects fake job postings.

This agent is responsible for:
- Removing duplicate job postings
- Filtering out irrelevant positions
- Detecting potentially fake job postings using LLM
- Enriching company information
"""

from crewai import Agent
from tools.dedup import DeduplicationManager
from tools.enrich import CompanyEnricher
from tools.db import DatabaseManager
import logging

logger = logging.getLogger(__name__)


class CleanerAgent:
    """Agent responsible for cleaning and enriching internship data."""
    
    def __init__(self, db_manager: DatabaseManager, dedup_manager: DeduplicationManager, 
                 enricher: CompanyEnricher):
        self.db_manager = db_manager
        self.dedup_manager = dedup_manager
        self.enricher = enricher
        
        self.agent = Agent(
            role="Data Cleaner",
            goal="Clean, deduplicate, and enrich internship data",
            backstory="You are an expert data cleaner with experience in job board data. "
                     "You excel at identifying duplicates, filtering irrelevant content, "
                     "and detecting fraudulent job postings.",
            verbose=True,
            allow_delegation=False,
            tools=[self.dedup_manager, self.enricher]
        )
    
    def clean_data(self, raw_data_id: str, filters: dict = None):
        """
        Clean and process raw internship data.
        
        Args:
            raw_data_id: ID of the raw data to process
            filters: Optional filtering criteria
            
        Returns:
            dict: Summary of cleaning operation
        """
        try:
            logger.info(f"Starting data cleaning for raw_data_id: {raw_data_id}")
            
            # Get raw data
            raw_data = self.db_manager.get_raw_data(raw_data_id)
            if not raw_data:
                raise ValueError(f"No raw data found for ID: {raw_data_id}")
            
            jobs = raw_data.get('jobs', [])
            logger.info(f"Processing {len(jobs)} jobs")
            
            # Step 1: Deduplication
            unique_jobs = self.dedup_manager.remove_duplicates(jobs)
            logger.info(f"After deduplication: {len(unique_jobs)} jobs")
            
            # Step 2: Apply filters
            if filters:
                filtered_jobs = self._apply_filters(unique_jobs, filters)
                logger.info(f"After filtering: {len(filtered_jobs)} jobs")
            else:
                filtered_jobs = unique_jobs
            
            # Step 3: Fake detection
            verified_jobs = self._detect_fake_jobs(filtered_jobs)
            logger.info(f"After fake detection: {len(verified_jobs)} jobs")
            
            # Step 4: Company enrichment
            enriched_jobs = self._enrich_companies(verified_jobs)
            logger.info(f"After enrichment: {len(enriched_jobs)} jobs")
            
            # Store processed data
            processed_data = {
                'raw_data_id': raw_data_id,
                'jobs': enriched_jobs,
                'metadata': {
                    'original_count': len(jobs),
                    'after_dedup': len(unique_jobs),
                    'after_filtering': len(filtered_jobs),
                    'after_verification': len(verified_jobs),
                    'final_count': len(enriched_jobs)
                }
            }
            
            self.db_manager.store_processed_data(processed_data)
            
            logger.info("Data cleaning completed successfully")
            
            return {
                'status': 'success',
                'processed_data_id': processed_data.get('id'),
                'summary': processed_data['metadata']
            }
            
        except Exception as e:
            logger.error(f"Error cleaning data: {str(e)}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def _apply_filters(self, jobs: list, filters: dict) -> list:
        """Apply filtering criteria to job list."""
        filtered = []
        
        for job in jobs:
            # Apply keyword filters
            if 'keywords' in filters:
                job_text = f"{job.get('title', '')} {job.get('description', '')}".lower()
                if not any(keyword.lower() in job_text for keyword in filters['keywords']):
                    continue
            
            # Apply location filters
            if 'location' in filters:
                job_location = job.get('location', '').lower()
                if filters['location'].lower() not in job_location:
                    continue
            
            # Apply salary filters
            if 'min_salary' in filters:
                salary = job.get('salary', 0)
                if salary < filters['min_salary']:
                    continue
            
            filtered.append(job)
        
        return filtered
    
    def _detect_fake_jobs(self, jobs: list) -> list:
        """Detect potentially fake job postings using LLM."""
        verified = []
        
        for job in jobs:
            # Simple heuristics for now - can be enhanced with LLM
            is_likely_fake = (
                'work from home' in job.get('title', '').lower() and
                'no experience' in job.get('description', '').lower() and
                'immediate start' in job.get('description', '').lower()
            )
            
            if not is_likely_fake:
                verified.append(job)
        
        return verified
    
    def _enrich_companies(self, jobs: list) -> list:
        """Enrich company information for each job."""
        enriched = []
        
        for job in jobs:
            company_name = job.get('company', '')
            if company_name:
                enrichment = self.enricher.enrich_company(company_name)
                job['company_info'] = enrichment
            
            enriched.append(job)
        
        return enriched
