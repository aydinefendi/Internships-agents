"""
API Client for Fantastic.jobs Job Board API.

This module provides a wrapper around the Fantastic.jobs API
with rate limiting, error handling, and data normalization.
"""

import http.client
import json
import time
import logging
from typing import List, Dict, Optional
from datetime import datetime
import urllib.parse

logger = logging.getLogger(__name__)


class JobBoardAPIClient:
    """Client for interacting with the RapidAPI Internships API."""
    
    def __init__(self, api_key: str, base_url: str = "internships-api.p.rapidapi.com"):
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {
            'x-rapidapi-key': api_key,
            'x-rapidapi-host': base_url
        }
        
        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 1.0  # seconds between requests
    
    def search_jobs(self, filters: Dict, location: str = None, 
                   limit: int = 100, page: int = 1) -> Dict:
        """
        Search for jobs using keywords and optional location.
        
        Args:
            keywords: List of keywords to search for
            location: Optional location filter
            limit: Maximum number of results per page
            page: Page number for pagination
            
        Returns:
            Dict containing job results and metadata
        """
        try:
            self._rate_limit()
            
            # Build the search query for RapidAPI
            title_filter = " OR ".join(filters["internship_indicators"])
            location_filter = location if location else ""
            
            # URL encode the parameters
            title_encoded = urllib.parse.quote(title_filter)
            location_encoded = urllib.parse.quote(location_filter) if location_filter else ""
            
            # Build the endpoint URL
            endpoint = f"/active-jb-7d?title_filter={title_encoded}"
            if location_encoded:
                endpoint += f"&location_filter={location_encoded}"
            
            # Make API request using http.client
            conn = http.client.HTTPSConnection(self.base_url)
            conn.request("GET", endpoint, headers=self.headers)
            
            response = conn.getresponse()
            data = response.read()
            
            if response.status != 200:
                raise Exception(f"API request failed with status {response.status}: {data.decode('utf-8')}")
            
            # Parse JSON response
            json_data = json.loads(data.decode('utf-8'))
            
            # Apply security filtering if security_indicators are provided
            jobs = json_data if isinstance(json_data, list) else json_data.get('jobs', [])
            if 'security_indicators' in filters:
                jobs = self._filter_security_jobs(jobs, filters['security_indicators'])
                logger.info(f"Filtered to {len(jobs)} security-related jobs")
            
            # Normalize the response format
            normalized_data = self._normalize_job_data(jobs)
            
            logger.info(f"Retrieved {len(normalized_data.get('jobs', []))} jobs from RapidAPI")
            
            conn.close()
            return normalized_data
            
        except Exception as e:
            logger.error(f"Error searching jobs: {str(e)}")
            raise

    def _is_security_posting(self, title: str, description: str, security_indicators: List[str]) -> bool:
        """Check if a job posting is related to security based on title and description."""
        text = f"{title} {description}".lower()
        return any(kw.lower() in text for kw in security_indicators)
    
    def _filter_security_jobs(self, jobs: List[Dict], security_indicators: List[str]) -> List[Dict]:
        """Filter jobs to only include security-related postings."""
        return [
            job for job in jobs 
            if self._is_security_posting(job.get('title', ''), job.get('description', ''), security_indicators)
        ]
    
    def get_job_details(self, job_id: str) -> Dict:
        """
        Get detailed information for a specific job.
        
        Args:
            job_id: Unique identifier for the job
            
        Returns:
            Dict containing detailed job information
        """
        try:
            self._rate_limit()
            
            # For RapidAPI, we would need a different endpoint for individual job details
            # This is a placeholder implementation
            logger.warning(f"Individual job details not supported by RapidAPI for job_id: {job_id}")
            return {}
            
        except Exception as e:
            logger.error(f"Error getting job details: {str(e)}")
            raise
    
    def get_company_info(self, company_id: str) -> Dict:
        """
        Get company information.
        
        Args:
            company_id: Unique identifier for the company
            
        Returns:
            Dict containing company information
        """
        try:
            self._rate_limit()
            
            # For RapidAPI, we would need a different endpoint for company details
            # This is a placeholder implementation
            logger.warning(f"Individual company details not supported by RapidAPI for company_id: {company_id}")
            return {}
            
        except Exception as e:
            logger.error(f"Error getting company info: {str(e)}")
            raise
    
    def _rate_limit(self):
        """Ensure we don't exceed API rate limits."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _normalize_job_data(self, api_response: Dict) -> Dict:
        """
        Normalize RapidAPI response to a consistent format.
        
        Args:
            api_response: Raw API response from RapidAPI
            
        Returns:
            Normalized job data
        """
        # RapidAPI returns a list of jobs directly
        jobs = api_response if isinstance(api_response, list) else api_response.get('jobs', [])
        normalized_jobs = []
        
        for job in jobs:
            # Extract location information
            locations_raw = job.get('locations_raw', [])
            location_info = self._extract_location_info(locations_raw)
            
            # Extract salary information
            salary_info = self._extract_salary_from_job(job)
            
            # Extract employment type
            employment_types = job.get('employment_type', [])
            job_type = employment_types[0] if employment_types else 'UNKNOWN'
            
            normalized_job = {
                'id': job.get('id', ''),
                'title': job.get('title', ''),
                'organization': job.get('organization', ''),
                'organization_url': job.get('organization_url', ''),
                'organization_logo': job.get('organization_logo', ''),
                'date_posted': job.get('date_posted', ''),
                'date_validthrough': job.get('date_validthrough', ''),
                'description': job.get('description', ''),
                'external_apply_url': job.get('external_apply_url', ''),
                'url': job.get('url', ''),
                
                # Location information
                'address_country': location_info.get('country', ''),
                'address_locality': location_info.get('locality', ''),
                'address_region': location_info.get('region', ''),

                
                # Salary information
                'salary': salary_info,
                'salary_raw': job.get('salary_raw'),
                
                # Job type and seniority
                'job_type': job_type,
                'employment_type': employment_types,

                'directapply': job.get('directapply', False),
                
                # LinkedIn organization information
                'linkedin_org_url': job.get('linkedin_org_url', ''),
                'linkedin_org_size': job.get('linkedin_org_size', ''),
                'linkedin_org_industry': job.get('linkedin_org_industry', ''),
                'linkedin_org_headquarters': job.get('linkedin_org_headquarters', ''),
                'linkedin_org_type': job.get('linkedin_org_type', ''),
                'linkedin_org_specialties': job.get('linkedin_org_specialties', []),
                'linkedin_org_locations': job.get('linkedin_org_locations', []),
                'linkedin_org_description': job.get('linkedin_org_description', ''),
                
                # Additional metadata
                'ats_duplicate': job.get('ats_duplicate', False),
                'raw_data': job  # Keep original for debugging
            }
            
            normalized_jobs.append(normalized_job)
       
        return {
            'jobs': normalized_jobs,
            'total_count': len(normalized_jobs),
            'page': 1,
            'per_page': len(normalized_jobs)
        }
    
    def _extract_location_info(self, locations_raw: List) -> Dict:
        """Extract location information from locations_raw array."""
        if not locations_raw or len(locations_raw) == 0:
            return {}
        
        # Get the first location (usually the primary one)
        location = locations_raw[0]
        address = location.get('address', {})
        
        return {
            'country': address.get('addressCountry', ''),
            'locality': address.get('addressLocality', ''),
            'location_type': location.get('location_type')
        }

        return None
