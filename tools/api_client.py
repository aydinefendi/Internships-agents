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
            
            # Normalize the response format
            normalized_data = self._normalize_job_data(json_data)
            
            logger.info(f"Retrieved {len(normalized_data.get('jobs', []))} jobs from RapidAPI")
            
            conn.close()
            return normalized_data
            
        except Exception as e:
            logger.error(f"Error searching jobs: {str(e)}")
            raise

    def is_security_posting(title, description, security_indicators):
        text = f"{title} {description}".lower()
        return any(kw in text for kw in security_indicators)
    
    filtered_jobs = [
        job for job in jobs 
        if is_security_posting(job['title'], job.get('description', ''), filters["security_indicators"])
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
            
            response = self.session.get(
                f"{self.base_url}/jobs/{job_id}",
                timeout=30
            )
            
            response.raise_for_status()
            data = response.json()
            
            return self._normalize_job_data({'jobs': [data]})['jobs'][0]
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get job details for {job_id}: {str(e)}")
            raise
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
            
            response = self.session.get(
                f"{self.base_url}/companies/{company_id}",
                timeout=30
            )
            
            response.raise_for_status()
            data = response.json()
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get company info for {company_id}: {str(e)}")
            raise
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
            # Extract salary information
            
            normalized_job = {
                'id': job.get('id', job.get('job_id', '')),
                'title': job.get('title', job.get('job_title', '')),
                'organization': job.get('organization', job.get('organization_name', '')),
                'organization_id': job.get('organization_id', ''),
                'organization_url': job.get('organization_url', job.get('organization_url', '')),
                'addressCountry': job.get('addressCountry', job.get('address_country', '')),
                'addressLocality': job.get('addressLocality', job.get('address_locality', '')),
                'date_posted': job.get('date_posted', job.get('date_posted', '')),
                'description': job.get('description', job.get('job_description', '')),
                'external_apply_url': job.get('url', job.get('job_url', '')),
                'linkedin_org_industry': job.get('linkedin_org_industry', job.get('linkedin_org_industry', '')),
                'linkedin_org_specialties': job.get('linkedin_org_specialties', job.get('linkedin_org_specialties', '')),
                'linkedin_org_size': job.get('linkedin_org_size', job.get('linkedin_org_size', '')),
                'linkedin_org_headquarters': job.get('linkedin_org_headquarters', job.get('linkedin_org_headquarters', '')),
                'linkedin_org_description': job.get('linkedin_org_description', job.get('linkedin_org_description', '')),
                'job_type': job.get('job_type', job.get('employment_type', '')),
                'remote': job.get('remote', job.get('is_remote', False))
            }
            
            normalized_jobs.append(normalized_job)
       
        return {
            'jobs': normalized_jobs,
            'total_count': len(normalized_jobs),
            'page': 1,
            'per_page': len(normalized_jobs)
        }
    
    
    def _parse_salary_string(self, salary_str: str) -> Optional[Dict]:
        """Parse salary information from a string."""
        import re
        
        # Look for patterns like "$50,000 - $70,000" or "£30,000-£40,000"
        pattern = r'[\$£€]?([0-9,]+)\s*-\s*[\$£€]?([0-9,]+)'
        match = re.search(pattern, salary_str)
        
        if match:
            min_sal = int(match.group(1).replace(',', ''))
            max_sal = int(match.group(2).replace(',', ''))
            
            currency = 'USD'
            if '£' in salary_str:
                currency = 'GBP'
            elif '€' in salary_str:
                currency = 'EUR'
            
            return {
                'min': min_sal,
                'max': max_sal,
                'currency': currency,
                'period': 'yearly'
            }
        
        return None
    
    def _extract_salary(self, salary_data) -> Optional[Dict]:
        """Extract and normalize salary information (legacy method)."""
        if not salary_data:
            return None
        
        if isinstance(salary_data, dict):
            return {
                'min': salary_data.get('min'),
                'max': salary_data.get('max'),
                'currency': salary_data.get('currency', 'USD'),
                'period': salary_data.get('period', 'yearly')
            }
        
        return None
