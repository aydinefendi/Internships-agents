"""
Company enrichment utilities.

This module provides functionality to enrich company information
using LLM analysis and external data sources like Wikipedia.
"""

import logging
import requests
import json
from typing import Dict, Optional
import time

logger = logging.getLogger(__name__)


class CompanyEnricher:
    """Enriches company information using various data sources."""
    
    def __init__(self, llm_client=None, wikipedia_api_url: str = "https://en.wikipedia.org/api/rest_v1"):
        self.llm_client = llm_client
        self.wikipedia_api_url = wikipedia_api_url
        self.cache = {}  # Simple in-memory cache
        self.rate_limit_delay = 1.0  # seconds between requests
    
    def enrich_company(self, company_name: str) -> Dict:
        """
        Enrich company information from multiple sources.
        
        Args:
            company_name: Name of the company to enrich
            
        Returns:
            Dict containing enriched company information
        """
        try:
            # Check cache first
            if company_name in self.cache:
                logger.debug(f"Using cached data for {company_name}")
                return self.cache[company_name]
            
            logger.info(f"Enriching company: {company_name}")
            
            enrichment = {
                'name': company_name,
                'description': '',
                'industry': '',
                'size': '',
                'website': '',
                'wikipedia_summary': '',
                'founded_year': None,
                'headquarters': '',
                'employee_count': None,
                'revenue': None,
                'sources': []
            }
            
            # Get Wikipedia information
            wiki_info = self._get_wikipedia_info(company_name)
            if wiki_info:
                enrichment.update(wiki_info)
                enrichment['sources'].append('wikipedia')
            
            # Get additional info from LLM if available
            if self.llm_client:
                llm_info = self._get_llm_company_info(company_name)
                if llm_info:
                    enrichment.update(llm_info)
                    enrichment['sources'].append('llm')
            
            # Cache the result
            self.cache[company_name] = enrichment
            
            logger.info(f"Successfully enriched {company_name}")
            return enrichment
            
        except Exception as e:
            logger.error(f"Error enriching company {company_name}: {str(e)}")
            return {
                'name': company_name,
                'error': str(e),
                'sources': []
            }
    
    def _get_wikipedia_info(self, company_name: str) -> Optional[Dict]:
        """Get company information from Wikipedia."""
        try:
            self._rate_limit()
            
            # Search for the company
            search_url = f"{self.wikipedia_api_url}/page/summary/{company_name.replace(' ', '_')}"
            
            response = requests.get(search_url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                return {
                    'wikipedia_summary': data.get('extract', ''),
                    'description': data.get('extract', ''),
                    'website': self._extract_website_from_wiki(data),
                    'headquarters': self._extract_headquarters_from_wiki(data)
                }
            
            # Try alternative search if direct lookup fails
            return self._search_wikipedia_alternative(company_name)
            
        except Exception as e:
            logger.debug(f"Wikipedia lookup failed for {company_name}: {str(e)}")
            return None
    
    def _search_wikipedia_alternative(self, company_name: str) -> Optional[Dict]:
        """Alternative Wikipedia search method."""
        try:
            self._rate_limit()
            
            # Use search API
            search_url = f"https://en.wikipedia.org/w/api.php"
            params = {
                'action': 'query',
                'format': 'json',
                'list': 'search',
                'srsearch': company_name,
                'srlimit': 1
            }
            
            response = requests.get(search_url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                search_results = data.get('query', {}).get('search', [])
                
                if search_results:
                    # Get the first result
                    page_title = search_results[0]['title']
                    return self._get_wikipedia_info(page_title)
            
            return None
            
        except Exception as e:
            logger.debug(f"Alternative Wikipedia search failed: {str(e)}")
            return None
    
    def _extract_website_from_wiki(self, wiki_data: Dict) -> str:
        """Extract website URL from Wikipedia data."""
        # This would need to be implemented based on Wikipedia's data structure
        # For now, return empty string
        return ""
    
    def _extract_headquarters_from_wiki(self, wiki_data: Dict) -> str:
        """Extract headquarters location from Wikipedia data."""
        # This would need to be implemented based on Wikipedia's data structure
        # For now, return empty string
        return ""
    
    def _get_llm_company_info(self, company_name: str) -> Optional[Dict]:
        """Get company information using LLM analysis."""
        if not self.llm_client:
            return None
        
        try:
            prompt = f"""
            Please provide information about the company "{company_name}". 
            Focus on:
            - Industry/sector
            - Company size (startup, mid-size, large corporation)
            - Brief description of what they do
            - Founded year (if known)
            - Employee count estimate (if known)
            
            Format your response as JSON with these fields:
            {{
                "industry": "...",
                "size": "...",
                "description": "...",
                "founded_year": null,
                "employee_count": null
            }}
            """
            
            # This would need to be implemented based on your LLM client
            # For now, return None
            return None
            
        except Exception as e:
            logger.debug(f"LLM enrichment failed for {company_name}: {str(e)}")
            return None
    
    def _rate_limit(self):
        """Simple rate limiting."""
        time.sleep(self.rate_limit_delay)
    
    def clear_cache(self):
        """Clear the enrichment cache."""
        self.cache.clear()
        logger.info("Company enrichment cache cleared")
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics."""
        return {
            'cache_size': len(self.cache),
            'cached_companies': list(self.cache.keys())
        }
