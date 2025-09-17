"""
Deduplication utilities for job postings.

This module provides functionality to identify and remove
duplicate job postings using various hashing and similarity techniques.
"""

import hashlib
import logging
from typing import List, Dict, Set
from difflib import SequenceMatcher
import re

logger = logging.getLogger(__name__)


class DeduplicationManager:
    """Manages deduplication of job postings."""
    
    def __init__(self, similarity_threshold: float = 0.8):
        self.similarity_threshold = similarity_threshold
        self.seen_hashes: Set[str] = set()
    
    def remove_duplicates(self, jobs: List[Dict]) -> List[Dict]:
        """
        Remove duplicate jobs from a list.
        
        Args:
            jobs: List of job dictionaries
            
        Returns:
            List of unique jobs
        """
        try:
            logger.info(f"Starting deduplication of {len(jobs)} jobs")
            
            unique_jobs = []
            duplicate_count = 0
            
            for job in jobs:
                # Generate hash for this job
                job_hash = self._generate_job_hash(job)
                
                # Check if we've seen this exact job before
                if job_hash in self.seen_hashes:
                    duplicate_count += 1
                    logger.debug(f"Found exact duplicate: {job.get('title', 'Unknown')}")
                    continue
                
                # Check for similar jobs
                if self._is_similar_to_existing(job, unique_jobs):
                    duplicate_count += 1
                    logger.debug(f"Found similar job: {job.get('title', 'Unknown')}")
                    continue
                
                # This is a unique job
                unique_jobs.append(job)
                self.seen_hashes.add(job_hash)
            
            logger.info(f"Deduplication complete: {len(unique_jobs)} unique jobs, "
                       f"{duplicate_count} duplicates removed")
            
            return unique_jobs
            
        except Exception as e:
            logger.error(f"Error during deduplication: {str(e)}")
            return jobs  # Return original list if deduplication fails
    
    def _generate_job_hash(self, job: Dict) -> str:
        """
        Generate a hash for a job based on key identifying fields.
        
        Args:
            job: Job dictionary
            
        Returns:
            Hash string
        """
        # Normalize key fields for hashing
        title = self._normalize_text(job.get('title', ''))
        company = self._normalize_text(job.get('company', ''))
        location = self._normalize_text(job.get('location', ''))
        description = self._normalize_text(job.get('description', ''))[:500]  # Limit length
        
        # Create hash from normalized fields
        hash_string = f"{title}|{company}|{location}|{description}"
        return hashlib.md5(hash_string.encode('utf-8')).hexdigest()
    
    def _normalize_text(self, text: str) -> str:
        """
        Normalize text for comparison.
        
        Args:
            text: Input text
            
        Returns:
            Normalized text
        """
        if not text:
            return ""
        
        # Convert to lowercase
        text = text.lower()
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove common punctuation
        text = re.sub(r'[^\w\s]', '', text)
        
        # Remove common words that don't add meaning
        stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'}
        words = text.split()
        words = [word for word in words if word not in stop_words]
        
        return ' '.join(words)
    
    def _is_similar_to_existing(self, job: Dict, existing_jobs: List[Dict]) -> bool:
        """
        Check if a job is similar to any existing job.
        
        Args:
            job: Job to check
            existing_jobs: List of existing jobs
            
        Returns:
            True if similar job found
        """
        job_title = self._normalize_text(job.get('title', ''))
        job_company = self._normalize_text(job.get('company', ''))
        
        for existing_job in existing_jobs:
            existing_title = self._normalize_text(existing_job.get('title', ''))
            existing_company = self._normalize_text(existing_job.get('company', ''))
            
            # Check title similarity
            title_similarity = SequenceMatcher(None, job_title, existing_title).ratio()
            
            # Check company similarity
            company_similarity = SequenceMatcher(None, job_company, existing_company).ratio()
            
            # If both title and company are very similar, it's likely a duplicate
            if (title_similarity > self.similarity_threshold and 
                company_similarity > self.similarity_threshold):
                return True
            
            # If title is extremely similar (95%+) regardless of company
            if title_similarity > 0.95:
                return True
        
        return False
    
    def get_duplicate_groups(self, jobs: List[Dict]) -> List[List[Dict]]:
        """
        Group jobs by similarity to identify potential duplicates.
        
        Args:
            jobs: List of job dictionaries
            
        Returns:
            List of groups, where each group contains similar jobs
        """
        try:
            groups = []
            processed_indices = set()
            
            for i, job in enumerate(jobs):
                if i in processed_indices:
                    continue
                
                # Start a new group with this job
                group = [job]
                processed_indices.add(i)
                
                # Find similar jobs
                for j, other_job in enumerate(jobs[i+1:], i+1):
                    if j in processed_indices:
                        continue
                    
                    if self._is_similar_to_existing(other_job, [job]):
                        group.append(other_job)
                        processed_indices.add(j)
                
                # Only add groups with more than one job
                if len(group) > 1:
                    groups.append(group)
            
            logger.info(f"Found {len(groups)} duplicate groups")
            return groups
            
        except Exception as e:
            logger.error(f"Error grouping duplicates: {str(e)}")
            return []
    
    def reset_hashes(self):
        """Reset the seen hashes set (useful for testing)."""
        self.seen_hashes.clear()
        logger.info("Deduplication hashes reset")
