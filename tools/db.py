"""
Database management for the internship pipeline.

This module handles SQLite/PostgreSQL setup, data storage,
and retrieval operations for both raw and processed data.
"""

import sqlite3
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime
import os

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database operations for the internship pipeline."""
    
    def __init__(self, db_path: str = None, db_type: str = 'sqlite'):
        self.db_type = db_type
        
        if db_type == 'sqlite':
            if not db_path:
                db_path = '/Users/ae/Desktop/2026-summer-is-going-to-change-your-life/data/internships.db'
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            
            self.db_path = db_path
            self.connection = sqlite3.connect(db_path)
            self.connection.row_factory = sqlite3.Row
            
        elif db_type == 'postgresql':
            # PostgreSQL setup would go here
            raise NotImplementedError("PostgreSQL support not implemented yet")
        
        self._create_tables()
    
    def _create_tables(self):
        """Create necessary database tables."""
        cursor = self.connection.cursor()
        
        # Raw data table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS raw_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data TEXT NOT NULL,
                metadata TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Processed data table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_data_id INTEGER,
                data TEXT NOT NULL,
                metadata TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (raw_data_id) REFERENCES raw_data (id)
            )
        ''')
        
        # Jobs table for easier querying
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT UNIQUE,
                title TEXT,
                company TEXT,
                location TEXT,
                salary_min INTEGER,
                salary_max INTEGER,
                salary_currency TEXT,
                description TEXT,
                url TEXT,
                posted_date TEXT,
                job_type TEXT,
                remote BOOLEAN,
                processed_data_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (processed_data_id) REFERENCES processed_data (id)
            )
        ''')
        
        # Companies table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id TEXT UNIQUE,
                name TEXT,
                description TEXT,
                website TEXT,
                size TEXT,
                industry TEXT,
                location TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.connection.commit()
        logger.info("Database tables created successfully")
    
    def store_raw_data(self, data: Dict) -> int:
        """
        Store raw API data.
        
        Args:
            data: Raw data dictionary
            
        Returns:
            int: ID of the stored record
        """
        try:
            cursor = self.connection.cursor()
            
            cursor.execute('''
                INSERT INTO raw_data (data, metadata)
                VALUES (?, ?)
            ''', (
                json.dumps(data),
                json.dumps(data.get('metadata', {}))
            ))
            
            record_id = cursor.lastrowid
            self.connection.commit()
            
            logger.info(f"Stored raw data with ID: {record_id}")
            return record_id
            
        except Exception as e:
            logger.error(f"Error storing raw data: {str(e)}")
            raise
    
    def store_processed_data(self, data: Dict) -> int:
        """
        Store processed data.
        
        Args:
            data: Processed data dictionary
            
        Returns:
            int: ID of the stored record
        """
        try:
            cursor = self.connection.cursor()
            
            cursor.execute('''
                INSERT INTO processed_data (raw_data_id, data, metadata)
                VALUES (?, ?, ?)
            ''', (
                data.get('raw_data_id'),
                json.dumps(data),
                json.dumps(data.get('metadata', {}))
            ))
            
            record_id = cursor.lastrowid
            data['id'] = record_id
            
            # Store individual jobs
            self._store_jobs(data.get('jobs', []), record_id)
            
            self.connection.commit()
            
            logger.info(f"Stored processed data with ID: {record_id}")
            return record_id
            
        except Exception as e:
            logger.error(f"Error storing processed data: {str(e)}")
            raise
    
    def _store_jobs(self, jobs: List[Dict], processed_data_id: int):
        """Store individual job records."""
        cursor = self.connection.cursor()
        
        for job in jobs:
            salary = job.get('salary', {})
            
            cursor.execute('''
                INSERT OR REPLACE INTO jobs (
                    job_id, title, company, location, salary_min, salary_max,
                    salary_currency, description, url, posted_date, job_type,
                    remote, processed_data_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                job.get('id'),
                job.get('title'),
                job.get('company'),
                job.get('location'),
                salary.get('min') if salary else None,
                salary.get('max') if salary else None,
                salary.get('currency') if salary else None,
                job.get('description'),
                job.get('url'),
                job.get('posted_date'),
                job.get('job_type'),
                job.get('remote', False),
                processed_data_id
            ))
    
    def get_raw_data(self, data_id: int) -> Optional[Dict]:
        """Retrieve raw data by ID."""
        try:
            cursor = self.connection.cursor()
            cursor.execute('SELECT data FROM raw_data WHERE id = ?', (data_id,))
            row = cursor.fetchone()
            
            if row:
                return json.loads(row[0])
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving raw data: {str(e)}")
            return None
    
    def get_processed_data(self, data_id: int) -> Optional[Dict]:
        """Retrieve processed data by ID."""
        try:
            cursor = self.connection.cursor()
            cursor.execute('SELECT data FROM processed_data WHERE id = ?', (data_id,))
            row = cursor.fetchone()
            
            if row:
                return json.loads(row[0])
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving processed data: {str(e)}")
            return None
    
    def get_processed_data_by_date(self, date: str) -> Optional[Dict]:
        """Get processed data for a specific date."""
        try:
            cursor = self.connection.cursor()
            cursor.execute('''
                SELECT data FROM processed_data 
                WHERE DATE(created_at) = ?
                ORDER BY created_at DESC
                LIMIT 1
            ''', (date,))
            row = cursor.fetchone()
            
            if row:
                return json.loads(row[0])
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving data by date: {str(e)}")
            return None
    
    def get_processed_data_by_date_range(self, start_date: str, end_date: str) -> List[Dict]:
        """Get processed data for a date range."""
        try:
            cursor = self.connection.cursor()
            cursor.execute('''
                SELECT data FROM processed_data 
                WHERE DATE(created_at) BETWEEN ? AND ?
                ORDER BY created_at ASC
            ''', (start_date, end_date))
            
            rows = cursor.fetchall()
            return [json.loads(row[0]) for row in rows]
            
        except Exception as e:
            logger.error(f"Error retrieving data by date range: {str(e)}")
            return []
    
    def search_jobs(self, filters: Dict = None) -> List[Dict]:
        """Search jobs with optional filters."""
        try:
            cursor = self.connection.cursor()
            
            query = "SELECT * FROM jobs WHERE 1=1"
            params = []
            
            if filters:
                if 'company' in filters:
                    query += " AND company LIKE ?"
                    params.append(f"%{filters['company']}%")
                
                if 'location' in filters:
                    query += " AND location LIKE ?"
                    params.append(f"%{filters['location']}%")
                
                if 'min_salary' in filters:
                    query += " AND salary_min >= ?"
                    params.append(filters['min_salary'])
                
                if 'remote' in filters:
                    query += " AND remote = ?"
                    params.append(filters['remote'])
            
            query += " ORDER BY created_at DESC"
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Error searching jobs: {str(e)}")
            return []
    
    def close(self):
        """Close database connection."""
        if hasattr(self, 'connection'):
            self.connection.close()
