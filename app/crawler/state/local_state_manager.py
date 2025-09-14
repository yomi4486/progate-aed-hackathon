"""
Local State Manager for distributed crawler.

File-based state management implementation for local development that provides
the same interface as URLStateManager without requiring DynamoDB.
"""

import asyncio
import json
import logging
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel

from ..config.settings import CrawlerSettings
from ..core.types import CrawlResult
from ..utils.url import generate_url_hash, extract_domain
from .models import URLStateEnum

logger = logging.getLogger(__name__)


class LocalURLState(BaseModel):
    """Local URL state model"""
    
    url_hash: str
    url: str
    domain: str
    state: str
    ttl: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    
    # Crawler lock fields
    crawler_id: Optional[str] = None
    acquired_at: Optional[datetime] = None
    
    # Crawl result fields
    last_crawled: Optional[datetime] = None
    s3_key: Optional[str] = None
    error_message: Optional[str] = None
    retries: int = 0


class StateTransitionError(Exception):
    """Exception raised when an invalid state transition is attempted"""
    pass


class LocalStateManager:
    """
    Local file-based state manager for URL lifecycle management.
    
    Provides the same interface as URLStateManager but stores state
    in local JSON files for development and testing.
    """
    
    def __init__(self, crawler_id: str, settings: CrawlerSettings):
        self.crawler_id = crawler_id
        self.settings = settings
        
        # State file paths
        self.state_file = settings.local_state_file
        self.backup_file = settings.local_state_file.parent / "url_states_backup.json"
        
        # Ensure state directory exists
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Thread lock for file operations
        self._lock = threading.Lock()
        
        # Statistics tracking
        self.stats = {
            "states_updated": 0,
            "retries_scheduled": 0,
            "batch_operations": 0,
            "pending_urls_fetched": 0,
            "errors_encountered": 0,
        }
        
        logger.info(f"Initialized Local State Manager for crawler {crawler_id}")
    
    def _read_states(self) -> Dict[str, LocalURLState]:
        """Read all URL states from file"""
        try:
            if not self.state_file.exists():
                return {}
            
            with open(self.state_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return {
                    url_hash: LocalURLState(**state_data) 
                    for url_hash, state_data in data.items()
                }
        except (json.JSONDecodeError, Exception) as e:
            logger.error(f"Error reading state file: {e}")
            return {}
    
    def _write_states(self, states: Dict[str, LocalURLState]):
        """Write all URL states to file"""
        try:
            # Create backup before writing
            if self.state_file.exists():
                import shutil
                shutil.copy2(self.state_file, self.backup_file)
            
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(
                    {
                        url_hash: state.model_dump(mode='json') 
                        for url_hash, state in states.items()
                    },
                    f,
                    indent=2,
                    default=str
                )
        except Exception as e:
            logger.error(f"Error writing state file: {e}")
            raise
    
    async def add_url(
        self, 
        url: str, 
        domain: Optional[str] = None, 
        initial_state: URLStateEnum = URLStateEnum.PENDING
    ) -> str:
        """
        Add a new URL to the system.
        
        Args:
            url: URL to add
            domain: Domain (extracted from URL if not provided)  
            initial_state: Initial state for the URL
            
        Returns:
            URL hash of the added URL
        """
        url_hash = generate_url_hash(url)
        
        if domain is None:
            domain = extract_domain(url)
        
        now = datetime.now(timezone.utc)
        
        try:
            with self._lock:
                states = self._read_states()
                
                # Check if URL already exists
                if url_hash in states:
                    logger.debug(f"URL {url} already exists with state {states[url_hash].state}")
                    return url_hash
                
                # Create new URL state
                states[url_hash] = LocalURLState(
                    url_hash=url_hash,
                    url=url,
                    domain=domain,
                    state=initial_state.value,
                    ttl=now + timedelta(seconds=self.settings.acquisition_ttl_seconds),
                    created_at=now,
                    updated_at=now
                )
                
                self._write_states(states)
            
            logger.info(f"Added new URL {url} with state {initial_state.value}")
            return url_hash
            
        except Exception as e:
            self.stats["errors_encountered"] += 1
            raise StateTransitionError(f"Failed to add URL {url}: {e}") from e
    
    async def update_state(
        self,
        url_hash: str,
        new_state: URLStateEnum,
        crawler_id: Optional[str] = None,
        result: Optional[CrawlResult] = None,
        error: Optional[str] = None,
        retries: Optional[int] = None,
    ) -> bool:
        """
        Update URL state with optional crawl result data.
        
        Args:
            url_hash: Hash of URL to update
            new_state: New state to set
            crawler_id: Crawler ID (defaults to this manager's crawler_id)
            result: Optional crawl result data
            error: Optional error message
            retries: Optional retry count
            
        Returns:
            True if state was updated successfully
        """
        if crawler_id is None:
            crawler_id = self.crawler_id
        
        now = datetime.now(timezone.utc)
        
        try:
            with self._lock:
                states = self._read_states()
                
                if url_hash not in states:
                    raise StateTransitionError(f"URL {url_hash} not found")
                
                current_state = states[url_hash]
                
                # Validate state transition
                self._validate_state_transition(current_state.state, new_state.value, crawler_id)
                
                # Update basic fields
                current_state.state = new_state.value
                current_state.updated_at = now
                
                # Update fields based on new state
                if new_state == URLStateEnum.IN_PROGRESS:
                    current_state.crawler_id = crawler_id
                    current_state.acquired_at = now
                    current_state.ttl = now + timedelta(seconds=self.settings.acquisition_ttl_seconds)
                
                elif new_state == URLStateEnum.DONE:
                    current_state.last_crawled = now
                    if result and result.html_s3_key:
                        current_state.s3_key = result.html_s3_key
                    # Clear lock-related fields
                    current_state.crawler_id = None
                    current_state.acquired_at = None
                
                elif new_state == URLStateEnum.FAILED:
                    if error:
                        current_state.error_message = error
                    if retries is not None:
                        current_state.retries = retries
                    # Clear lock-related fields
                    current_state.crawler_id = None
                    current_state.acquired_at = None
                
                elif new_state == URLStateEnum.PENDING:
                    # Clear all processing-related fields
                    current_state.crawler_id = None
                    current_state.acquired_at = None
                    current_state.error_message = None
                
                # Save updated states
                self._write_states(states)
            
            self.stats["states_updated"] += 1
            logger.info(f"Updated URL {url_hash} state to {new_state.value}")
            return True
            
        except Exception as e:
            self.stats["errors_encountered"] += 1
            raise StateTransitionError(f"Failed to update URL {url_hash} state: {e}") from e
    
    def _validate_state_transition(self, current_state: str, new_state: str, crawler_id: str) -> None:
        """
        Validate that a state transition is allowed.
        
        Args:
            current_state: Current state
            new_state: Requested new state  
            crawler_id: Crawler requesting the transition
        """
        # Define valid state transitions
        valid_transitions = {
            URLStateEnum.PENDING.value: [URLStateEnum.IN_PROGRESS.value],
            URLStateEnum.IN_PROGRESS.value: [
                URLStateEnum.DONE.value,
                URLStateEnum.FAILED.value,
                URLStateEnum.PENDING.value,  # For timeout recovery
            ],
            URLStateEnum.FAILED.value: [
                URLStateEnum.PENDING.value,  # For retry
                URLStateEnum.IN_PROGRESS.value,  # Direct retry
            ],
            URLStateEnum.DONE.value: [
                URLStateEnum.PENDING.value  # For re-crawling
            ],
        }
        
        if new_state not in valid_transitions.get(current_state, []):
            raise StateTransitionError(f"Invalid state transition from {current_state} to {new_state}")
    
    async def schedule_retry(
        self, 
        url_hash: str, 
        delay_seconds: int, 
        max_retries: Optional[int] = None,
        error_message: Optional[str] = None
    ) -> bool:
        """
        Schedule a URL for retry after a specified delay.
        
        Args:
            url_hash: Hash of URL to retry
            delay_seconds: Delay before retry becomes available
            max_retries: Maximum retry count (defaults to settings)
            error_message: Optional error message
            
        Returns:
            True if retry was scheduled, False if max retries exceeded
        """
        if max_retries is None:
            max_retries = self.settings.max_retries
        
        try:
            with self._lock:
                states = self._read_states()
                
                if url_hash not in states:
                    logger.error(f"Cannot schedule retry for non-existent URL {url_hash}")
                    return False
                
                current_state = states[url_hash]
                
                # Check retry limit
                if current_state.retries >= max_retries:
                    logger.info(f"URL {url_hash} has exceeded max retries ({max_retries})")
                    return False
                
                # Calculate retry time
                now = datetime.now(timezone.utc)
                retry_time = now + timedelta(seconds=delay_seconds)
                
                # Update state to failed with retry scheduling
                current_state.state = URLStateEnum.FAILED.value
                current_state.retries += 1
                current_state.ttl = retry_time  # TTL acts as retry delay
                current_state.updated_at = now
                
                if error_message:
                    current_state.error_message = error_message
                
                # Clear lock fields
                current_state.crawler_id = None
                current_state.acquired_at = None
                
                # Save updated states
                self._write_states(states)
            
            self.stats["retries_scheduled"] += 1
            logger.info(
                f"Scheduled retry for URL {url_hash} in {delay_seconds}s "
                f"(attempt {current_state.retries}/{max_retries})"
            )
            return True
            
        except Exception as e:
            self.stats["errors_encountered"] += 1
            logger.error(f"Failed to schedule retry for URL {url_hash}: {e}")
            return False
    
    async def get_pending_urls_for_domain(
        self,
        domain: str,
        limit: int = 100,
        exclude_recently_failed: bool = True
    ) -> List[str]:
        """
        Get pending URLs for a specific domain.
        
        Args:
            domain: Domain to query
            limit: Maximum number of URLs to return
            exclude_recently_failed: Whether to exclude recently failed URLs
            
        Returns:
            List of URL hashes ready for crawling
        """
        try:
            now = datetime.now(timezone.utc)
            url_hashes = []
            
            with self._lock:
                states = self._read_states()
                
                for url_hash, state in states.items():
                    # Filter by domain and pending state
                    if state.domain != domain or state.state != URLStateEnum.PENDING.value:
                        continue
                    
                    # Exclude recently failed URLs if requested
                    if exclude_recently_failed and state.ttl and state.ttl > now:
                        continue
                    
                    url_hashes.append(url_hash)
                    
                    # Limit results
                    if len(url_hashes) >= limit:
                        break
            
            self.stats["pending_urls_fetched"] += len(url_hashes)
            logger.debug(f"Retrieved {len(url_hashes)} pending URLs for domain {domain}")
            
            return url_hashes
            
        except Exception as e:
            self.stats["errors_encountered"] += 1
            logger.error(f"Failed to get pending URLs for domain {domain}: {e}")
            return []
    
    async def get_urls_by_state(
        self,
        state: URLStateEnum,
        limit: int = 100,
        domain: Optional[str] = None
    ) -> List[LocalURLState]:
        """
        Get URLs by state, optionally filtered by domain.
        
        Args:
            state: State to filter by
            limit: Maximum number of URLs to return
            domain: Optional domain filter
            
        Returns:
            List of LocalURLState instances
        """
        try:
            results = []
            
            with self._lock:
                states = self._read_states()
                
                for url_hash, url_state in states.items():
                    # Filter by state
                    if url_state.state != state.value:
                        continue
                    
                    # Filter by domain if specified
                    if domain and url_state.domain != domain:
                        continue
                    
                    results.append(url_state)
                    
                    # Limit results
                    if len(results) >= limit:
                        break
            
            logger.debug(f"Retrieved {len(results)} URLs with state {state.value}")
            return results
            
        except Exception as e:
            self.stats["errors_encountered"] += 1
            logger.error(f"Failed to get URLs by state {state.value}: {e}")
            return []
    
    async def batch_add_urls(
        self, 
        urls: List[str], 
        initial_state: URLStateEnum = URLStateEnum.PENDING
    ) -> List[str]:
        """
        Add multiple URLs in batch for better performance.
        
        Args:
            urls: List of URLs to add
            initial_state: Initial state for all URLs
            
        Returns:
            List of URL hashes for successfully added URLs
        """
        if not urls:
            return []
        
        now = datetime.now(timezone.utc)
        ttl_timestamp = now + timedelta(seconds=self.settings.acquisition_ttl_seconds)
        
        url_hashes = []
        
        try:
            with self._lock:
                states = self._read_states()
                
                for url in urls:
                    try:
                        url_hash = generate_url_hash(url)
                        domain = extract_domain(url)
                        
                        # Skip if URL already exists
                        if url_hash in states:
                            logger.debug(f"URL {url} already exists, skipping")
                            continue
                        
                        # Add new state
                        states[url_hash] = LocalURLState(
                            url_hash=url_hash,
                            url=url,
                            domain=domain,
                            state=initial_state.value,
                            ttl=ttl_timestamp,
                            created_at=now,
                            updated_at=now
                        )
                        
                        url_hashes.append(url_hash)
                        
                    except Exception as e:
                        logger.error(f"Error preparing URL {url} for batch insert: {e}")
                        continue
                
                # Save all states
                if url_hashes:
                    self._write_states(states)
            
            self.stats["batch_operations"] += 1
            logger.info(f"Batch added {len(url_hashes)} URLs")
            
            return url_hashes
            
        except Exception as e:
            self.stats["errors_encountered"] += 1
            logger.error(f"Batch add URLs failed: {e}")
            return []
    
    async def get_domain_statistics(self, domain: str) -> Dict[str, int]:
        """
        Get statistics for a specific domain.
        
        Args:
            domain: Domain to get statistics for
            
        Returns:
            Dictionary with state counts
        """
        stats = {state.value: 0 for state in URLStateEnum}
        
        try:
            with self._lock:
                states = self._read_states()
                
                for url_hash, state in states.items():
                    if state.domain == domain:
                        stats[state.state] += 1
            
            logger.debug(f"Retrieved statistics for domain {domain}: {stats}")
            return stats
            
        except Exception as e:
            self.stats["errors_encountered"] += 1
            logger.error(f"Failed to get domain statistics for {domain}: {e}")
            return stats
    
    async def get_url_state(self, url: str) -> Optional[LocalURLState]:
        """
        Get URL state for a single URL.
        
        Args:
            url: URL to check
            
        Returns:
            LocalURLState if exists, None otherwise
        """
        try:
            url_hash = generate_url_hash(url)
            
            with self._lock:
                states = self._read_states()
                return states.get(url_hash)
                
        except Exception as e:
            logger.error(f"Error getting URL state for {url}: {e}")
            return None
    
    async def batch_get_url_states(self, urls: List[str]) -> Dict[str, LocalURLState]:
        """
        Get URL states for multiple URLs in batch.
        
        Args:
            urls: List of URLs to check
            
        Returns:
            Dictionary mapping url_hash to LocalURLState for existing URLs
        """
        if not urls:
            return {}
        
        try:
            result = {}
            
            with self._lock:
                states = self._read_states()
                
                for url in urls:
                    url_hash = generate_url_hash(url)
                    if url_hash in states:
                        result[url_hash] = states[url_hash]
            
            logger.debug(f"Batch fetched states for {len(result)}/{len(urls)} URLs")
            return result
            
        except Exception as e:
            logger.error(f"Error batch getting URL states: {e}")
            return {}
    
    def get_stats(self) -> Dict[str, Any]:
        """Get URL state manager statistics"""
        return {
            **self.stats,
            "crawler_id": self.crawler_id,
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on state manager.
        
        Returns:
            Health status information
        """
        try:
            # Test basic file operations
            with self._lock:
                states = self._read_states()
                state_count = len(states)
            
            # Test domain statistics
            await self.get_domain_statistics("example.com")
            
            return {
                "status": "healthy",
                "total_states": state_count,
                "operations_completed": sum(self.stats.values()),
                "error_rate": (self.stats["errors_encountered"] / max(1, sum(self.stats.values()))),
                "state_file_accessible": self.state_file.exists(),
                "backup_file_accessible": self.backup_file.exists() if self.backup_file else False,
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
            }