"""
IndexNow Integration Module

Environment Variables Required:
- INDEXNOW_KEY: A random hex string (32-64 characters) that must match the filename
  served at /{key}.txt for verification by IndexNow-participating search engines.
  
  Example: If INDEXNOW_KEY = "abc123def456", the application must serve GET /abc123def456.txt
  returning the raw key value as plain text.
  
  In production, set this environment variable on your hosting platform (e.g., Render).
  The fallback value in code is for development only and should be replaced.
"""

import logging
import os
from urllib.parse import urlparse
import httpx

# Module logger
logger = logging.getLogger("indexnow")

# IndexNow API endpoint
INDEXNOW_ENDPOINT = "https://api.indexnow.org/indexnow"


async def submit_urls_to_indexnow(urls: list[str]) -> dict:
    """
    Submit URLs to IndexNow API for search engine indexing.
    
    Args:
        urls: List of URLs to submit for indexing
        
    Returns:
        dict: {"success": bool, "status_code": int|None, "error": str|None}
    """
    # Deduplicate URLs while preserving order
    unique_urls = list(dict.fromkeys(urls))
    
    # No-op if list is empty
    if not unique_urls:
        return {"success": True, "status_code": None, "error": None, "skipped": True}
    
    # Read IndexNow key from environment
    key = os.getenv("INDEXNOW_KEY")
    if not key:
        logger.warning("INDEXNOW_KEY environment variable not set, skipping submission")
        return {"success": False, "status_code": None, "error": "INDEXNOW_KEY not set"}
    
    # Extract host from first URL
    try:
        parsed = urlparse(unique_urls[0])
        host = parsed.netloc
    except Exception as e:
        logger.error(f"Failed to parse URL '{unique_urls[0]}': {e}")
        return {"success": False, "status_code": None, "error": f"Invalid URL: {e}"}
    
    # Build payload
    payload = {
        "host": host,
        "key": key,
        "keyLocation": f"https://{host}/{key}.txt",
        "urlList": unique_urls
    }
    
    # Submit with one retry on failure
    for attempt in range(2):
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(
                    INDEXNOW_ENDPOINT,
                    json=payload,
                    headers={"Content-Type": "application/json"}
                )
                
                if response.status_code == 200:
                    logger.info(f"Successfully submitted {len(unique_urls)} URLs to IndexNow for host: {host}")
                    return {"success": True, "status_code": response.status_code, "error": None}
                else:
                    logger.warning(f"IndexNow submission failed with status {response.status_code}: {response.text}")
                    return {"success": False, "status_code": response.status_code, "error": response.text}
                    
        except httpx.TimeoutException as e:
            if attempt == 0:
                logger.warning(f"IndexNow submission timed out, retrying...")
                continue
            logger.error(f"IndexNow submission timed out after retry: {e}")
            return {"success": False, "status_code": None, "error": f"Timeout: {e}"}
            
        except httpx.RequestError as e:
            if attempt == 0:
                logger.warning(f"IndexNow submission request failed, retrying...")
                continue
            logger.error(f"IndexNow submission request failed after retry: {e}")
            return {"success": False, "status_code": None, "error": f"Request error: {e}"}
            
        except Exception as e:
            logger.error(f"Unexpected error in IndexNow submission: {e}")
            return {"success": False, "status_code": None, "error": f"Unexpected error: {e}"}
    
    return {"success": False, "status_code": None, "error": "Max retries exceeded"}
