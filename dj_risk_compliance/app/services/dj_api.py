# app/services/dj_api.py
import httpx
import time
from typing import Optional, Dict, Any, List
from app.config import settings
from app.auth.service import DJAuthService
import asyncio
from fastapi import HTTPException
import logging
from app.api.models import (BulkScreeningRequest)


logger = logging.getLogger(__name__)

class DowJonesAPIService:
    def __init__(self):
        self.api_host = f"https://{settings.dj_api_host}"
        self.auth_service = DJAuthService()
        self.content_type = settings.content_type
        self.last_request_time = 0
        self.min_request_interval = 0.1


    async def _get_headers(self) -> Dict[str, str]:
        """Get standard headers for all API requests"""
        token = await self.auth_service.get_valid_token()
        return {
            "Authorization": token,
            "Accept": self.content_type,
            "Content-Type": self.content_type
        }

    async def _make_api_request(self, method: str, endpoint: str, payload: Optional[Dict] = None) -> Dict[str, Any]:
        """Generic method to make API requests"""
        url = f"{self.api_host}{endpoint}"
        headers = await self._get_headers()
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                if method == "GET":
                    response = await client.get(url, headers=headers)
                elif method == "POST":
                    response = await client.post(url, json=payload, headers=headers)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                
                response.raise_for_status()
                return response.json()
                
        except httpx.HTTPStatusError as e:
            error_msg = f"API request failed: {e.response.status_code} - {e.response.text}"
            logger.error(error_msg)
            raise HTTPException(
                status_code=e.response.status_code,
                detail=error_msg
            )
        except Exception as e:
            error_msg = f"Unexpected error during API request: {str(e)}"
            logger.error(error_msg)
            raise HTTPException(
                status_code=500,
                detail=error_msg
            )

    async def name_search(
        self,
        name: str,
        record_types: List[str] = ["Person", "Entity"],
        content_set: List[str] = ["WatchList"],
        offset: int = 0,
        limit: int = 20,
        search_type: str = "BROAD"
    ) -> Dict[str, Any]:
        """Perform a name search"""
        payload = {
            "data": {
                "type": "RiskEntitySearch",
                "attributes": {
                    "paging": {"offset": offset, "limit": limit},
                    "sort": None,
                    "filter_group_and": {
                        "filters": {
                            "content_set": content_set,
                            "record_types": record_types,
                            "search_keyword": {
                                "scope": ["Name"],
                                "text": name,
                                "type": search_type
                            },
                            "country_territory": {
                                "country_territory_types": {
                                    "country_territory_types": [],
                                    "operator": "OR"
                                },
                                "countries_territories": {
                                    "codes": [],
                                    "exclude_codes": [],
                                    "operator": "OR"
                                }
                            }
                        },
                        "group_operator": "And"
                    },
                    "filter_group_or": self._get_default_filter_group_or()
                }
            }
        }
        
        return await self._make_api_request("POST", "/riskentities/search", payload)

    async def get_risk_profile(self, profile_id: str) -> Dict[str, Any]:
        """Retrieve a full risk profile by ID"""
        headers = {
            "Authorization": await self.auth_service.get_valid_token(),
            "Accept": self.profiles_api_version,
            "Content-Type": self.profiles_api_version,
            "cache-control": "no-cache"
        }
        
        return await self._make_api_request("GET", f"/riskentities/profiles/{profile_id}")

    def _get_default_filter_group_or(self) -> Dict[str, Any]:
        """Get the default filter group for OR conditions"""
        return {
            "filters": {
                "sanctions_list": {
                    "is_all_excluded": False,
                    "operator": "OR"
                },
                "content_category": {
                    "special_interest": {
                        "is_all_excluded": False,
                        "operator": "OR"
                    },
                    "adverse_media": {
                        "is_all_excluded": False,
                        "operator": "OR"
                    },
                    "location": {
                        "is_all_excluded": False,
                        "operator": "OR"
                    }
                },
                "other_official_list": {
                    "is_all_excluded": False,
                    "operator": "OR"
                },
                "other_exclusion_list": {
                    "is_all_excluded": False,
                    "operator": "OR"
                },
                "state_ownership": {
                    "is_all_excluded": False
                },
                "occupation_category": {
                    "is_all_excluded": False,
                    "operator": "Or"
                },
                "hrf_category": {
                    "is_all_excluded": False,
                    "operator": "OR"
                }
            },
            "group_operator": "Or"
        }
    async def _get_screening_headers(self) -> Dict[str, str]:
        """Get headers for screening API requests"""
        token = await self.auth_service.get_valid_token()
        return {
            "Authorization": token,
            "Accept": "application/vnd.dowjones.dna.bulk-associations.v_1.2+json",
            "Content-Type": "application/vnd.dowjones.dna.bulk-associations.v_1.2+json"
        }
    async def wait_for_matches(self, case_id: str, max_attempts: int = 10, delay: int = 5) -> Dict[str, Any]:
        """Wait for matches to be ready with retry logic"""
        for attempt in range(max_attempts):
            try:
                matches = await self.get_case_matches(case_id)
                
                # If matches are ready (status code 200)
                if 'data' in matches or ('errors' not in matches):
                    return matches
                    
                # If still processing (status code 202)
                if any(err.get('status') == 202 for err in matches.get('errors', [])):
                    if attempt < max_attempts - 1:
                        await asyncio.sleep(delay * (attempt + 1))  # Exponential backoff
                        continue
                        
                return matches
                
            except Exception as e:
                if attempt < max_attempts - 1:
                    await asyncio.sleep(delay * (attempt + 1))
                    continue
                raise


    async def create_screening_case(self, payload: Dict) -> Dict[str, Any]:
        """Create a new screening case with associations"""
        endpoint = "/risk-entity-screening-cases/bulk-associations?details=true"
        headers = await self._get_headers()
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{self.api_host}{endpoint}",
                    json=payload,
                    headers=headers
                )
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            error_msg = f"API request failed: {e.response.status_code} - {e.response.text}"
            logger.error(error_msg)
            raise
        except Exception as e:
            error_msg = f"Unexpected error during API request: {str(e)}"
            logger.error(error_msg)
            raise

    async def get_case_by_id(self, case_id: str) -> Dict[str, Any]:
        """Get a specific screening case by ID"""
        endpoint = f"/risk-entity-screening-cases/{case_id}"
        headers = await self._get_headers()
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    f"{self.api_host}{endpoint}",
                    headers=headers
                )
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            error_msg = f"API request failed: {e.response.status_code} - {e.response.text}"
            logger.error(error_msg)
            raise
        except Exception as e:
            error_msg = f"Unexpected error during API request: {str(e)}"
            logger.error(error_msg)
            raise
   
    async def get_transaction_details(self, case_id: str, transaction_id: str) -> Dict[str, Any]:
        """Get details for a specific transaction"""
        endpoint = f"/risk-entity-screening-cases/{case_id}/bulk-associations/{transaction_id}?details=true"
        headers = await self._get_headers()
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    f"{self.api_host}{endpoint}",
                    headers=headers
                )
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            error_msg = f"API request failed: {e.response.status_code} - {e.response.text}"
            logger.error(error_msg)
            raise
        except Exception as e:
            error_msg = f"Unexpected error during API request: {str(e)}"
            logger.error(error_msg)
            raise

    async def get_case_matches(self, case_id: str, limit: int = 5000) -> Dict[str, Any]:
        """Get matches for a specific case"""
        endpoint = f"/risk-entity-screening-cases/{case_id}/matches"
        params = {
            "filter[has_alerts]": "true",
            "filter[is_match_valid]": "true",
            "page[limit]": str(limit)
        }
        
        headers = await self._get_headers()
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    f"{self.api_host}{endpoint}",
                    params=params,
                    headers=headers
                )
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            error_msg = f"API request failed: {e.response.status_code} - {e.response.text}"
            logger.error(error_msg)
            raise
        except Exception as e:
            error_msg = f"Unexpected error during API request: {str(e)}"
            logger.error(error_msg)
            raise
