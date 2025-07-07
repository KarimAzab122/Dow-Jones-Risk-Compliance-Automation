# app/auth/service.py
import httpx
from app.config import settings
from typing import Optional, List, Dict, Any
import logging
import time
import asyncio
from app.api.models import (BulkScreeningRequest)


logger = logging.getLogger(__name__)

class DJAuthService:
    def __init__(self):
        self.auth_url = f"https://{settings.dj_auth_url}"
        self.client_id = settings.dj_client_id
        self.username = settings.dj_username
        self.password = settings.dj_password
        self.authn_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.jwt_bearer: Optional[str] = None
        self.token_expiry: Optional[int] = None

    async def _make_auth_request(self, url: str, payload: dict) -> dict:
        """Generic method to make authentication requests"""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(url, json=payload, headers=headers)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"Auth request failed: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during auth request: {str(e)}")
            raise

    async def get_authn_token(self) -> str:
        """Step 1: Retrieve AuthN Token"""
        url = f"{self.auth_url}/oauth2/v1/token"
        payload = {
            "client_id": self.client_id,
            "username": self.username,
            "password": self.password,
            "connection": "service-account",
            "device": "mac",
            "grant_type": "password",
            "scope": "openid service_account_id offline_access"
        }

        data = await self._make_auth_request(url, payload)
        
        self.authn_token = data.get("id_token")
        self.refresh_token = data.get("refresh_token")
        
        if not self.authn_token:
            raise ValueError("Failed to retrieve AuthN token")
        
        return self.authn_token

    async def get_jwt_bearer(self) -> str:
        """Step 2: Retrieve AuthZ Token"""
        if not self.authn_token:
            await self.get_authn_token()

        url = f"{self.auth_url}/oauth2/v1/token"
        payload = {
            "assertion": self.authn_token,
            "client_id": self.client_id,
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "scope": "openid pib"
        }

        data = await self._make_auth_request(url, payload)
        
        token_type = data.get("token_type")
        access_token = data.get("access_token")
        expires_in = data.get("expires_in", 3600)  # Default to 1 hour if not provided
        
        if not token_type or not access_token:
            raise ValueError("Failed to retrieve JWT Bearer token")
        
        self.jwt_bearer = f"{token_type} {access_token}"
        self.token_expiry = int(time.time()) + expires_in - 300  # Set expiry with 5-minute buffer
        
        return self.jwt_bearer

    async def refresh_authn_token(self) -> str:
        """Refresh AuthN Token using refresh token"""
        if not self.refresh_token:
            await self.get_authn_token()

        url = f"{self.auth_url}/oauth2/v1/token"
        payload = {
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "grant_type": "refresh_token",
            "scope": "openid service_account_id"
        }

        data = await self._make_auth_request(url, payload)
        
        token_type = data.get("token_type")
        access_token = data.get("access_token")
        expires_in = data.get("expires_in", 3600)
        
        if not token_type or not access_token:
            raise ValueError("Failed to refresh token")
        
        self.jwt_bearer = f"{token_type} {access_token}"
        self.token_expiry = int(time.time()) + expires_in - 300
        
        return self.jwt_bearer

    async def get_valid_token(self) -> str:
        """Get a valid JWT token, refreshing if needed"""
        # If we have a token and it's not expired, return it
        if self.jwt_bearer and self.token_expiry and time.time() < self.token_expiry:
            return self.jwt_bearer
        
        try:
            # Try to get a new token
            return await self.get_jwt_bearer()
        except Exception as e:
            logger.warning(f"Initial token fetch failed, trying refresh: {str(e)}")
            try:
                # If initial token fails, try refreshing
                return await self.refresh_authn_token()
            except Exception as refresh_error:
                logger.error(f"Token refresh failed: {str(refresh_error)}")
                # If refresh fails, try full reauthentication
                self.authn_token = None
                self.refresh_token = None
                return await self.get_jwt_bearer()
    async def _get_screening_headers(self) -> Dict[str, str]:
        """Get headers specific for screening API"""
        token = await self.auth_service.get_valid_token()
        return {
            "Authorization": token,
            "Accept": self.screening_api_version,
            "Content-Type": "application/json"
        }

    async def create_screening_case(self, request: BulkScreeningRequest, details: bool = True) -> Dict[str, Any]:
        """Create a new screening case with associations"""
        endpoint = "/risk-entity-screening-cases/bulk-associations"
        if details:
            endpoint += "?details=true"
        
        payload = {
            "data": {
                "attributes": request.dict(),
                "type": "risk-entity-screening-cases/bulk-associations"
            }
        }
        
        headers = await self._get_screening_headers()
        return await self._make_api_request("POST", endpoint, payload, headers)


    async def get_case_matches(self, case_id: str, max_retries: int = 5, delay: int = 5) -> Dict[str, Any]:
        """Get matches for a specific case with retry logic"""
        endpoint = f"/risk-entity-screening-cases/{case_id}/matches"
        params = {
            "filter[has_alerts]": "true",
            "filter[is_match_valid]": "true"
        }
        
        headers = await self._get_headers()
        
        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.get(
                        f"{self.api_host}{endpoint}",
                        params=params,
                        headers=headers
                    )
                    
                    # If still processing, wait and retry
                    if response.status_code == 202:
                        if attempt < max_retries - 1:
                            await asyncio.sleep(delay * (attempt + 1)) 
                            continue
                    
                    response.raise_for_status()
                    return response.json()
                    
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 202 and attempt < max_retries - 1:
                    await asyncio.sleep(delay * (attempt + 1))
                    continue
                error_msg = f"API request failed: {e.response.status_code} - {e.response.text}"
                logger.error(error_msg)
                raise
            except Exception as e:
                error_msg = f"Unexpected error during API request: {str(e)}"
                logger.error(error_msg)
                raise
        
        return {"errors": [{"status": 202, "detail": "Max retries reached, matches still processing"}]}
    











    async def get_case_by_id(self, case_id: str) -> Dict[str, Any]:
        """Get a specific case by ID"""
        endpoint = f"/risk-entity-screening-cases/{case_id}"
        headers = await self._get_screening_headers()
        return await self._make_api_request("GET", endpoint, headers=headers)

    async def get_all_cases(self, offset: int = 0, limit: int = 100) -> Dict[str, Any]:
        """Get all screening cases"""
        endpoint = "/risk-entity-screening-cases"
        params = {
            "page[offset]": offset,
            "page[limit]": limit
        }
        headers = await self._get_screening_headers()
        return await self._make_api_request("GET", endpoint, headers=headers, params=params)
    