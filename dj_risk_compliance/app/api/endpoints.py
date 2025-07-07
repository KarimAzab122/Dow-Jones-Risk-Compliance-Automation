from fastapi import APIRouter, Depends, HTTPException, Query, Body
from typing import List, Optional
from app.services.dj_api import DowJonesAPIService
import httpx
import asyncio
from app.api.models import *

import logging

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/search/name")
async def name_search(request: NameSearchRequest):
    try:
        service = DowJonesAPIService()
        result = await service.name_search(
            name=request.name,
            record_types=request.record_types,
            content_set=request.content_set,
            offset=request.offset,
            limit=request.limit,
            search_type=request.search_type
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/search/person")
async def person_name_search(request: PersonNameSearchRequest):
    try:
        service = DowJonesAPIService()
        result = await service.person_name_search(
            first_name=request.first_name,
            middle_name=request.middle_name,
            last_name=request.last_name,
            content_set=request.content_set,
            offset=request.offset,
            limit=request.limit,
            search_type=request.search_type
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/search/entity")
async def entity_name_search(request: EntityNameSearchRequest):
    try:
        service = DowJonesAPIService()
        result = await service.entity_name_search(
            full_name=request.full_name,
            content_set=request.content_set,
            offset=request.offset,
            limit=request.limit,
            search_type=request.search_type
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/search/id")
async def id_search(request: IdSearchRequest):
    try:
        service = DowJonesAPIService()
        result = await service.id_search(
            id_number=request.id_number,
            id_type=request.id_type,
            record_types=request.record_types,
            content_set=request.content_set,
            offset=request.offset,
            limit=request.limit
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/profiles/{profile_id}")
async def get_risk_profile(profile_id: str):
    try:
        service = DowJonesAPIService()
        result = await service.get_risk_profile(profile_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/screening/cases")
async def create_screening_case(request: BulkScreeningRequest, details: bool = True):
    try:
        service = DowJonesAPIService()
        result = await service.create_screening_case(request, details)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@router.get("/screening/cases/{case_id}")
async def get_screening_case(case_id: str):
    try:
        service = DowJonesAPIService()
        result = await service.get_case_by_id(case_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/screening/cases")
async def get_all_screening_cases(
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000)
):
    try:
        service = DowJonesAPIService()
        result = await service.get_all_cases(offset, limit)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    



@router.post("/screening/bulk-associations")
async def create_screening_case(names: List[str] = Body(..., embed=True, example=["Name1", "Name2"])):
    wait_for_matches: bool = True,
    max_wait_seconds: int = 10
    try:
        service = DowJonesAPIService()
        
        #  API specs
        payload = {
            "data": {
                "attributes": {
                    "paging": {"offset": 0,"limit": 100},
                    "case_info": {
                        "associations": [
                            {
                                "names": [{"single_string_name": name, "name_type": "PRIMARY"}],
                                "record_type": "UNKNOWN"
                            } for name in names
                        ],
                        "case_name": "screening_case",
                        "external_id": "external_id_123",
                        "owner_id": "DJ",
                        "has_alerts": False,
                        "options": {
                            "filter_content_category": ["WL"],
                            "has_to_match_low_quality_alias": True,
                            "is_indexed": True,
                            "search_type": "BROAD"
                        },
                        "score_preferences": {
                            "country": {"has_exclusions": False, "score": 0},
                            "gender": {"has_exclusions": False, "score": 0},
                            "identification_details": {"has_exclusions": False, "score": 0},
                            "industry_sector": {"has_exclusions": False, "score": 0},
                            "year_of_birth": {"has_exclusions": False, "score": 0}
                        }
                    }
                },
                "type": "risk-entity-screening-cases/bulk-associations"
            }
        }
        
        # Create the screening case
        creation_result = await service.create_screening_case(payload)
        
        # Get the case ID from the response
        case_id = creation_result["data"]["attributes"]["case_id"]
        transaction_id = creation_result["data"]["id"]
        transaction_details = await service.get_transaction_details(case_id, transaction_id)
        # Get matches for this case
        # matches_result = await service.get_case_matches(case_id)
        response_data = {
            "case_creation": creation_result,
            "transaction_details": transaction_details,
            "case_id": case_id,
            "transaction_id": transaction_id,
            "status": "completed" if not wait_for_matches else "processing"
        }
        if wait_for_matches:
            matches = await service.wait_for_matches(case_id, max_wait_seconds)
            response_data["matches"] = matches
            response_data.update({
                "matches": matches,
                "status": "completed" if matches and "errors" not in matches else "processing"
            })
        
        return response_data
       
        
    except httpx.HTTPStatusError as e:
        error_detail = f"Dow Jones API error: {e.response.status_code} - {e.response.text}"
        raise HTTPException(
            status_code=e.response.status_code,
            detail=error_detail
        )
    except Exception as e:
        error_detail = f"Unexpected error: {str(e)}"
        raise HTTPException(
            status_code=500,
            detail=error_detail
        )
    



@router.get("/screening/cases/{case_id}/transactions/{transaction_id}")
async def get_transaction_details(case_id: str, transaction_id: str):
    try:
        service = DowJonesAPIService()
        return await service.get_transaction_details(case_id, transaction_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@router.get("/screening/cases/{case_id}/matches")
async def get_case_matches(case_id: str):
    try:
        service = DowJonesAPIService()
        return await service.get_case_matches(case_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        