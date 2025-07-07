# test.py
import asyncio
from app.services.dj_api import DowJonesAPIService

async def test_screening():
    try:
        service = DowJonesAPIService()
        
        payload = {
            "data": {
                "attributes": {
                    "case_info": {
                        "associations": [
                            {
                                "names": [{"single_string_name": "GROUPE AL-KAMEL", "name_type": "SSN"}],
                                "record_type": "UNKNOWN"
                            },
                            {
                                "names": [{"single_string_name": "SAKPP", "name_type": "SSN"}],
                                "record_type": "UNKNOWN"
                            }
                        ],
                        "case_name": "test_case",
                        "external_id": "test_123",
                        "owner_id": "DJ",
                        "has_alerts": True,
                        "options": {
                            "filter_content_category": ["WL"],
                            "has_to_match_low_quality_alias": False,
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
        
        result = await service.create_screening_case(payload)
        print("Screening case created:", result)
        
        if result.get("data"):
            case_id = result["data"]["attributes"]["case_id"]
            print("\nCase ID:", case_id)
            
            # Try getting matches with retries
            max_retries = 3
            delay = 10  # seconds
            for attempt in range(max_retries):
                print(f"\nAttempt {attempt + 1} to get matches...")
                matches = await service.get_case_matches(case_id)
                
                if 'errors' in matches and any(e.get('status') == 202 for e in matches.get('errors', [])):
                    if attempt < max_retries - 1:
                        print(f"Matches still processing. Waiting {delay} seconds...")
                        await asyncio.sleep(delay)
                        continue
                
                print("\nMatches:", matches)
                break
                
    except Exception as e:
        print("Error:", str(e))

asyncio.run(test_screening())