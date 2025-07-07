from pydantic import BaseModel, Field
from typing import Optional, List, Dict

class ScreeningName(BaseModel):

    single_string_name: str
    name_type: str = Field(default="SSN", const=True)

class ScreeningAssociation(BaseModel):
    names: List[ScreeningName]
    record_type: str = Field(default="UNKNOWN", const=True)


class ScreeningCaseOptions(BaseModel):
    filter_content_category: List[str] = Field(default=["WL"])
    has_to_match_low_quality_alias: bool = True
    is_indexed: bool = True
    search_type: str = "BROAD"

class ScorePreference(BaseModel):

    has_exclusions: bool = False
    score: int = 0


class ScreeningCaseRequest(BaseModel):
    associations: List[ScreeningAssociation]
    case_name: str = "screening_case"
    external_id: str = "external_id_113"
    owner_id: str = "DJ"
    has_alerts: bool = False
    options: ScreeningCaseOptions = Field(default_factory=ScreeningCaseOptions)
    score_preferences: dict = Field(default_factory=lambda: {
        "country": ScorePreference(),
        "gender": ScorePreference(),
        "identification_details": ScorePreference(),
        "industry_sector": ScorePreference(),
        "year_of_birth": ScorePreference()
    })


class BulkScreeningRequest(BaseModel):
    # case_info: ScreeningCaseRequest
     data: dict

class CaseIdRequest(BaseModel):
    case_id: str

class AssociationIdRequest(BaseModel):
    association_id: str

class MatchFilters(BaseModel):
    has_alerts: bool = False
    is_match_valid: bool = True
    last_match_activity_on: Optional[Dict[str, str]] = None  # {"from": "2021-01-01T00:00:00Z", "to": "2021-01-12T00:00:00Z"}
class NameSearchRequest(BaseModel):
    name: str
    record_types: List[str] = ["Person", "Entity"]
    content_set: List[str] = ["WatchList"]
    offset: int = 0
    limit: int = 20
    search_type: str = "BROAD"


class PersonNameSearchRequest(BaseModel):
    first_name: Optional[str] = ""
    middle_name: Optional[str] = ""
    last_name: Optional[str] = ""
    content_set: List[str] = ["WatchList"]
    offset: int = 0
    limit: int = 20
    search_type: str = "BROAD"

class EntityNameSearchRequest(BaseModel):
    full_name: str
    content_set: List[str] = ["WatchList"]
    offset: int = 0
    limit: int = 20
    search_type: str = "BROAD"

class IdSearchRequest(BaseModel):
    id_number: str
    id_type: str
    record_types: List[str] = ["Person", "Entity"]
    content_set: List[str] = ["WatchList"]
    offset: int = 0
    limit: int = 20

class ProfileRequest(BaseModel):
    profile_id: str