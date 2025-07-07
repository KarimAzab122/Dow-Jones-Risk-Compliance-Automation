from pydantic import BaseSettings, Field
from typing import Optional

class Settings(BaseSettings):
    dj_client_id: str = Field(..., env="DJ_CLIENT_ID")
    dj_username: str = Field(..., env="DJ_USERNAME")
    dj_password: str = Field(..., env="DJ_PASSWORD")
    dj_auth_url: str = Field("auth.accounts.dowjones.com", env="DJ_AUTH_URL")
    dj_api_host: str = Field("api.dowjones.com", env="DJ_API_HOST")

    sftp_host: str = Field(..., env="SFTP_HOST")
    sftp_port: int = Field(22, env="SFTP_PORT")
    sftp_username: str = Field(..., env="SFTP_USERNAME")
    sftp_password: str = Field(..., env="SFTP_PASSWORD")
    sftp_host_key: Optional[str] = Field(None, env="SFTP_HOST_KEY")
    sftp_input_path: str = Field("/input/path", env="SFTP_INPUT_PATH")
    sftp_output_path: str = Field("/output/path", env="SFTP_OUTPUT_PATH")
    
    search_api_version: str = Field(
        "application/vnd.dowjones.dna.riskentities.v_2.0+json",
        env="SEARCH_API_VERSION"
    )
    profiles_api_version: str = Field(
        "application/vnd.dowjones.dna.riskentities-profiles.v_2.0+json",
        env="PROFILES_API_VERSION"
    )
    content_type: str = Field("application/json", env="CONTENT_TYPE")

    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'

        secrets_dir = '/run/secrets'

settings = Settings()