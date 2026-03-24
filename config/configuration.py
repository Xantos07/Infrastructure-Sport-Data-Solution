from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import HttpUrl

PROJECT_ROOT = Path(__file__).resolve().parent.parent

class BaseAppSettings(BaseSettings):
    """Settings communs à tous les services."""
    
    employees_xlsx_filename: str = "DonneesRH.xlsx"
    sport_xlsx_filename: str = "DonneesSportive.xlsx"

    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore"
    )

    @property
    def base_dir(self) -> Path:
        return PROJECT_ROOT

    @property
    def input_dir(self) -> Path:
        return PROJECT_ROOT / "inputs"

    @property
    def xlsx_employees_full_path(self) -> Path:
        return self.input_dir / self.employees_xlsx_filename

    @property
    def xlsx_sport_full_path(self) -> Path:
        return self.input_dir / self.sport_xlsx_filename


class DiscordSettings(BaseAppSettings):
    """Settings pour le consumer Discord."""
    
    discord_webhook_url: Optional[HttpUrl] = None
    bootstrap_servers: str
    kafka_topic: str
    discord_interval: int = 300


class TicketGenerationSettings(BaseAppSettings):
    """Settings pour la génération de tickets (local)."""

    postgres_user: str
    postgres_password: str
    postgres_db: str
    postgres_host: str
    postgres_port: int

    @property
    def db_config(self) -> dict:
        return {
            "host": self.postgres_host,
            "port": self.postgres_port,
            "dbname": self.postgres_db,
            "user": self.postgres_user,
            "password": self.postgres_password,
        }

class SparkSettings(BaseAppSettings):
    """Settings pour les jobs Spark."""

    minio_base_url: str
    minio_user: str
    minio_password: str
    minio_bucket: str = "delta-lake"

    XLSX_EMPLOYEES: str = "/opt/spark/app/inputs/DonneesRH.xlsx"
    XLSX_SPORT: str = "/opt/spark/app/inputs/DonneesSportive.xlsx"

    delta_bronze_path: str = "s3a://delta-lake/bronze/activities"
    delta_silver_activities: str = "s3a://delta-lake/silver/activities"
    delta_silver_employees: str = "s3a://delta-lake/silver/employees"
    delta_gold_eligibility: str = "s3a://delta-lake/gold/employee_eligibility"
    powerbi_eligibility: str = "s3a://powerbi/employee_eligibility.parquet"
    powerbi_activities: str = "s3a://powerbi/activities.parquet"
    checkpoint_bronze: str = "s3a://delta-lake/checkpoints/bronze_activities"
    delta_bronze_console_checkpoint_path: str = "s3a://delta-lake/checkpoints/bronze_console"
    
    delta_input_employees: str = "s3a://delta-lake/inputs/employees.csv"
    delta_input_sports: str = "s3a://delta-lake/inputs/sports.csv"
