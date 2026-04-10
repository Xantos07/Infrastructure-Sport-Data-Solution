from pathlib import Path
import math
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, HttpUrl, BaseModel, field_validator

PROJECT_ROOT = Path(__file__).resolve().parent.parent

class BaseAppSettings(BaseSettings):
    """Settings communs à tous les services."""
    
    employees_csv_filename: str = "employees.csv"
    sport_csv_filename: str = "sports.csv"

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
    def csv_employees_full_path(self) -> Path:
        return self.input_dir / self.employees_csv_filename

    @property
    def csv_sport_full_path(self) -> Path:
        return self.input_dir / self.sport_csv_filename


class DiscordSettings(BaseAppSettings):
    """Settings pour le consumer Discord."""
    
    discord_webhook_url: Optional[HttpUrl] = None
    bootstrap_servers: str
    kafka_topic: str
    discord_interval: int = 300


class TicketGenerationSettings(BaseAppSettings):
    """Settings pour la génération de tickets (local).

    .env.local (non versionné) surcharge .env — utile pour POSTGRES_HOST=localhost
    quand generate_ticket tourne hors Docker.
    """

    model_config = SettingsConfigDict(
        env_file=[str(PROJECT_ROOT / ".env"), str(PROJECT_ROOT / ".env.local")],
        env_file_encoding="utf-8",
        extra="ignore"
    )

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

    bootstrap_servers: str = "redpanda-0:9092"
    kafka_topic: str = "topic_activities.public.activities"
    kafka_starting_offsets: str = "earliest"

    minio_base_url: str
    minio_user: str
    minio_password: str
    minio_bucket: str = "delta-lake"


    # ===== Chemins de stockage =====
    delta_bronze_path: str = "s3a://delta-lake/bronze/activities"
    delta_silver_path: str = "s3a://delta-lake/silver"
    delta_silver_activities: str = "s3a://delta-lake/silver/activities"
    delta_silver_employees: str = "s3a://delta-lake/silver/employees"
    delta_gold_eligibility: str = "s3a://delta-lake/gold/employee_eligibility"

    # ===== powerbi data clean =====
    powerbi_eligibility: str = "s3a://powerbi/employee_eligibility.parquet"
    powerbi_activities: str = "s3a://powerbi/activities.parquet"
    
    # ===== Checkpoints =====
    checkpoint_bronze: str = "s3a://delta-lake/checkpoints/bronze_activities"
    delta_bronze_console_checkpoint_path: str = "s3a://delta-lake/checkpoints/bronze_console"

    # ===== Données de référence (CSV - Delta) =====
    delta_reference_data_path: str = "s3a://delta-lake/reference_data"
    CSV_EMPLOYEES: str = "/opt/spark/app/inputs/employees.csv"
    CSV_SPORT: str = "/opt/spark/app/inputs/sports.csv"
    delta_input_employees: str = "s3a://delta-lake/inputs/employees.csv"
    delta_input_sports: str = "s3a://delta-lake/inputs/sports.csv"


class EmployeeRecord(BaseModel):
    employee_id: int = Field(alias="ID salarié")
    transport_mode: str = Field(alias="Moyen de déplacement")
    sport_practice: Optional[str] = Field(None, alias="Pratique d'un sport")

    @field_validator("sport_practice", mode="before")
    @classmethod
    def normalize_sport_practice(cls, v):
        if v is None:
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        if isinstance(v, str) and not v.strip():
            return None
        return v

    @field_validator("employee_id")
    @classmethod
    def id_not_empty(cls, v):
        if not v:
            raise ValueError("employee_id ne peut pas être vide")
        return v