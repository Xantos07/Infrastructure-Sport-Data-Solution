from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import HttpUrl

PROJECT_ROOT = Path(__file__).resolve().parent

class Settings(BaseSettings):
    
    sheet_activities_id: str = ""
    sheet_employees_id: str = ""
    sheet_sports_id: str = ""

    discord_webhook_url: Optional[HttpUrl] = None
    employees_xlsx_filename: str = "DonneesRH.xlsx"
    sport_xlsx_filename: str = "DonneesSportive.xlsx"

    @property
    def base_dir(self) -> Path:
        """Retourne le répertoire racine du projet."""
        return PROJECT_ROOT
    
    @property
    def input_dir(self) -> Path:
        """Retourne le répertoire des fichiers d'entrée."""
        return PROJECT_ROOT / "inputs"
    
    @property
    def output_dir(self) -> Path:
        """Retourne le répertoire des fichiers de sortie."""
        return PROJECT_ROOT / "outputs"

    @property
    def xlsx_employees_full_path(self) -> Path:
        """Retourne le chemin complet du fichier XLSX."""
        return self.input_dir / self.employees_xlsx_filename
    
    @property
    def xlsx_sport_full_path(self) -> Path:
        """Retourne le chemin complet du fichier XLSX Clean."""
        return self.input_dir / self.sport_xlsx_filename

    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore" 
    )

settings = Settings()