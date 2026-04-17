"""Fixtures partagées pour les tests Spark.

Configure sys.path, fournit une SparkSession locale (session-scoped)
et des settings de test avec chemins temporaires.
"""

import sys
import os
import pytest
from pathlib import Path

# Ajouter spark_job/ et la racine projet au path pour les imports
SPARK_JOB_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = SPARK_JOB_DIR.parent

for p in [str(SPARK_JOB_DIR), str(PROJECT_ROOT)]:
    if p not in sys.path:
        sys.path.insert(0, p)

# Variables requises par SparkSettings (jamais utilisées dans les tests)
os.environ.setdefault("minio_base_url", "http://localhost:9000")
os.environ.setdefault("minio_user", "test")
os.environ.setdefault("minio_password", "test")

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """SparkSession locale partagée par tous les tests."""
    session = (
        SparkSession.builder
        .master("local[*]")
        .appName("unit-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


class _TestSettings:
    """Settings de test avec chemins locaux temporaires (remplace SparkSettings)."""

    def __init__(self, tmp_path):
        self.delta_bronze_path = str(tmp_path / "bronze" / "activities")
        self.delta_silver_activities = str(tmp_path / "silver" / "activities")
        self.delta_silver_employees = str(tmp_path / "silver" / "employees")
        self.delta_gold_eligibility = str(tmp_path / "gold" / "eligibility")
        self.delta_reference_data_path = str(tmp_path / "reference")
        self.powerbi_eligibility = str(tmp_path / "powerbi" / "eligibility")
        self.powerbi_activities = str(tmp_path / "powerbi" / "activities")


@pytest.fixture
def test_settings(tmp_path):
    """SparkSettings de test — chaque test reçoit un répertoire temporaire isolé."""
    return _TestSettings(tmp_path)
