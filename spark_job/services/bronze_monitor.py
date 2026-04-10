# bronze_monitor.py
import time
from delta import DeltaTable


class BronzeMonitor:
    def __init__(self, spark, path):
        self.spark = spark
        self.path = path


    def get_bronze_count(self) -> int:
        """Retourne le nombre de lignes dans Bronze, ou -1 si inexistant.

        Delta Lake résout ce count via les statistiques du transaction log
        (pas de scan fichier) — opération rapide.
        """
        try:
            if not DeltaTable.isDeltaTable(self.spark, self.path):
                return -1
            return self.spark.read.format("delta").load(self.path).count()
        except Exception:
            return -1


    def bronze_has_data(self) -> bool:
        try:
            is_delta = DeltaTable.isDeltaTable(self.spark, self.path)
            print(f"  [bronze_has_data] isDeltaTable={is_delta} path={self.path}")
            if not is_delta:
                return False
            rows = self.spark.read.format("delta").load(self.path).take(1)
            print(f"  [bronze_has_data] take(1)={rows}")
            return rows != []
        except Exception as e:
            print(f"  [bronze_has_data] ERREUR: {e}")
            return False


    def wait_for_bronze_data(self, timeout_seconds, interval_seconds) -> bool:
        print(f"Attente Bronze : path={self.path}")
        deadline = None if timeout_seconds <= 0 else time.time() + timeout_seconds

        while True:
            if self.bronze_has_data():
                print("Bronze prête : au moins 1 enregistrement détecté.")
                return True

            if deadline is not None and time.time() >= deadline:
                print(f"Timeout Bronze atteint ({timeout_seconds}s) sans donnée.")
                return False

            print(f"Bronze vide/non disponible, nouvelle vérification dans {interval_seconds}s...")
            time.sleep(interval_seconds)

