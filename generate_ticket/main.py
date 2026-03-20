"""
Pipeline de génération de tickets d'activité sportive.

Étapes :
  1. Ingestion   — chargement des données RH et sportives
  2. Analytics   — statistiques d'éligibilité
  3. Nettoyage   — retrait des employés non éligibles
  4. Génération  — création des tickets d'activité
  5. Chargement  — insertion dans PostgreSQL
"""

import sys
from dataclasses import dataclass

from generate_ticket.ingestion.excel_loader import load_employee_data
from generate_ticket.analytics.statistics import print_statistics
from generate_ticket.transformation.cleaner import remove_non_eligible, split_by_eligibility
from generate_ticket.transformation.ticket_generator import generate_tickets
from generate_ticket.repository.activity_repository import insert_tickets_batch
from config.logger import logger


@dataclass
class PipelineResult:
    """Résumé d'exécution du pipeline."""
    total_employees: int
    eligible_employees: int
    tickets_generated: int
    success: bool
    error: str | None = None


def main() -> PipelineResult:
    """Exécute le pipeline ETL et retourne un résumé."""
    try:
        # 1. Ingestion
        df = load_employee_data()

        # 2. Statistiques avant nettoyage
        print_statistics(df)
        total_employees = len(df)

        # 3. Nettoyage
        df = remove_non_eligible(df)
        eligible_employees = len(df)

        # 4. Séparation en groupes + génération des tickets
        df_sport_only, df_transport_only, df_both = split_by_eligibility(df)
        logger.info(f"Sport uniquement : {len(df_sport_only)}")
        logger.info(f"Transport uniquement : {len(df_transport_only)}")
        logger.info(f"Les deux : {len(df_both)}")

        tickets = generate_tickets(df_sport_only, df_transport_only, df_both, total=500)

        # 5. Chargement dans PostgreSQL
        insert_tickets_batch(tickets, False)

        result = PipelineResult(
            total_employees=total_employees,
            eligible_employees=eligible_employees,
            tickets_generated=len(tickets),
            success=True,
        )
        logger.info(f"Pipeline terminé — {result.tickets_generated} tickets insérés "
                     f"({result.eligible_employees}/{result.total_employees} employés éligibles)")
        return result

    except Exception as e:
        logger.error(f"Pipeline échoué : {e}")
        return PipelineResult(
            total_employees=0,
            eligible_employees=0,
            tickets_generated=0,
            success=False,
            error=str(e),
        )


if __name__ == "__main__":
    result = main()
    sys.exit(0 if result.success else 1)
