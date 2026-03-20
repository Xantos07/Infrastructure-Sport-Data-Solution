import logging

import pandas as pd
import pytest

from generate_ticket.analytics.statistics import print_statistics


class TestPrintStatistics:
    def test_empty_df_does_not_crash(self, caplog):
        df = pd.DataFrame(columns=["sport_practice", "transport_mode"])
        with caplog.at_level(logging.INFO, logger="generate_ticket"):
            print_statistics(df)
        assert "Aucun employé" in caplog.text

    def test_normal_df_logs_percentages(self, caplog):
        df = pd.DataFrame({
            "sport_practice": ["Running", None, "Natation", None],
            "transport_mode": [
                "véhicule thermique/électrique",
                "Marche/running",
                "Vélo/Trottinette/Autres",
                "véhicule thermique/électrique",
            ],
        })
        with caplog.at_level(logging.INFO, logger="generate_ticket"):
            print_statistics(df)
        assert "Statistiques" in caplog.text
        assert "%" in caplog.text
