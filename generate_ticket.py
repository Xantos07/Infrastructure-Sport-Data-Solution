# ID ; ID salarié ; Date de début de l'activité ; Type ; Distance ; Date de fin de l'activité ; Commentaire.
# générer un ticket d'activité à partir des informations fournies

# DonneesRH DonneesSportive
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from datetime import datetime
from configuration import Settings
from connect import connect
transport_modes = ["Marche/running", "Vélo/Trottinette/Autres"]

def create_pandas_dataframe():
    # Crée un DataFrame pandas à partir des données et des colonnes fournies du donnees RH et donnees sportives
    
    settings = Settings()  # La configuration est chargée automatiquement ici

    # Récupérer les chemins des fichiers (sans parenthèses car ce sont des propriétés)
    chemin_rh = settings.xlsx_employees_full_path
    chemin_sportives = settings.xlsx_sport_full_path

    # Lire les fichiers Excel
    df_rh = pd.read_excel(chemin_rh)
    df_sport = pd.read_excel(chemin_sportives)
    
    # Fusionner les deux DataFrames sur l'ID salarié
    df = pd.merge(df_rh, df_sport, on='ID salarié', how='left')

    return df

def clean_dataframe(df):
    """
    Nettoie le DataFrame en supprimant les entrées non éligibles à la prime sportive
    """
    # Nettoyer le DataFrame en supprimant les employés non éligibles
    
    df.drop( df[ (df['Pratique d\'un sport'].isna()) & 
                (~df['Moyen de déplacement'].isin(transport_modes)) ].index, inplace=True)
    
    print("dataframe netoyé ", df)

def statistics(df):
    """
    Affiche des statistiques sur les données sportives des employés
    """
    # ==========
    # Analyser les données pour la prime sportive
    # ==========


    # Regarde en pourcentage combien de personne viennent au travail 
    # en vélo/trottinette/marche/running et ne font pas de sport exterieur
    total_employees = len(df)
    eligible_employees = len(df[(df['Pratique d\'un sport'].isna()) & 
                                 (df['Moyen de déplacement'].isin(transport_modes)) ])
    percentage_eligible = (eligible_employees / total_employees) * 100

    print(f"============================== Statistiques des employés ==============================")

    print(f"----------------------------------------------------------------------------------------")
    print(f"Pourcentage d'employés venant au travail en vélo/trottinette/marche/running sans faire de sport extérieur: {percentage_eligible:.2f}%")
    print(f"----------------------------------------------------------------------------------------")

    # regarde en pourcentage combien de personne font du sport exterieur 
    # et viennent au travail en vélo/trottinette/marche/running
    total_employees = len(df)
    both_criteria_employees = len(df[ ~df['Pratique d\'un sport'].isna() &
                                        (df['Moyen de déplacement'].isin(transport_modes)) ])
   
    percentage_both_criteria = (both_criteria_employees / total_employees) * 100
    
    print("----------------------------------------------------------------------------------------")
    print(f"Pourcentage d'employés pratiquant un sport extérieur et venant au travail en vélo/trottinette/marche/running: {percentage_both_criteria:.2f}%")
    print("----------------------------------------------------------------------------------------")

    # regarde en pourcentage combien de personne font du sport exterieur 
    # et ne viennent pas au travail en vélo/trottinette/marche/running
    total_employees = len(df)
    sport_only_employees = len(df[ ~df['Pratique d\'un sport'].isna() &
                                   (~df['Moyen de déplacement'].isin(transport_modes)) ])
    percentage_sport_only = (sport_only_employees / total_employees) * 100
    
    print("----------------------------------------------------------------------------------------")
    print(f"Pourcentage d'employés pratiquant un sport extérieur sans venir au travail en vélo/trottinette/marche/running: {percentage_sport_only:.2f}%")
    print("----------------------------------------------------------------------------------------")

    # regarde en pourcentage combien de personne ne font pas de sport exterieur 
    # et ne viennent pas au travail en vélo/trottinette/marche/running
    total_employees = len(df)
    non_eligible_employees = len(df[ (df['Pratique d\'un sport'].isna()) &
                                        (~df['Moyen de déplacement'].isin(transport_modes)) ])
    percentage_non_eligible = (non_eligible_employees / total_employees) * 100

    print("----------------------------------------------------------------------------------------")
    print(f"Pourcentage d'employés non éligibles à la prime sportive: {percentage_non_eligible:.2f}%")
    print("----------------------------------------------------------------------------------------")
    
    return


def generate_ticket(df):
    """
    Génère un ticket d'activité sportive avec des données aléatoires
    """
    # generer aleatoirement la distance si running ou cycling ou marching ou randonnee ou triathlon
    # type de sport récupéré depuis la table DonneesSportive
    # il faut faire attention car certains font pas de sport exterieurs mais vienne marche/running au travaille
    # dautre font que du sport externe 
    # d'autre font les deux

    # Condition tout ce qui font pas de sport externe ET ne vienne pas au travaille en vélo/trotinette/marche/running ne genere pas de ticket 
    
    # personne uniquement sur du sport exterieur
    df_sport_only = df[ ~df['Pratique d\'un sport'].isna() &
                        (~df['Moyen de déplacement'].isin(transport_modes)) ]
    # personne uniquement sur du déplacement sportif au travail
    df_transport_only = df[ df['Pratique d\'un sport'].isna() &
                            (df['Moyen de déplacement'].isin(transport_modes)) ]
    # personne qui font les deux
    df_both = df[ ~df['Pratique d\'un sport'].isna() &
                 (df['Moyen de déplacement'].isin(transport_modes)) ]

    print(f"df_sport_only : {len(df_sport_only)}")
    print(f"df_transport_only : {len(df_transport_only)}")
    print(f"df_both : {len(df_both)}")


    print("----------------------------------------------------------------------------------------")
    print(f"Génération de tickets d'activité sportive pour les employés éligibles...")
    print("----------------------------------------------------------------------------------------")
    # Validation pour une prime :
    # Prime sportive : 5% du salaire annuel brut pour les salariés venant au bureau
    # en pratiquant une activité physique (vélo, trottinette, course à pied, marche,
    # etc.). Il faut que le déplacement prenne une forme sportive la majorité du temps
    # pour pouvoir être éligible et c’est fait avec le déclaratif des salariés (tu
    # trouveras cette information dans le fichier RH).

    # 5 journées bien-être : Accordées aux salariés ayant une activité physique en
    # dehors du travail. Pour être éligible, il faut au minimum 15 activités physiques
    # dans l’année. Pour le moment, nous allons demander au salarié de déclarer les
    # différentes activités dans un google doc, mais nous souhaitons à terme utiliser
    # une application comme Strava pour récupérer directement les données.

 
    # mettre un pourcentage de X% de personne qui font du sport exterieur OU/ET viennent au travail en vélo/trottinette/marche/running
    # qui forcement sera eligible à la prime sportive et qui vont donc générer un ticket d'activité sportive
    percentage_sport_only = 20
    percentage_transport_only = 20 
    percentage_both = 20

    # Generer les tickets pour les personnes qui font que du sport exterieur
    # ID ; ID salarié ; Date de début de l'activité ; Type ; Distance ; Date de fin de l'activité ; Commentaire.
    # "ID salarié"
    # "Date de début" : 
    #  - entre 01/01/2026 à 31/12/2026
    # - entre 5h-22h en WK 
    # - entre 17h-22h après le taff en semaine
    # "Type" : 
    #  - running, vélo, marche, randonnee, triathlon
    # "Distance" : 
    #  - entre 1 et 20 km pour le running, marche, randonnee
    #  - entre 1 et 50 km pour le vélo
    #  - entre 1 et 100 km pour le triathlon
    # "Date de fin" : 
    #  - 01/01/2026 15h00:00
    # "Commentaire" :
    #  - "Morning run to work", "Evening bike ride", "Weekend hike", "Triathlon training", etc.

    # Exemple de ticket
    # ID ; ID salarié ; Date de début de l'activité ; Type ; Distance ;
    #  Date de fin de l'activité ; Commentaire.
    # Exemple : 1; 101; 2023-10-01 07:30:00; running; 5.0; 2023-10-01 08:00:00; "Morning run to work"


    

    return


def main():
    # Créer le DataFrame pandas à partir des données
    df = create_pandas_dataframe()

    # Afficher les statistiques avant le nettoyage pour voir approximativement les personnes éligibles
    statistics(df)

    # Nettoyer le DataFrame des personnes non éligibles des le début
    clean_dataframe(df)

    # Générer les tickets d'activité sportive pour les employés éligibles
    generate_ticket(df)
    
if __name__ == "__main__":
    main()