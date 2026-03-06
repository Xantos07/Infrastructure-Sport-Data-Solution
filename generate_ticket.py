# ID ; ID salarié ; Date de début de l'activité ; Type ; Distance ; Date de fin de l'activité ; Commentaire.
# générer un ticket d'activité à partir des informations fournies

# DonneesRH DonneesSportive
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import numpy as np
from datetime import datetime
from configuration import Settings
from connect import connect
from config_postgresql import load_config
transport_modes = ["Marche/running", "Vélo/Trottinette/Autres"]
sport_types_distance = ["Runing", "Randonnée", "Triathlon", "Natation"]


### SEPARATION DES LOGIQUES METIERES EN FONCTIONS POUR PLUS DE CLARTE ET DE MAINTENABILITE !!! ###

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

    ################################################
    ## mettre dans une fonction
    ################################################
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

    ################################################
    ## mettre dans une fonction
    ################################################
    ID_salaries_selectioned_sport_only = []
    for index, row in df_sport_only.iterrows():
        if len(ID_salaries_selectioned_sport_only) >= (len(df_sport_only) * percentage_sport_only / 100):
            break
        ID_salaries_selectioned_sport_only.append(row['ID salarié'])

    print(f"ID_salaries_selectioned_sport_only : {ID_salaries_selectioned_sport_only}")


    ID_salaries_selectioned_transport_only = []
    for index, row in df_transport_only.iterrows():
        if len(ID_salaries_selectioned_transport_only) >= (len(df_transport_only) * percentage_transport_only / 100):
            break
        ID_salaries_selectioned_transport_only.append(row['ID salarié'])

    print(f"ID_salaries_selectioned_transport_only : {ID_salaries_selectioned_transport_only}")

    ID_salaries_selectioned_both = []
    for index, row in df_both.iterrows():
        if len(ID_salaries_selectioned_both) >= (len(df_both) * percentage_both / 100):
            break
        ID_salaries_selectioned_both.append(row['ID salarié'])

    print(f"ID_salaries_selectioned_both : {ID_salaries_selectioned_both}")


    # Collecter tous les tickets générés
    all_tickets = []
    all_tickets.extend(generate_ticket_for_employee(df_sport_only, ID_salaries_selectioned_sport_only))
    all_tickets.extend(generate_ticket_for_employee(df_transport_only, ID_salaries_selectioned_transport_only))
    all_tickets.extend(generate_ticket_for_employee(df_both, ID_salaries_selectioned_both))


    # Exemple de ticket
    # ID ; ID salarié ; Date de début de l'activité ; Type ; Distance ;
    #  Date de fin de l'activité ; Commentaire.
    # Exemple : 1; 101; 2023-10-01 07:30:00; running; 5.0; 2023-10-01 08:00:00; "Morning run to work"

    print(f"\n{len(all_tickets)} tickets ont été générés au total.")
    
    return all_tickets

def generate_ticket_for_employee(employee_df, selected_employee_ids):
    """
    Génère des tickets d'activité sportive pour les employés donnés
    """
    # Cette fonction génère des tickets en fonction des critères de chaque employé
    # Pour les employés qui font les deux (transport + sport), on génère les deux types de tickets
    
    all_tickets = []

    for employee_id in selected_employee_ids:
        employee_data = employee_df[employee_df['ID salarié'] == employee_id].iloc[0]
        # Générer les données du ticket en fonction des critères de l'employé
        # Par exemple, si l'employé fait du sport extérieur, générer un ticket de sport extérieur
        # Si l'employé vient au travail en vélo, générer un ticket de déplacement sportif, etc.

        # Générer un ticket pour le déplacement au travail si applicable
        if employee_data['Moyen de déplacement'] in transport_modes:
            ticket_transport = {
                "ID salarié": employee_id,
                "Date de début": generate_date_deplacement_to_work(),
                "Type": "Déplacement au travail - " + employee_data['Moyen de déplacement'],
                "Distance": f"{np.random.uniform(1000, 10000):.2f} m",
                "Durée": f"{np.random.randint(15, 60)} minutes",
                "Commentaire": np.random.choice([
                    'Super bon déplacement sportif pour aller au travail !', 
                    'J\'ai adoré venir au travail en sport aujourd\'hui !', 
                    'C\'était une super façon de commencer la journée !', 
                    'Je me suis senti(e) en pleine forme après ce déplacement sportif !'])
            }
            all_tickets.append(ticket_transport)
            print(f"Ticket transport généré pour l'employé {employee_id}: {ticket_transport}")

        # Générer un ticket pour l'activité sportive extérieure si applicable
        if pd.notna(employee_data['Pratique d\'un sport']):
            activity_type = employee_data['Pratique d\'un sport']
            sport_types_distance = ['Running', 'Triathlon', 'Natation', 'Randonnée']
            
            if activity_type in sport_types_distance:
                distance = f"{np.random.uniform(1000, 20000):.2f} m"
            else:
                distance = "N/A"
            
            ticket_sport = {
                "ID salarié": employee_id,
                "Date de début": generate_date_sport_activity(),
                "Type": activity_type,
                "Distance": distance,
                "Durée": f"{np.random.randint(30, 120)} minutes",
                "Commentaire": np.random.choice([
                    'C\'était super, j\'ai adoré !', 
                    'J\'ai eu du mal à finir, mais c\'était génial !', 
                    'Je me suis senti(e) en pleine forme après ça !', 
                    'C\'était un bon moyen de me détendre après le travail.'])
            }
            all_tickets.append(ticket_sport)
            print(f"Ticket sport généré pour l'employé {employee_id}: {ticket_sport}")
    
    return all_tickets


def generate_date_deplacement_to_work():
    """
    genere une date pour le déplacement au travail aléatoire entre le 01/01/2026 et le 31/12/2026, entre 7h-10h en semaine uniquement
    """
    # Générer une date aléatoire en utilisant un timestamp (nombre de jours depuis une date de référence)
    start_date = pd.Timestamp('2026-01-01')
    end_date = pd.Timestamp('2026-12-31')
    
    # Générer un nombre aléatoire de jours entre le début et la fin
    random_days = np.random.randint(0, (end_date - start_date).days + 1)
    date = start_date + pd.Timedelta(days=random_days)
    
    # Vérifier si c'est un jour de semaine (0=lundi, 4=vendredi, 5=samedi, 6=dimanche)
    while date.weekday() > 4:  # Si c'est samedi (5) ou dimanche (6)
        random_days = np.random.randint(0, (end_date - start_date).days + 1)
        date = start_date + pd.Timedelta(days=random_days)
    
    # Ajouter une heure entre 7h et 10h
    hour = np.random.randint(7, 11)
    minute = np.random.randint(0, 60)
    return date.replace(hour=hour, minute=minute, second=0)
    
def generate_date_sport_activity():
    """
    genere une date de début d'activité aléatoire entre le 01/01/2026 et le 31/12/2026, 
    entre 5h-22h en weekend et entre 17h-22h après le travail en semaine
    """
    # Générer une date aléatoire en utilisant un timestamp
    start_date = pd.Timestamp('2026-01-01')
    end_date = pd.Timestamp('2026-12-31')
    
    # Générer un nombre aléatoire de jours entre le début et la fin
    random_days = np.random.randint(0, (end_date - start_date).days + 1)
    date = start_date + pd.Timedelta(days=random_days)
    
    # Vérifier si c'est le weekend (samedi=5, dimanche=6)
    if date.weekday() > 4:  # Weekend
        # Entre 5h et 22h
        hour = np.random.randint(5, 23)
    else:  # Jour de semaine
        # Entre 17h et 22h (après le travail)
        hour = np.random.randint(17, 23)
    
    minute = np.random.randint(0, 60)
    return date.replace(hour=hour, minute=minute, second=0)

def insert_ticket_into_database(ticket):
    """
    Insère un ticket d'activité sportive dans la base de données activities
    """
    config = load_config()
    connection = connect(config)
    if connection is not None:
        try:
            with connection.cursor() as cursor:
                # Convertir la durée de "X minutes" en secondes
                duration_str = ticket["Durée"].replace(" minutes", "")
                elapsed_time_seconds = int(duration_str) * 60
                
                # Convertir la distance de "X.XX m" en entier (mètres)
                distance_str = ticket["Distance"].replace(" m", "")
                if distance_str == "N/A":
                    distance_meters = None
                else:
                    distance_meters = int(float(distance_str))
                
                insert_query = """
                INSERT INTO activities (employee_id, start_timestamp, sport_type, distance, elapsed_time, details)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_query, (
                    ticket["ID salarié"],
                    ticket["Date de début"],
                    ticket["Type"],
                    distance_meters,
                    elapsed_time_seconds,
                    ticket["Commentaire"]
                ))
                connection.commit()
                print(f"Activité insérée dans la base de données pour l'employé {ticket['ID salarié']}")
        except Exception as e:
            print(f"Erreur lors de l'insertion de l'activité dans la base de données: {e}")
        finally:
            connection.close()


def main():
    # Créer le DataFrame pandas à partir des données
    df = create_pandas_dataframe()

    # Afficher les statistiques avant le nettoyage pour voir approximativement les personnes éligibles
    statistics(df)

    # Nettoyer le DataFrame des personnes non éligibles des le début
    clean_dataframe(df)

    # Générer les tickets d'activité sportive pour les employés éligibles
    tickets = generate_ticket(df)

    # Insérer les tickets dans la base de données
    if tickets:
        print(f"\nInsertion de {len(tickets)} tickets dans la base de données...")
        for ticket in tickets:
            insert_ticket_into_database(ticket)
        print("Insertion terminée !")
    else:
        print("Aucun ticket à insérer dans la base de données.")

    
if __name__ == "__main__":
    main()