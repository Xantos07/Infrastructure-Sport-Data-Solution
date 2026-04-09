from psycopg2.extras import execute_values

from generate_ticket.models.ticket import ActivityTicket
from config.connect import connect
from config.logger import logger


def insert_tickets_batch(tickets: list[ActivityTicket], clean_before_insert: bool = True):
    """Insère un lot de tickets en une seule connexion.

    Args:
        tickets: Liste de tickets à insérer.
        clean_before_insert: Si True, vide la table avant insertion (idempotence).
    """
    if not tickets:
        logger.info("Aucun ticket à insérer.")
        return

    connection = connect()

    try:
        with connection.cursor() as cursor:
            if clean_before_insert:
                cursor.execute("TRUNCATE TABLE activities RESTART IDENTITY")
                logger.info("Table activities vidée (idempotence).")

            insert_query = """
                INSERT INTO activities (employee_id, start_timestamp, sport_type, distance, elapsed_time, details)
                VALUES %s
            """
            ticket_data = [
                (
                    ticket.employee_id,
                    ticket.start_time,
                    ticket.sport_type,
                    ticket.distance_meters,
                    ticket.elapsed_time_seconds,
                    ticket.details,
                )
                for ticket in tickets
            ]
            execute_values(cursor, insert_query, ticket_data)
            connection.commit()
            logger.info(f"{len(tickets)} activités insérées dans la base de données.")
    except Exception as e:
        connection.rollback()
        logger.error(f"Erreur lors de l'insertion batch : {e}")
        raise
    finally:
        connection.close()
