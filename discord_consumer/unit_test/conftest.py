"""Fixtures partagees pour les tests du consumer Discord.

Mock le module kafka_consumer AVANT tout import des modules qui en dependent,
car kafka_consumer instancie un KafkaConsumer a l'import.

Configure les variables d'environnement requises par DiscordSettings.
"""

import sys
from unittest.mock import MagicMock
from config.configuration import DiscordSettings

DiscordSettings().bootstrap_servers = "localhost:9092"
DiscordSettings().kafka_topic = "test-topic"

# Mock kafka_consumer 
_mock_kafka_module = MagicMock()
_mock_kafka_module.consumer = MagicMock()
sys.modules["kafka_consumer"] = _mock_kafka_module

# Mock message_processor
_mock_message_processor = MagicMock()
sys.modules["message_processor"] = _mock_message_processor
