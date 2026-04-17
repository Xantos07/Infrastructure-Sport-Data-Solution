import os
from kafka import KafkaConsumer
import json

_bootstrap = os.environ.get("BOOTSTRAP_SERVERS", "localhost:19092")
_topic = os.environ.get("KAFKA_TOPIC", "topic_activities.public.activities")

consumer = KafkaConsumer(
  bootstrap_servers=_bootstrap.split(","),
  group_id="demo-group",
  auto_offset_reset="earliest",
  enable_auto_commit=False,
  consumer_timeout_ms=1000,
  value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
  key_deserializer=lambda m: m.decode('utf-8') if m else None
)

consumer.subscribe([_topic])