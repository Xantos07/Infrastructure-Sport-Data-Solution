from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
  bootstrap_servers=["localhost:19092"],
  group_id="demo-group",
  auto_offset_reset="earliest",
  enable_auto_commit=False,
  consumer_timeout_ms=1000,
  value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
  key_deserializer=lambda m: m.decode('utf-8') if m else None
)

consumer.subscribe(["topic_activities.public.activities"])