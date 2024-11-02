# app/consumer/src/consumer.py

# Standard library imports
import logging
import threading
from typing import Any, Callable, Dict, List

# Third-party imports
from confluent_kafka import Consumer, KafkaError

logger = logging.getLogger(__name__)

class KafkaConsumerService:
    def __init__(self, config: dict, get_session_func: Callable[[], Any]):
        """
        Initialize the KafkaConsumerService.

        :param config: Kafka consumer configuration.
        :param get_session_func: A callable that returns a context manager for database sessions.
        """
        self.config = config
        self.get_session_func = get_session_func
        self.consumer = Consumer(self.config)
        self.running = False
        self.topic_handlers: Dict[str, List[Callable]] = {}
        self._thread = None

    def register_handlers(self, topic: str, handler: Callable):
        """
        Register a handler for a specific Kafka topic.

        :param topic: The Kafka topic to subscribe to.
        :param handler: The function to handle messages from the topic.
        """
        if topic not in self.topic_handlers:
            self.topic_handlers[topic] = []
            self.consumer.subscribe([topic])
            logger.info(f"Subscribed to topic '{topic}'.")
        
        self.topic_handlers[topic].append(handler)
        logger.info(f"Handler '{handler.__name__}' registered for topic '{topic}'.")

    def start(self):
        """
        Start the Kafka consumer loop.
        """
        self.running = True
        logger.info("Kafka consumer service started.")
        while self.running:
            msg = self.consumer.poll(1.0)  # Poll for messages
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                else:
                    logger.error(f"Kafka error: {msg.error()}")
            else:
                # Process message
                topic = msg.topic()
                message = msg.value()
                logger.info(f"Received message from topic '{topic}': {message}")

                handlers = self.topic_handlers.get(topic, [])
                for handler in handlers:
                    try:
                        with self.get_session_func() as session:
                            response = handler(message, db_session=session)
                            
                            if response.status == "SUCCESS":
                                logger.info(f"Handler '{handler.__name__}' processed message successfully.")
                                self.consumer.commit(asynchronous=False)
                            else:
                                logger.error(f"Handler '{handler.__name__}' failed: {response.detail}")
                    except Exception as e:
                        logger.error(f"Error processing message with handler '{handler.__name__}': {e}")

    def run(self):
        """
        Run the Kafka consumer in a separate thread.
        """
        self._thread = threading.Thread(target=self.start, name="KafkaConsumerThread")
        self._thread.start()

    def stop(self):
        """
        Stop the Kafka consumer gracefully.
        """
        self.running = False
        self.consumer.close()
        if self._thread:
            self._thread.join()
        logger.info("Kafka consumer service stopped.")
