# main.py

import logging
import json
from typing import Generator

from sqlmodel import Session, SQLModel, Field, create_engine
from pydantic import BaseModel

from app.consumer import KafkaConsumerService

from contextlib import contextmanager

# Configure logging
logger = logging.getLogger("kafka_clients.examples.example_app")
logger.setLevel(logging.DEBUG)  # Set desired log level

# Define your database models
class VendorCreate(BaseModel):
    erp_code: str
    name: str

class VendorRead(BaseModel):
    id: int
    erp_code: str
    name: str
    created_at: str
    modified_at: str

class Vendor(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    erp_code: str
    name: str
    created_at: str
    modified_at: str

# Define CRUD functions
def create_vendor(session: Session, vendor: VendorCreate) -> VendorRead:
    new_vendor = Vendor(
        erp_code=vendor.erp_code,
        name=vendor.name,
        created_at="2024-04-27T12:35:00Z",  # Replace with actual timestamp logic
        modified_at="2024-04-27T12:35:00Z"  # Replace with actual timestamp logic
    )
    session.add(new_vendor)
    session.commit()
    session.refresh(new_vendor)
    return VendorRead(
        id=new_vendor.id,
        erp_code=new_vendor.erp_code,
        name=new_vendor.name,
        created_at=new_vendor.created_at,
        modified_at=new_vendor.modified_at
    )

# Define the session factory
def get_session() -> Generator[Session, None, None]:
    DATABASE_URL = "sqlite:///./test.db"  # Replace with your actual DB URL
    engine = create_engine(DATABASE_URL, echo=True)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session

@contextmanager
def session_context():
    session_gen = get_session()
    session = next(session_gen)
    try:
        yield session
    finally:
        session_gen.close()

# Initialize the KafkaConsumerService
kafka_config = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'SOUHJ4FF6DLRVVIU',
    'sasl.password': 'fQ1GrfduCanFTdBUy6CrhobO9uElpvg7Dw7GVxlxkfpOwiDbj24gVTd4lQwvy50m',
    'group.id': 'test_ap_vendors',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'session.timeout.ms': 60000,
    'heartbeat.interval.ms': 20000,
    'max.poll.interval.ms': 300000
}

consumer_service = KafkaConsumerService(
    config=kafka_config,
    get_session_func=session_context
)

# Define handler functions
def handle_ap_invoice(message: bytes, db_session: Session):
    try:
        logger.debug(f"Attempting to create a new vendor with data: {message}")
        data = json.loads(message)
        erp_code = data.get('erp_code')
        name = data.get('name')

        if not erp_code or not name:
            logger.warning("Missing 'erp_code' or 'name' in message.")
            return

        vendor_create = VendorCreate(erp_code=erp_code, name=name)
        vendor_read = create_vendor(session=db_session, vendor=vendor_create)
        logger.info(f"Vendor created successfully with ERP Code: {vendor_read.erp_code}")
        return
    except json.JSONDecodeError:
        logger.error("Failed to decode JSON message.")
        return
    except Exception as e:
        logger.error(f"Error in handler: {e}")
        return

# Register the handler
consumer_service.register_handlers('ap_invoice', handle_ap_invoice)

# Start the consumer
if __name__ == "__main__":
    try:
        consumer_service.run()
        logger.info("Kafka consumer started. Press Ctrl+C to stop.")
        while True:
            pass  # Keep the main thread alive
    except KeyboardInterrupt:
        consumer_service.stop()
        logger.info("Kafka consumer stopped.")
