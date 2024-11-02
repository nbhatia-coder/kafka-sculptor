import logging
import json
from typing import Generator
from pydantic import BaseSettings, BaseModel
from sqlmodel import Session, SQLModel, Field, create_engine
from contextlib import contextmanager
from app.consumer import KafkaConsumerService, HandlerResponse

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Define Pydantic settings for configuration
class Settings(BaseSettings):
    # Database settings
    database_url: str

    # Kafka settings
    bootstrap_servers: str
    security_protocol: str
    sasl_mechanism: str
    sasl_username: str
    sasl_password: str
    group_id: str
    auto_offset_reset: str
    enable_auto_commit: bool
    session_timeout_ms: int
    heartbeat_interval_ms: int
    max_poll_interval_ms: int

    class Config:
        env_file = ".env"  # Automatically load environment variables from .env file

# Load settings
settings = Settings()

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
        created_at="2024-04-27T12:35:00Z",
        modified_at="2024-04-27T12:35:00Z"
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
    engine = create_engine(settings.database_url, echo=True)
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
    'bootstrap.servers': settings.bootstrap_servers,
    'security.protocol': settings.security_protocol,
    'sasl.mechanism': settings.sasl_mechanism,
    'sasl.username': settings.sasl_username,
    'sasl.password': settings.sasl_password,
    'group.id': settings.group_id,
    'auto.offset.reset': settings.auto_offset_reset,
    'enable.auto.commit': settings.enable_auto_commit,
    'session.timeout.ms': settings.session_timeout_ms,
    'heartbeat.interval.ms': settings.heartbeat_interval_ms,
    'max.poll.interval.ms': settings.max_poll_interval_ms,
}

consumer_service = KafkaConsumerService(
    config=kafka_config,
    get_session_func=session_context
)

# Define handler functions
def handle_ap_invoice(message: bytes, db_session: Session) -> HandlerResponse:
    try:
        logger.debug(f"Attempting to create a new vendor with data: {message}")
        data = json.loads(message)
        erp_code = data.get('erp_code')
        name = data.get('name')

        if not erp_code or not name:
            logger.warning("Missing 'erp_code' or 'name' in message.")
            return HandlerResponse(status="FAILURE", detail="Invalid data")

        vendor_create = VendorCreate(erp_code=erp_code, name=name)
        vendor_read = create_vendor(session=db_session, vendor=vendor_create)
        logger.info(f"Vendor created successfully with ERP Code: {vendor_read.erp_code}")
        return HandlerResponse(status="SUCCESS")
    except json.JSONDecodeError:
        logger.error("Failed to decode JSON message.")
        return HandlerResponse(status="FAILURE", detail="JSON decode error")
    except Exception as e:
        logger.error(f"Error in handler: {e}")
        return HandlerResponse(status="FAILURE", detail=str(e))

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
