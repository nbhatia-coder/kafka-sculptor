# app/consumer/src/handler_response.py

from typing import Optional
from pydantic import BaseModel

class HandlerResponse(BaseModel):
    status: str  # "SUCCESS" or "FAILURE"
    detail: Optional[str] = None
