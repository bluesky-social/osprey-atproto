from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class OperationService(BaseModel):
    type: str
    endpoint: str


class DidLogEntry(BaseModel):
    sig: str
    prev: Optional[str] = None
    type: Optional[str] = None
    string: Optional[str] = None  # This is a bug in go, lol
    services: Optional[Dict[str, OperationService]]
    also_known_as: Optional[List[str]] = Field(alias='alsoKnownAs')
    rotation_keys: Optional[List[str]] = Field(alias='rotationKeys')
    verification_methods: Optional[Dict[str, str]] = Field(alias='verificationMethods')


class DidAuditEntry(BaseModel):
    did: str
    operation: DidLogEntry
    cid: str
    nullified: bool
    created_at: str = Field(alias='createdAt')


# Just a type alias, matches our Go implementation
DidAuditLog = List[DidAuditEntry]


class AuditLog(BaseModel):
    entries: List[DidAuditEntry]
