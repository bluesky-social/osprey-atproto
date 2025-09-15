from typing import List, Optional

from pydantic import BaseModel, Field


class DocVerificationMethod(BaseModel):
    id: str
    type: str
    controller: str
    public_key_multibase: str = Field(alias='publicKeyMultibase')


class DocService(BaseModel):
    id: str
    type: str
    service_endpoint: str = Field(alias='serviceEndpoint')


class DIDDocument(BaseModel):
    did: str = Field(alias='id')  # 'id' in JSON, 'did' in Python
    also_known_as: Optional[List[str]] = Field(default=None, alias='alsoKnownAs')
    verification_method: Optional[List[DocVerificationMethod]] = Field(default=None, alias='verificationMethod')
    service: Optional[List[DocService]] = Field(default=None)
