Import(
  rules=[
    'models/base.sml',
  ],
)

LikeSubjectUri: Entity[str] = EntityJson(
  type='Uri',
  path='$.record.subject.uri',
  coerce_type=True,
  required=True,
)

LikeSubjectDid: Optional[str] = DidFromUri(uri=LikeSubjectUri)
