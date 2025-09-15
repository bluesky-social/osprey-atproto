Import(
  rules=[
    'models/base.sml',
  ],
)

RepostSubjectUri: Entity[str] = EntityJson(
  type='Uri',
  path='$.record.subject.uri',
  coerce_type=True,
)

RepostSubjectDid: Optional[str] = DidFromUri(uri=RepostSubjectUri)
