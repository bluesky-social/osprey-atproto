Import(
  rules=[
    'models/base.sml',
  ],
)

BlockSubjectDid: Entity[str] = EntityJson(
  type='User',
  path='$.record.subject',
  coerce_type=True,
)
