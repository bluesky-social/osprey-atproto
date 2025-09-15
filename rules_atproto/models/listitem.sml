Import(
  rules=[
    'models/base.sml',
  ],
)

ListitemSubjectDid: Entity[str] = EntityJson(
  type='User',
  path='$.record.subject',
  coerce_type=True,
)

ListitemList: Entity[str] = EntityJson(
  type='Uri',
  path='$.record.list',
  coerce_type=True,
)
