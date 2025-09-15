Import(
  rules=[
    'models/base.sml',
  ],
)

FollowSubjectDid: Entity[str] = EntityJson(
  type='User',
  path='$.record.subject',
  coerce_type=True,
)
