Import(
  rules=[
    'models/base.sml',
  ],
)

StarterpackList: Entity[str] = EntityJson(
  type='Uri',
  path='$.record.list',
  coerce_type=True,
)

StarterpackName: str = JsonData(
  path='$.record.name',
  coerce_type=True,
)
