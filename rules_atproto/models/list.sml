Import(
  rules=[
    'models/base.sml',
  ],
)

ListName: str = JsonData(
  path='$.record.name',
  coerce_type=True,
)

ListPurpose: str = JsonData(
  path='$.record.purpose',
  coerce_type=True,
)
