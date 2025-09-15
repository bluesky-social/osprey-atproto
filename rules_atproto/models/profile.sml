Import(
  rules=[
    'models/base.sml',
  ],
)

ProfileDescription: Optional[str] = JsonData(
  path='$.record.description',
  required=False,
)

PinnedPost: Entity[str] = EntityJson(
  type='Uri',
  path='$.record.pinnedPost.uri',
  required=False,
)

IsPinnedPostSet: bool = PinnedPost != None
