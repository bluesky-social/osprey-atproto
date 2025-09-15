Import(
  rules=[
    'models/base.sml',
    'models/post.sml',
  ],
)

HaileyPostRule = Rule(
  when_all=[
    StringEndsWith(
      s=PDSHost,
      end='cocoon.hailey.at',
    ),
  ],
  description='Post by hailey',
)

WhenRules(
  rules_any=[
    HaileyPostRule,
  ],
  then=[
    AddAtprotoTag(
      entity=Did,
      tag='hailey-post',
    ),
  ],
)
