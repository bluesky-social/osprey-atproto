Import(
  rules=[
    'models/base.sml',
    'models/post.sml'
  ],
)

SuddenRepliesCount = IncrementWindow(
  key=f'sudden-replies-{Did}',
  window_seconds=Minute * 3,
  when_all=[
    PostsCount <= 10,
    IsReply,
    not IsSelfThread,
  ],
)

SuddenRepliesRule = Rule(
  when_all=[
    SuddenRepliesCount == 5,
  ],
  description='User created five reply posts within 3 minutes in their first ten posts.'
)

WhenRules(
  rules_any=[
    SuddenRepliesRule,
  ],
  then=[
    AddAtprotoTag(
      entity=Did,
      tag='possible-spam',
      comment='User created five reply posts within 3 minutes in their first ten posts.',
    ),
  ],
)

WhenRules(
  rules_any=[
    SuddenRepliesRule,
  ],
  then=[
    AtprotoReport(
      entity=Did,
      report_kind='spam',
      comment='User created five reply posts within 3 minutes in their first ten posts.',
    ),
  ],
)
