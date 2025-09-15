Import(
  rules=[
    'models/base.sml',
    'models/post.sml'
  ],
)

PostDomainSpamCount = IncrementWindow(
  key=f'post-domains-{Did}',
  window_seconds=5 * Minute,
  when_all=[
    AccountAge <= 3 * Day,
    not IsReply,
    ListLength(list=PostAllDomains) > 0,
  ],
)

PostDomainSpamRule = Rule(
  when_all=[
    AccountAge <= 3 * Day,
    not IsReply,
    PostDomainSpamCount == 5,
  ],
  description='A new account has posted links five times in five minutes'
)

WhenRules(
  rules_any=[PostDomainSpamRule],
  then=[
    AddAtprotoTag(
      entity=Did,
      label='possible-domain-spam',
      comment='A new account has posted links five times in five minutes',
    ),
  ],
)

WhenRules(
  rules_any=[PostDomainSpamRule],
  then=[
    AtprotoReport(
      entity=Did,
      report_kind='spam',
      comment='A new account has posted links five times in five minutes',
    ),
  ],
)
