# Do not include any rules logic here. Only include imports and requires.

Import(
  rules=[
    'models/base.sml',
    'models/post.sml'
  ],
)

# Sudden Replies
Require(
  rule='rules/post/sudden_replies.sml',
  require_if=IsReply and not IsSelfThread and PostsCount <= 10,
)

Require(
  rule='rules/post/post_domain_spam.sml',
  require_if=HighRiskYoungAcctScreen,
)
