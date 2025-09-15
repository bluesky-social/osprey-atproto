# Here, we conditionally import the rulesets that we need based on the type of
# event being processed. Unless you are adding a new conditional ruleset, you
# probably should not be modifying this file.

Import(
  rules=[
    'models/base.sml',
  ],
)

Require(
  rule='rules/block/index.sml',
  require_if=IsBlock,
)
Require(
  rule='rules/follow/index.sml',
  require_if=IsFollow,
)
Require(
  rule='rules/like/index.sml',
  require_if=IsLike,
)
Require(
  rule='rules/list/index.sml',
  require_if=IsList,
)
Require(
  rule='rules/listitem/index.sml',
  require_if=IsListitem,
)
Require(
  rule='rules/post/index.sml',
  require_if=IsPost,
)
Require(
  rule='rules/profile/index.sml',
  require_if=IsProfile,
)
Require(
  rule='rules/repost/index.sml',
  require_if=IsRepost,
)
Require(
  rule='rules/starterpack/index.sml',
  require_if=IsStarterpack,
)
