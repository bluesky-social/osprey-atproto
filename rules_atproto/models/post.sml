Import(
  rules=[
    'models/base.sml',
  ],
)

PostText: str = JsonData(
  path='$.record.text',
  coerce_type=True,
)

PostTextCleaned: str = CleanString(s=PostText)

RootUri: Entity[str] = EntityJson(
  type='Uri',
  path='$.record.reply.root.uri',
  required=False,
)

RootDid: Optional[str] = DidFromUri(uri=RootUri)

ParentUri: Entity[str] = EntityJson(
  type='Uri',
  path='$.record.reply.parent.uri',
  required=False,
)

ParentDid: Optional[str] = DidFromUri(uri=ParentUri)

IsReply: bool = RootUri != None

IsSelfThread: bool = RootDid == Did and ParentDid == Did

# Tokenize a single time inside the model
PostTextTokens: List[str] = Tokenize(
  s=PostText,
)

PostTextCleanedTokens: List[str] = Tokenize(
  s=PostText,
)

ToxicityScore: float = JsonData(
  path='$.toxic_check_results.score',
  coerce_type=True,
  required=False,
)

PostTextDomains = ExtractDomains(s=PostText)

PostEmbedType: Optional[str] = JsonData(
  path='$.record.embed',
  required=False,
)
IsEmbedPost: bool = PostEmbedType != None

PostQuoteUri: Optional[str] = JsonData(
  path='$.record.embed.record.uri',
  required=False,
)
IsQuote: bool = PostQuoteUri != None

PostEmbedLink: Optional[str] = JsonData(
  path='$.record.embed.external.uri',
  required=False,
)
PostEmbedTitle: Optional[str] = JsonData(
  path='$.record.embed.external.title',
  required=False,
)
PostEmbedDescription: Optional[str] = JsonData(
  path='$.record.embed.external.description',
  required=False,
)
IsLinkEmbedPost: bool = PostEmbedLink != None

PostLanguages: List[str] = JsonData(
  path='$.record.langs',
  coerce_type=True,
  required=False,
)

PostAllDomains: List[str] = ConcatStringLists(
  lists=[
    PostTextDomains,
    ExtractDomains(s=ForceString(s=PostEmbedLink)),
  ],
)

PostShoppingDomain: Optional[str] = ListContains(
  list='shopping_domains',
  phrases=PostAllDomains
)


PostEmoji: List[str] = ExtractEmoji(s=PostText)
