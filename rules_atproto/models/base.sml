UserId: Entity[str] = EntityJson(
  type='User',
  path='$.did',
  coerce_type=True,
)

# Alias for UserId
Did: str = JsonData(
  path='$.did',
  coerce_type=True,
)

Uri: Entity[str] = Entity(
  type='Uri',
  id=GetRecordURI(),
)

Cid: Entity[str] = Entity(
  type='Cid',
  id=GetRecordCID(),
)

Collection: Entity[str] = EntityJson(
  type='Collection',
  path='$.collection',
  coerce_type=True,
)

Handle = GetHandle()

AccountCreatedAt = GetDIDCreatedAt()
AccountAge = TimestampAge(timestamp=AccountCreatedAt)

PDSHost = GetPDSService()
DIDCreatedAtTimestamp = GetDIDCreatedAt()
DIDCreatedAt = TimestampAge(timestamp=DIDCreatedAtTimestamp)

# Only available if you are moderating your own PDS
#Email: Entity[str] = EntityJson(
#  type='Email',
#  path='$.ozone_repo_view_detail.email',
#  coerce_type=True,
#  required=False,
#)

#EmailConfirmedAt: Optional[str] = JsonData(
#  path='$.ozone_repo_view_detail.emailConfirmedAt',
#  required=False,
#)

#IsEmailConfirmed: bool = EmailConfirmedAt != None

DisplayName: Optional[str] = JsonData(
  path='$.profile_view.displayName',
  coerce_type=True,
)

PostsCount: int = JsonData(
  path='$.profile_view.postsCount',
  coerce_type=True,
  required=False,
)

FollowersCount: int = JsonData(
  path='$.profile_view.followersCount',
  coerce_type=True,
  required=False,
)

FollowsCount: int = JsonData(
  path='$.profile_view.followsCount',
  coerce_type=True,
  required=False,
)

ListsCount: int = JsonData(
  path='$.profile_view.associated.lists',
  coerce_type=True,
  required=False,
)

FeedgensCount: int = JsonData(
  path='$.profile_view.associated.feedgens',
  coerce_type=True,
  required=False,
)

StarterPacksCount: int = JsonData(
  path='$.profile_view.associated.starterPacks',
  coerce_type=True,
  required=False,
)

Avatar: Optional[str] = JsonData(
  path='$.profile_view.avatar',
  required=False,
)

IsAvatarSet: bool = Avatar != None

VerificationStatus: Optional[str] = JsonData(
  path='$.profile_view.verification.verifiedStatus',
  required=False,
)

IsVerified: bool = VerificationStatus != None and VerificationStatus == 'valid'

TrustedVerifierStatus: Optional[str] = JsonData(
  path='$.profile_view.verification.trustedVerifierStatus',
  required=False,
)

IsTrustedVerifier: bool = TrustedVerifierStatus != None and VerificationStatus == 'valid'

# Last IP info - only available if you are moderating your own PDS
#LastSigninIp: Entity[str] = EntityJson(
#  type='IP',
#  path='$.ozone_repo_view_detail.threatSignatures.lastSigninIp',
#  required=False,
#)


# Registration ip info - only available if you are moderating your own PDS
RegistrationIp: Entity[str] = EntityJson(
  type='IP',
  path='$.ozone_repo_view_detail.threatSignatures.registrationIp',
  required=False,
)

IsBlock: bool = Collection == 'app.bsky.graph.block'
IsFollow: bool = Collection == 'app.bsky.graph.follow'
IsLike: bool = Collection == 'app.bsky.feed.like'
IsList: bool = Collection == 'app.bsky.graph.list'
IsListitem: bool = Collection == 'app.bsky.graph.listitem'
IsPost: bool = Collection == 'app.bsky.feed.post'
IsProfile: bool = Collection == 'app.bsky.actor.profile'
IsRepost: bool = Collection == 'app.bsky.feed.repost'
IsStarterpack: bool = Collection == 'app.bsky.graph.starterpack'

Second: float = 1
Minute: float = Second * 60
FiveMinute: float = Minute * 5
TenMinute: float = Minute * 10
ThirtyMinute: float = Minute * 30
Hour: float = Minute * 60
Day: float = Hour * 24
Week: float = Day * 7
