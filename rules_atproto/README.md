# Bluesky Osprey Ruleset

Welcome to the Bluesky Osprey ruleset. Here, you can find and create moderation rules that run inside of the Osprey worker.

Osprey rules are written in SML, a stripped down yet Python-like language. Rules can be written for a variety of different events that may occur on the network, and
depending on what sort of behavior you are looking for.

## Project Structure

Inside of the ruleset directory, you will find three sub-directories:

- `rules`
- `models`
- `lists`

These directories and their contents make up the parsing and rules logic of Osprey. We have organized the rules in such a way that the minimum amount of parsing
ever needs to be performed for a given event to have each of its rules ran on it.

## Rules Directory

Inside of the `rules` directory you will find a `index.sml` file. If you take a look at this file, you will see that all we do is require each event type's own
`index.sml` file, and ensure that we only require those files when they are actually related to the input event.

Next, we can look more closely at the `rules/post` directory. Inside you will find each of the rules that is ran whenever a post event is created on the network.

Let's start by taking a look at `rules/post/index.sml`. Inside, you'll see that - similar to the `rules/index.sml` file - we are importing some models and requiring
various rules based on certain criteria. Every time that you create a new rule - by creating a `rule.sml` file and adding rules logic to that file - you will need
to actually "enable" it by adding it to the event type's `index.sml` file.

> [!NOTE]
> Unless you are adding a _new event type_, you should never need to modify the `rules/index.sml` file. Additionally, you should _never_ need to modify the root
> `main.sml` file at the root of the ruleset. When adding new rules, you very likely will only need to add it to the event type's `index.sml`.

There's an important thing to note here however! Notice that we are adding `require_if` logic to some of the imports. Whenever you have a rule that you intend to
skip for a likely large number of events (based on some criteria), you should _avoid actually requiring the file_. This is a performance optimization, and it
helps to make clear at a high level some basic criteria for your rule to run.

Some example criteria may be:

- Post is a reply (vs a root, top-level post)
- Account age
- Post count
- Third-party PDS

For example, we don't benefit from tagging toxic posts that are _root_ level posts, so we only run that rule whenever the post is a reply.

But...what is in a rule file?

## Rules

Deeper-level documentation on rules-writing can be found in `docs/WRITING-RULES.md`, but we'll offer a brief glance here. Let's take a peek at `bsky_shop.sml` to
see what what rule is doing.

Once again, we see that we import two models - the `base` model and the `post` model - and then run a bit of logic for the event.

```sml
BskyShopMaybeWord = SimpleListContains(
  cache_name='bsky-shop',
  list=[
    'caesaribus',
    'mundus',
    'caesar',
  ],
  phrases=PostTextTokens,
)
BskyShopWord = ForceString(s=BskyShopMaybeWord)

BskyShopRule = Rule(
  when_all=[
    PostsCount <= 5,
    FollowersCount <= 5,
    BskyShopMaybeWord != None,
  ],
  description=f'{BskyShopComment}',
)

WhenRules(
  rules_any=[BskyShopRule],
  then=[
    AddAtprotoLabel(
      entity=Did,
      label='spam',
      comment=f'{BskyShopComment}. Word was {BskyShopWord}',
      email=None,
      expiration_in_hours=None,
    ),
  ],
)
```

The logic here is quite simple! First, we create a variable that will evaluate to either a `str` or `None`, based on whether any of the words inside of a post are in the
provided list. Next, we create a `Rule` that will evaluate to `True` if _all_ of the following are true:

- `PostsCount <= 5`
- `FollowersCount <= 5`
- `BlueskyShopMaybeWord != None`

Finally, we conditionally - `WhenRules` - add an `AtprotoLabel` effect to the `Did` if `BskyShopRule` was `True`.

## Models

So, where does all of the data available to the rules engine come from? At a high level, the rules engine receives events from a Kafka event stream, which are - for all intents
and purposes - just JSON. The rules engine then takes that JSON and lets you run your SML rules on said JSON. We use "models" to define common values that are used throughout
the ruleset. Let's start by taking a look at `base.sml`.

Right away, we can see that there are a variety of variables for things that all of our events have in common.

```sml
UserId: Entity[str] = EntityJson(
  type='User',
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
```

Here, you can see that we are creating some "entities". We use either `EntityJson()` - when getting an entity from a JSON value - or `Entity()` - when getting an entity value
from a UDF (more on UDFs in a moment). By defining a variable as an `Entity`, it will (eventually >.<) be "linkable" within the Osprey UI. For example, by defining `UserId` as
an entity, we will be able to link all of the various events that `User` entity has performed. Similarly, by defining `Uri` as an entity, we will be able to see every instance
of that URI being interacted with (e.g. likes, reposts, replies).

There are other types of variables that we see inside of `base.sml`. For example, we have some basic `int` variables.

```sml
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
```

Note that although this looks similar to how we created some variables earlier with `EntityJson`, we are actually just using `JsonData` here. We don't plan to ever need to link
various counts like `PostsCount` together, so rather than making them an entity, we just define them as a variable that we can access throughout our ruleset.

Finally, you can see that we also have some entities for things like user PII like email address and IP address. Again, these are defined as entities so that they can be used in
the future for linking entities together.

Notice that all of the variables inside of `base.sml` might seem to be useful _across a variety of event types_. That is on purpose! Variables that you expect to use in rules for
various event types should go inside of `base.sml`. Howwever, variables that are _only_ useful in specific event types should go inside of their _respective `models/*.sml` file.

As an example, take a look at `models/post.sml`.

```sml
PostText: str = JsonData(
  path='$.record.text',
  coerce_type=True,
)

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
  s=PostText
)

ToxicityScore: float = JsonData(
  path='$.toxic_check_results.score',
  coerce_type=True,
  required=False,
)
```

All of these values are _only_ available to us for post events, so we avoid defining them unless we are actually processing a post event.

> [!NOTE]
> If you have an expensive variable to define that you think - or know - will get used across many rules, you should define it _once_ inside of the releveant `models/*.sml` file.
> For example, we tokenize the text of a post _once_ inside of `models/post.sml` rather than tokenize it multiple times across different rules.

## Lists

Finally, lists are datasets that can be used for things like searching text. There are two different kinds of lists:

- Simple string lists
- Regex pattern lists

Both of these list types are created as `yaml` files and placed inside of the `lists` directory. After doing so, you can use that list from inside of your rules by referencing
it by file name, like so:

```sml
ListMaybeWord = ListContains(
  list='my-list',
  phrases=PostTextTokens,
)
```

Of course, you may have a very small list that is specific to a certain rule that you may wish to just use `SimpleListContains` with (like we saw in `bsky_shop.sml`), but creating
list files inside of `lists` can be useful when you are either A. maintaing a large set of items inside of your list or B. want to use the values in that list across multiple rules.

Note that lists _may_ be used in a case-sensitive fashion. For example, you may use a list like so:

```sml
ListMaybeWord = ListContains(
  list='my-list',
  phrases=PostTextTokens,
  case_sensitive=True,
)
```

In this case, if your list contained the word `BOOP` and the post tokens contained the word `boop`, there would _not_ be a match. This is however _not_ the default behavior.

For more information about using `ListContains`, `SimpleListContains`, and `RegexListMatch`, see `docs/UDF-REFERENCE.md`.

## UDFs

Ultimately, SML is only a subset of Python that makes writing rules logic easy and readable, but isn't always powerful enough for what you need to do. That is where UDFs come into
play. UDFs (User Definied Functions) allow you to write Python code that can then be called from inside of SML rules. For example, `ListContains` is a UDF that opens a list file
and searches an array of inputs for values inside of your list file.

As rules become more complex, you will likely find new needs that you want to fill. That's great! UDFs will allow you to expand the power of Osprey in whatever way you need. For
more information on writing UDFs, see `docs/WRITING-UDFS.md`.
