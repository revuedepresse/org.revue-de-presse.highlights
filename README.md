# revue-de-presse.org highlights

Refresh daily highlights which can be found in press releases.

## Dependencies

Install Leiningen by following [the official instructions](https://github.com/technomancy/leiningen)

```
lein deps
```

## How to configure the project

```
cp lein-env{.dist,}
# fill the missing properties 
# in the newly created configuration file
```

## Available commands

### How to import first degree subscriptions?

```
# 1000 ; Maximum number of messages to be consumed
# 5    ; Parallel consumers
lein run consume-amqp-messages network [1000] [5]
```

### How to import favorited statuses?

```
# 100 ; Maximum number of messages to be consumed
# 2   ; Parallel consumers
lein run consume-amqp-messages likes [100] [2]
```

### How to import statuses from lists?

```
# 1 ; Maximum number of messages to be consumed
# 3 ; Parallel consumers
lein run consume-amqp-messages lists [1] [3]
```

### How to collect statuses?

```
# 1 ; Maximum number of messages to be consumed
# 3 ; Parallel consumers
lein run consume-amqp-messages status [1] [3]
```

### How to update bios of members?

```
lein run update-members-descriptions-urls
```

### How to recommend new subscriptions based on a history of subscriptions?

```
lein run recommend-subscriptions twitter_handle
```

## How to save highlights for all aggregates?

```
lein run save-highlights-for-all-aggregates `date -I`
```

## How to refresh highlights?

```
lein run record-popularity-of-highlights `date -I`
```

## How to generate keywords from statuses?

```
lein run generate-keywords-from-statuses `date -I`
```

## How to generate keywords for an aggregate

```
lein run generate-keywords-for-aggregate aggregate_name
```

## How to generate keywords for aggregates sharing a name

```
lein run generate-keywords-for-aggregates-sharing-name aggregate_name
```

### How to unarchive statuses from a database to another?

```
# Say we would like to unarchive a year of archives (by going week by week through the archives)
/bin/bash -c 'for week in `seq 0 52`; do lein run unarchive-statuses $week 2018; done;'
```

### How to collect statuses of a member?

```
lein run collect-timely-statuses-for-member twitter_handle
```

### How to collect statuses from subscriptions of a member?

```
lein run collect-timely-statuses-for-member-subscriptions twitter_handle
```

### How to collect statuses from aggregates?

```
lein run collect-timely-statuses-from-aggregates ":reverse-order"
```

### How to collect statuses from aggregates sharing a name?

```
lein run collect-timely-statuses-from-aggregate aggregate_name
```

### How to collect statuses identities?

```
lein run collect-status-identities-for-aggregates "news :: France"
```

## Tests

There is no 100% (very far from it) code coverage as intended behaviors heavily depend on Twitter API...  
OK, this is totally wrong (even though I didn't want to use actual tokens 
in continuous integration to test the API as data may vary, accounts might get suspended, protected or deleted).

However, previous implementations of mosts commands are available at 
[github.com/thierrymarianne/daily-press-review](https://github.com/thierrymarianne/daily-press-review)
and the commands outcomes being pretty much the same, I feel confident enough to carry changes regularly
without worring about breaking everything.

In order to worry less about breaking original intents (or breaking them wholeheartedly without worrying neither),
more tests will be added with new commands.

```
lein test
```
