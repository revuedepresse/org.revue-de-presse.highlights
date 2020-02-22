# DevObs

[![CircleCI](https://circleci.com/gh/thierrymarianne/daily-press-review-clojure.svg?style=svg)](https://circleci.com/gh/thierrymarianne/devobs-workers)

Easing observation of statuses from Twitter lists related to software development

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

### How to save highlights for all aggregates?

```
lein run save-highlights-for-all-aggregates `date -I`
```

### How to save highlights for an aggregate?

```
lein run save-highlights-for-main-aggregate `date -I`
```

### How to refresh highlights?

```
# For default aggregate
lein run record-popularity-of-highlights `date -I`
```

```
# For all aggregate
lein run record-popularity-of-highlights-for-all-aggregates `date -I`
```

```
# For a specific aggregate (press related aggregate for instance)
lein run record-popularity-of-highlights-for-main-aggregate `date -I`
```

### How to generate keywords from statuses?

```
lein run generate-keywords-from-statuses `date -I`
```

```
# Generate keywords from highlights for the first week of 2019
lein run generate-keywords-from-statuses 2019 0
```

### How to generate keywords for an aggregate

```
lein run generate-keywords-for-aggregate aggregate_name
```

### How to generate keywords for aggregates sharing a name

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

### How to migrate all statuses to publications?

```
lein run migrate-all-status-to-publications
```

### How to collect statuses identities?

```
export AGGREGATE_NAME='__FILL_ME__'
lein run collect-status-identities-for-aggregates "${AGGREGATE_NAME}"
```

## Tests

```
lein test
```
