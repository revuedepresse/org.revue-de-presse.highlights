# Daily Press Review

Easing observation of Twitter lists to publish a daily press review

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

### How to recommend new subscriptions based on a history of subscriptions?

```
lein run recommend-subscriptions twitter_handle
```

## Tests

There are no tests as intended behaviors heavily depend on Twitter API...  
OK, this is only wrong (even though I didn't want to use actual tokens 
in continuous integration to test the API as data may vary, accounts might get suspended, protected or deleted).

However, previous implementations of mosts commands are available at 
[github.com/thierrymarianne/daily-press-review](https://github.com/thierrymarianne/daily-press-review)
and the commands outcomes being pretty much the same, I feel confident enough to carry changes regularly
without worring about breaking everything.

Unit tests would become a more urgent matter whenever I would need to take care of external contributions
without having to worry about breaking the original intents (or breaking them without worrying neither).
