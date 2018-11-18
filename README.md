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

## How to import first degree subscriptions?

```
# 1000 ; Maximum number of messages to be consumed
# 5    ; Parallel consumers
lein run consume-amqp-messages network [1000] [5]
```

## How to import favorited statuses?

```
# 100 ; Maximum number of messages to be consumed
# 2   ; Parallel consumers
lein run consume-amqp-messages likes [100] [2]
```

## How to import statuses from lists?

```
# 1 ; Maximum number of messages to be consumed
# 3 ; Parallel consumers
lein run consume-amqp-messages lists [1] [3]
```

## How to recommend new subscriptions based on a history of subscriptions?

```
lein run recommend-subscriptions twitter_handle
```