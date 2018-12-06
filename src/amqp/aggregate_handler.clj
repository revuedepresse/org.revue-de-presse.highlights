(ns amqp.aggregate-handler
  (:use [repository.entity-manager]
        [repository.aggregate]
        [repository.status]
        [twitter.status]
        [command.generate-timely-statuses]))

(defn build-relationships
  "Build relationships missing between statuses and aggregates"
  [screen-name aggregate-id entity-manager unavailable-aggregate-message]
  ; do not process aggregate with id #1 (taken care of by another command)
  (when (not= 1 aggregate-id)
    (let [{member-model :members
           status-model :status
           status-aggregate-model :status-aggregate
           aggregate-model :aggregate} entity-manager
          aggregate (get-aggregate-by-id aggregate-id aggregate-model unavailable-aggregate-message)
          aggregate-name (:name aggregate)
          member (first (find-member-by-screen-name screen-name member-model))
          found-statuses (find-statuses-authored-by [(:screen-name member)] status-model)
          {total-new-relationships :total-new-relationships} (new-relationships
                                                               aggregate
                                                               found-statuses
                                                               status-aggregate-model
                                                               status-model)
          total-new-statuses (count found-statuses)]
      (log-new-relationships-between-aggregate-and-statuses
        total-new-relationships
        total-new-statuses
        aggregate-name)
      (generate-timely-statuses-for-aggregate aggregate-name 2018))))