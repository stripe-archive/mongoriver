# Version 0.4

## Outline

This version adds support for tailing TokuMX, which uses a different oplog format from Mongo, separating the concepts of time and ordering of oplog entries. This sadly breaks most applications currently using mongoriver and will require invasive changes to persistent tailers.

More specifically - TokuMX uses `BSON::Binary` instead of `BSON::Timestamp` for keeping track of the database oplog position.

## Changes

### Tailers

Tailers gained a few new methods: `database_type` either returns `:toku` or `:mongo` based on which type of database is connected to the tailer.

Tailers also have a `most_recent_position` method, which returns the most recent oplog position in the database. It has an optional `before_time` parameter of type `Time`, which if passed, finds the latest position before (or at) that time. It is useful for converting timestamps to positions to tail from in tailers.

### Persistent tailers

In persistent tailers, the `read_timestamp` and `write_timestamp` methods were replaced with `read_state` and `write_state`, which read/write state hashes from/to the database. `read_state` should return `nil` if not state has been stored in the database yet.

In addition, there are convenience methods `read_timestamp` and `read_position` (already implemented) which extract the appropriate field from the `read_state` results. Both will return `nil` if `read_state` returns `nil`.

## State hash

State hashes have two fields - `'time'` which is a ruby Time object (used for humans, logs) and `'position'` of type `BSON::Binary` or `BSON::Timestamp` which the tailer uses to keep track of its position.

## How to upgrade

Persistent tailers need to define both `read_state` and `write_state` methods, which deserialize and serialize state hashes according to the database type.

Also, calls to `read_timestamp` and other methods mentioned above will have to be updated.

Also, if your application has custom filters for namespaces or [other things tokumx has updated](http://www.tokutek.com/2014/03/comparing-a-tokumx-and-mongodb-oplog-entry/), those need to be updated.

Note that to do seamless upgrades, some more mangling might be needed (e.g. updating database tables, figuring out how to get the position from an existing timestamp). The following file should be a good example of what this might entail: https://github.com/stripe/mosql/blob/karl-mongoriver-support-for-tokumx/lib/mosql/tailer.rb 
