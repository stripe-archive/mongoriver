# Mongoriver [![Build Status](https://travis-ci.org/stripe/mongoriver.svg?branch=master)](https://travis-ci.org/stripe/mongoriver)

mongoriver is a library to monitor updates to your Mongo databases in
near-realtime. It provides a simple interface for you to take actions
when records are inserted, removed, or updated.


## How it works

MongoDB has an *oplog*, a log of all write operations. `mongoriver` monitors
updates to this oplog. See the [Mongo documentation for its oplog](http://docs.mongodb.org/manual/core/replica-set-oplog/) for more.

## How to use it

### Step 1: Create an outlet

You'll need to write a class subclassing
[Mongoriver::AbstractOutlet](https://github.com/stripe/mongoriver/blob/master/lib/mongoriver/abstract_outlet.rb).

You can override any of these methods:

* `update_optime(timestamp)`
* `insert(db_name, collection_name, document)`
* `remove(db_name, collection_name, document)`
* `update(db_name, collection_name, selector, update)`
* `create_index(db_name, collection_name, index_key, options)`
* `drop_index(db_name, collection_name, index_name)`
* `create_collection(db_name, collection_name,  options)`
* `drop_collection(db_name, collection_name)`
* `rename_collection(db_name, old_collection_name, new_collection_name)`
* `drop_database(db_name)`


You should think of these methods like callbacks -- if you want to do something
every time a document is inserted into the Mongo database, override the
`insert` method. You don't need to override all the methods -- if you only want
to take action on insert and update, just override `insert` and `update`.

### Step 2: Create a stream and start the logger

Once you've written your class, you can start tailing the Mongo oplog! Here's
the code you'll need to use:

```ruby
mongo = Mongo::MongoClient.from_uri(mongo_uri)
tailer = Mongoriver::Tailer.new([mongo], :existing)
outlet = YourOutlet.new(your_params) # Your subclass of Mongoriver::AbstractOutlet here
stream = Mongoriver::Stream.new(tailer, outlet)
stream.run_forever(starting_timestamp)
```

`starting_timestamp` here is the time you want the tailer to start at. We use
this to resume interrupted tailers so that no information is lost.


## Version history

### 0.5

Move from the Moped driver to the native Mongo 2.0 driver.

### 0.4

Add support for [tokumx](http://www.tokutek.com/products/tokumx-for-mongodb/). Backwards incompatible changes to persistent tailers to accomodate that. See [UPGRADING.md](UPGRADING.md).
