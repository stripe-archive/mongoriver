module Mongoriver
  class Streambed
    include Mongoriver::Logging

    attr_reader :stats

    class AssertionFailure < StandardError; end

    def assert(condition, msg)
      raise AssertionFailure.new(msg) unless condition
    end

    def initialize(upstreams, type)
      @tailer = Mongoriver::Tailer.new(upstreams, type)
      @record_fetch_batch_size = 1024
      @record_sync_batch_size = 256
      @stats = Hash.new(0)
    end

    def run
      self.class.validate_hooks!

      unless ts = starting_optime
        ts = @tailer.most_recent_timestamp
        initial_sync
        hook_update_optime(ts, true)
      end

      tail_from(ts)
    end

    def self.my_hooks
      @hooks ||= []
    end

    def self.all_hooks
      hooks = my_hooks
      if superclass <= Streambed
        hooks + superclass.all_hooks
      else
        hooks
      end
    end

    def self.validate_hooks!
      errors = []
      all_hooks.each do |name, args, opts|
        method = self.instance_method(hook_name(name))
        signature = "#{method.name}(#{args.join(', ')})"
        if method.owner == Streambed && !opts[:default]
          errors << "Must provide implementation of #{signature}"
        end
      end

      raise "You need to fix the following hook errors:

  #{errors.join("\n  ")}" if errors.length > 0
    end

    def self.hook_name(name)
      "hook_#{name}"
    end

    def self.hook(name, args=[], opts={})
      if default = opts[:default]
        target = hook_name(default)
        implementation = Proc.new do |*args, &blk|
          send(target, *args, &blk)
        end
      else
        implementation = Proc.new do
          raise NotImplementedError.new("Override in subclass")
        end
      end

      define_method(hook_name(name), implementation)
      my_hooks << [name, args, opts]
    end

    hook :optime
    hook :update_optime, [:ts, :mandatory]
    hook :initial_sync_index, [:db_name, :collection_name, :index_key, :options]
    hook :initial_sync_record_batch, [:db_name, :collection_name, :records]
    hook :stream_insert, [:db_name, :collection_name, :object]
    hook :stream_update, [:db_name, :collection_name, :selector, :update]
    hook :stream_remove, [:db_name, :collection_name, :object]
    # Not usually a difference between the initial index creation and
    # creating it while streaming ops.
    hook :stream_create_index, [:db_name, :collection_name, :index_key, :options], :default => :initial_sync_index
    # This seems to be called while doing a mapreduce.
    hook :stream_create_collection, [:db_name, :create]
    # This also seems to be called while doing a mapreduce. Note that
    # I think mongo has a concept of temporary table, which I should
    # look into, and renameCollection has some temporary table option.
    hook :stream_rename_collection, [:db_name, :source, :target]
    hook :stream_drop_index, [:db_name, :collection_name, :index_name]
    hook :stream_drop_collection, [:db_name, :dropped]
    hook :stream_drop_database, [:db_name]

    private

    def starting_optime
      case time = hook_optime
      when Integer
        if time >= 0
          BSON::Timestamp.new(time, 0)
        elsif time == -1
          @tailer.most_recent_timestamp
        else
          raise "Invalid optime: #{time}"
        end
      when BSON::Timestamp, nil
        time
      else
        raise "Unrecognized type #{time.class} (#{time.inspect}) for start time"
      end
    end

    def initial_sync
      initial_sync_all_indexes
      initial_sync_all_records
    end

    def initial_sync_all_indexes
      log.info("Beginning initial sync of indexes")
      syncable_databases.each {|db| initial_sync_indexes_for_db(db)}
      log.info("Done initial sync of indexes")
    end

    def initial_sync_indexes_for_db(db)
      db.collection('system.indexes').find.each do |index|
        options = extract_options_from_index_spec(index)
        index_key = index['key'].to_a

        ns = index['ns']
        db_name, collection_name = parse_ns(ns)
        assert(db_name == db.name, "Index db name #{db_name.inspect} differs from current db name #{db.name.inspect}")

        log.info("#{ns}: Initial sync of index #{options[:name]}")
        hook_initial_sync_index(db_name, collection_name, index_key, options)
      end
    end

    def initial_sync_all_records
      log.info("Beginning initial sync of records")
      syncable_databases.each {|db| initial_sync_records_for_db(db)}
      log.info("Done initial sync of records")
    end

    def initial_sync_records_for_db(db)
      syncable_collections(db).each do |collection|
        initial_sync_records_for_collection(collection)
      end
    end

    def initial_sync_records_for_collection(collection)
      db_name = collection.db.name
      collection_name = collection.name
      ns = "#{db_name}.#{collection_name}"

      log.info("#{ns}: Starting record initial sync")

      records = []
      collection.find({}, :batch_size => @record_fetch_batch_size, :timeout => false, :sort => [['$natural', 1]]) do |cursor|
        while cursor.has_next?
          records << cursor.next
          if records.length > @record_sync_batch_size
            # TODO: add better logging than this
            log.info("#{ns}: Running sync of batch of #{records.length} records")
            hook_initial_sync_record_batch(db_name, collection_name, records)
            records = []
          end
        end
      end
      log.info("#{ns}: Finishing sync with a batch of #{records.length} records")
      hook_initial_sync_record_batch(db_name, collection_name, records)

      log.info("#{ns}: Finished record initial sync")
    end

    # This should be fine to instantiate all at once, since
    # database_names returns all the dbs as strings anyway
    def syncable_databases
      @tailer.upstream_conn.database_names.map do |db_name|
        next if db_name == 'local'
        @tailer.upstream_conn.db(db_name)
      end.compact
    end

    def syncable_collections(db)
      db.collection_names.map do |collection_name|
        next if collection_name.start_with?('system.')
        db.collection(collection_name)
      end.compact
    end

    def extract_options_from_index_spec(index)
      options = {}
      index.each do |key, value|
        case key
        when 'v'
          raise NotImplementedError.new("Only v=1 indexes are supported at the moment, not v=#{value.inspect}") unless value == 1
        when 'ns', 'key'
        else
          options[key.to_sym] = value
        end
      end

      assert(options.include?(:name), "No name defined for index spec #{index.inspect}")
      options
    end

    def stream_op(entry)
      op = entry['op']
      data = entry['o']
      ns = entry['ns']

      if op == 'n'
        # This happens for initial rs.initiate() op, maybe others.
        log.info("Skipping no-op #{entry.inspect}")
        return
      end

      db_name, collection_name = parse_ns(ns)
      assert(db_name, "Nil db name #{db_name.inspect} for #{entry.inspect}")

      case op
      when 'i'
        if collection_name == 'system.indexes'
          record(ns, entry, :create_index)
          index_db_name, index_collection_name = parse_ns(data['ns'])
          index_key = data['key'].to_a
          options = extract_options_from_index_spec(data)
          hook_stream_create_index(index_db_name, index_collection_name, index_key, options)
        else
          record(ns, entry, :insert)
          hook_stream_insert(db_name, collection_name, data)
        end
      when 'u'
        record(ns, entry, :update)
        hook_stream_update(db_name, collection_name, entry['o2'], data)
      when 'd'
        record(ns, entry, :remove)
        hook_stream_remove(db_name, collection_name, data)
      when 'c'
        assert(collection_name == '$cmd', "Command collection name is #{collection_name.inspect} for #{entry.inspect}")
        if deleted_from = data['deleteIndexes']
          record(ns, entry, :drop_index)
          index = data['index']
          hook_stream_drop_index(db_name, deleted_from, index)
        elsif dropped = data['drop']
          record(ns, entry, :drop_collection)
          hook_stream_drop_collection(db_name, dropped)
        elsif dropped = data['dropDatabase']
          record(ns, entry, :drop_database)
          hook_stream_drop_database(db_name)
        elsif source = data['renameCollection']
          record(ns, entry, :rename_collection)
          target = data['to']
          hook_stream_rename_collection(db_name, source, target)
        elsif create = data['create']
          record(ns, entry, :create)
          hook_stream_create_collection(db_name, create)
        else
          raise "Unrecognized command #{data.inspect}"
        end
      else
        raise "Unrecognized op: #{op} (#{entry.inspect})"
      end

      optime = entry['ts']
      hook_update_optime(optime, false)
    end

    def tail_from(ts)
      begin
        @tailer.tail_from(ts)
        loop do
          @tailer.stream do |op|
            stream_op(op)
          end
        end
      ensure
        @tailer.stop
      end
    end

    def record(ns, entry, type)
      stats[type] += 1
      log.debug("#{ns}: #{type.inspect} #{entry.inspect}")
    end

    protected

    def parse_ns(ns)
      ns.split('.', 2)
    end
  end
end
