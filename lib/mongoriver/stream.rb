module Mongoriver
  class Stream
    include Mongoriver::Logging
    include Mongoriver::Assertions

    attr_accessor :tailer, :outlet

    def initialize(tailer, outlet)
      assert(tailer.is_a?(Tailer),
             "tailer must be a subclass/instance of Tailer")
      assert(outlet.is_a?(AbstractOutlet),
             "outlet must be a subclass (or instance) of AbstractOutlet")

      @tailer = tailer
      @outlet = outlet
      @stop = false
      @stats = {}
    end

    def stats
      @stats
    end

    def run_forever(starting_timestamp=nil)
      @starting_optime = optime_from_ts(starting_timestamp)

      if @starting_optime
        log.info("Streaming from #{Time.at(@starting_optime.seconds)}")
      else
        log.info("Streaming from #{Time.now}")
      end

      @tailer.tail_from(@starting_optime)

      until @stop
        @tailer.stream do |op|
          handle_op(op)
        end
      end
    end

    def stop
      @stop = true
      @tailer.stop
    end

    private

    def optime_from_ts(timestamp)
      return @tailer.most_recent_timestamp if timestamp.nil?

      if timestamp.is_a?(Integer)
        if timestamp >= 0
          BSON::Timestamp.new(timestamp, 0)
        else
          raise "Invalid optime: #{timestamp}"
        end
      else
        raise "Unrecognized type #{timestamp.class} (#{timestamp.inspect}) " \
              "for start_timestamp"
      end
    end

    def trigger(name, *args)
      signature = "#{name}(" + args.map { |arg| arg.inspect }.join(', ') + ")"
      log.debug("triggering #{signature}")
      @stats[name] ||= 0
      @stats[name] += 1

      @outlet.send(name, *args)
    end

    def parse_ns(ns)
      ns.split('.', 2)
    end

    def handle_op(entry)
      op = entry['op']
      data = entry['o']
      ns = entry['ns']

      if op == 'n'
        # This happens for initial rs.initiate() op, maybe others.
        log.debug("Skipping no-op #{entry.inspect}")
        return
      end

      db_name, collection_name = parse_ns(ns)
      assert(db_name, "nil db name #{db_name.inspect} for #{entry.inspect}")

      case op
      when 'i'
        handle_insert(db_name, collection_name, data)
      when 'u'
        selector = entry['o2']
        trigger(:update, db_name, collection_name, selector, data)
      when 'd'
        trigger(:remove, db_name, collection_name, data)
      when 'c'
        assert(collection_name == '$cmd',
               "Command collection name is #{collection_name.inspect} for " \
               "#{entry.inspect}, but should be '$cmd'}")

        handle_cmd(db_name, collection_name, data)
      else
        raise "Unrecognized op: #{op} (#{entry.inspect})"
      end

      optime = entry['ts']
      trigger(:update_optime, optime)
    end

    def handle_insert(db_name, collection_name, data)
      if collection_name == 'system.indexes'
        handle_create_index(data)
      else
        trigger(:insert, db_name, collection_name, data)
      end
    end

    def handle_create_index(spec)
      db_name, collection_name = parse_ns(spec['ns'])
      index_key = spec['key'].to_a
      options = {}

      spec.each do |key, value|
        case key
        when 'v'
          unless value == 1
            raise NotImplementedError.new("Only v=1 indexes are supported, " \
                                          "not v=#{value.inspect}")
          end
        when 'ns', 'key' # do nothing
        else
          options[key.to_sym] = value
        end
      end

      assert(options.include?(:name),
             "No name defined for index spec #{spec.inspect}")

      trigger(:create_index, db_name, collection_name, index_key, options)
    end

    def handle_cmd(db_name, collection_name, data)
      if deleted_from_collection = data['deleteIndexes']
        index_name = data['index']
        trigger(:drop_index, db_name, deleted_from_collection, index_name)
      elsif created_collection = data['create']
        trigger(:create_collection, db_name, created_collection)
      elsif dropped_collection = data['drop']
        trigger(:drop_collection, db_name, dropped_collection)
      elsif old_collection_name = data['renameCollection']
        new_collection_name = data['to']
        trigger(:rename_collection, old_collection_name, new_collection_name)
      elsif dropped_db = data['dropDatabase']
        trigger(:drop_database, dropped_db)
      else
        raise "Unrecognized command #{data.inspect}"
      end
    end
  end
end