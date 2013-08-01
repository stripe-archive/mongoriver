module Mongoriver
  class Tailer
    include Mongoriver::Logging

    attr_reader :upstream_conn
    attr_reader :oplog

    def initialize(upstreams, type, oplog = "oplog.rs")
      @upstreams = upstreams
      @type = type
      @oplog = oplog
      # This number seems high
      @conn_opts = {:op_timeout => 86400}

      @cursor = nil
      @stop = false

      connect_upstream
    end

    def most_recent_timestamp
      record = oplog_collection.find_one({}, :sort => [['$natural', -1]])
      record['ts']
    end

    def connect_upstream
      case @type
      when :replset
        opts = @conn_opts.merge(:read => :secondary)
        @upstream_conn = Mongo::ReplSetConnection.new(@upstreams, opts)
      when :slave, :direct
        opts = @conn_opts.merge(:slave_ok => true)
        host, port = parse_direct_upstream
        @upstream_conn = Mongo::Connection.new(host, port, opts)
        raise "Server at #{@upstream_conn.host}:#{@upstream_conn.port} is the primary -- if you're ok with that, check why your wrapper is passing :direct rather than :slave" if @type == :slave && @upstream_conn.primary?
        ensure_upstream_replset!
      when :existing
        raise "Must pass in a single existing Mongo::Connection with :existing" unless @upstreams.length == 1 && @upstreams[0].respond_to?(:db)
        @upstream_conn = @upstreams[0]
      else
        raise "Invalid connection type: #{@type.inspect}"
      end
    end

    def ensure_upstream_replset!
      # Might be a better way to do this, but not seeing one.
      config = @upstream_conn['admin'].command(:ismaster => 1)
      unless config['setName']
        raise "Server at #{@upstream_conn.host}:#{@upstream_conn.port} is not running as a replica set"
      end
    end

    def parse_direct_upstream
      raise "When connecting directly to a mongo instance, must provide a single upstream" unless @upstreams.length == 1
      upstream = @upstreams[0]
      parse_host_spec(upstream)
    end

    def parse_host_spec(host_spec)
      host, port = host_spec.split(':')
      host = '127.0.0.1' if host.to_s.length == 0
      port = '27017' if port.to_s.length == 0
      [host, port.to_i]
    end

    def oplog_collection
      @upstream_conn.db('local').collection(oplog)
    end

    def tail(opts = {})
      raise "Already tailing the oplog!" if @cursor

      query = opts[:filter] || {}
      if ts = opts[:from]
        # Maybe if ts is old enough, just start from the beginning?
        query['ts'] = { '$gte' => ts }
      end

      mongo_opts = {:timeout => false}.merge(opts[:mongo_opts] || {})

      oplog_collection.find(query, mongo_opts) do |oplog|
        oplog.add_option(Mongo::Constants::OP_QUERY_TAILABLE)
        oplog.add_option(Mongo::Constants::OP_QUERY_OPLOG_REPLAY) if query['ts']
        oplog.add_option(Mongo::Constants::OP_QUERY_AWAIT_DATA) unless opts[:dont_wait]

        log.info("Starting oplog stream from #{ts || 'start'}")
        @cursor = oplog
      end
    end

    # Deprecated: use #tail(:from => ts, ...) instead
    def tail_from(ts, opts={})
      opts.merge(:from => ts)
      tail(opts)
    end

    def stream(limit=nil)
      count = 0
      while !@stop && @cursor.has_next?
        count += 1
        break if limit && count >= limit

        yield @cursor.next
      end

      return @cursor.has_next?
    end

    def stop
      @stop = true
    end

    def close
      @cursor.close if @cursor
      @cursor = nil
      @stop = false
    end
  end
end
