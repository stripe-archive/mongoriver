module Mongoriver
  # This module deals with converting TokuMX oplog records into mongodb oplogs. 
  module Toku
    # @returns true if conn is a TokuMX database and the oplog records need to 
    #               be converted 
    def self.conversion_needed?(conn)
      conn.server_info.has_key? "tokumxVersion"
    end

    def self.operations_for(record, conn=nil)
      if record["ops"]
        return record["ops"]
      end
      # TODO: does this need to be sorted by seq?
      refs = conn.db('local').collection('oplog.refs').find({"_id.oid" => ref})
      refs.map { |r| r["ops"] }.flatten
    end

    # TODO: query for latest, planner for latest

    # Convert hash representing a tokumx oplog record to mongodb oplog records.
    # 
    # Things to note:
    #   1) Unlike mongo oplog, the timestamps will not be monotonically
    #      increasing
    #   2) h fields (unique ids) will also not be unique on multi-updates
    #   3) operations marked by 'n' toku are ignored, as these do <TODO: explain>
    # @see http://www.tokutek.com/2014/03/comparing-a-tokumx-and-mongodb-oplog-entry/
    # @returns Array<Hash> List of mongodb oplog records.
    def self.convert(record, conn=nil)
      result = []
      operations_for(record, conn).each do |operation|
        case operation["op"]
        when 'i'
          result << insert_record(operation, record)
        when 'ur'
          result << update_record(operation, record, true)
        when 'u'
          result << update_record(operation, record, false)
        when 'c'
          result << command_record(operation, record)
        when 'd'
          result << remove_record(operation, record)
        when 'n'
          # keepOplogAlive requests - safe to ignore?
        else
          raise "Unrecognized op: #{operation["op"]} (#{record.inspect})"
        end
      end

      result
    end

    private
    def self.timestamp(full_record)
      # Note that this loses the monotonically increasing property, if not
      # lost before.
      BSON::Timestamp.new(full_record["ts"].to_i, 0)
    end

    def self.insert_record(operation, full_record)
      {
        # Monotonically increasing timestamp in mongodb in oplog.
        # e.g. <BSON::Timestamp:0x000000100a81c8 @increment=1, @seconds=1408995488>
        "ts" => timestamp(full_record),
        # Unique ID for this operation
        # Note that not so unique anymore
        "h" => full_record["h"],
        # Ignoring v ("version") for now
        # "v" => nil,
        "op" => "i",
        # namespace being updated. in form of database-name.collection.name
        "ns" => operation["ns"],
        # operation being done. 
        # e.g. {"_id"=>BSON::ObjectId('53fb8f6b9e126b2106000003')}
        "o" => operation["o"]
      }
    end

    def self.remove_record(operation, full_record)
      {
        "ts" => timestamp(full_record),
        "h" => full_record["h"],
        # "v" => nil,
        "op" => "d",
        "ns" => operation["ns"],
        # "b" => true, # ???
        "o" => { "_id" => operation["o"]["_id"] }
      }
    end

    def self.command_record(operation, full_record)
      {
        "ts" => timestamp(full_record),
        "h" => full_record["h"],
        # "v" => nil,
        "op" => "c",
        "ns" => operation["ns"],
        "o" => operation["o"]
      }
    end


    def self.update_record(operation, full_record, is_ur)
      # Note that the o2 field will have some extra info compared to mongo oplog
      # u and ur differ here

      result = {
        "ts" => timestamp(full_record),
        "h" => full_record["h"],
        # "v" => nil,
        "op" => "u",
        "ns" => operation["ns"],
        # { _id: BSON::ObjectId } what object was updated
        "o2" => { "_id" => operation["o"]["_id"] },
        # "o" => operation["m"]
      }
      if is_ur
        return result.merge({
          "o" => operation["m"],
        })
      else
        return result.merge({
          "o" => operation["o2"],
        })
      end
    end
  end
end
