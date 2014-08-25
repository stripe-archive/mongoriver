module Mongoriver
  # This module deals with converting TokuMX oplog records into mongodb oplogs. 
  module Toku
    # @returns true if conn is a TokuMX database and the oplog records need to 
    #               be converted 
    def self.conversion_needed(conn)
      conn.server_info.has_key? "tokumxVersion"
    end

    # TODO: query for latest, planner for latest

    # Convert hash representing a tokumx oplog record to mongodb oplog records.
    # 
    # Things to note:
    #   1) Unlike mongo oplog, the timestamps will not be monotonically
    #      increasing
    #   2)
    # @see http://www.tokutek.com/2014/03/comparing-a-tokumx-and-mongodb-oplog-entry/
    # @returns Array<Hash> List of mongodb oplog records.
    def self.convert(record)
      # TODO: handle the case of long update with reference
      # TODO: handle 'n' after you figure out what that is
      # TODO: handle un-multi update
      result = record["ops"].each do |operation|
        result = nil
        case operation["op"]
        when 'i'
          insert_record(operation, record)
        when 'ur'
          update_record(operation, record)
        when 'c'
          command_record(operation, record)
        when 'd'
          remove_record(operation, record)
        else
          # ¯\_(ツ)_/¯
          # u, n
        end
      end
    end

    private
    def self.timestamp(full_record)
      # Note that this loses the monotonically increasing property, if not
      # lost before.
      BSON::TimeStamp(full_record["ts"].to_i, 0)
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
        "o" => operation["o"]
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


    def self.update_record(operation, full_record)
      # Note that the o2 field will have some extra info compared to mongo oplog

      {
        "ts" => timestamp(full_record),
        "h" => full_record["h"],
        # "v" => nil,
        "op" => "u",
        "ns" => operation["ns"],
        # { _id: BSON::ObjectId } what object was updated
        "o2" => operation["o"],
        "o" => operation["m"]
      }
    end
  end
end
