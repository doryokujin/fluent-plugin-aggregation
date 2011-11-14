
module Fluent

class AggregationBase  < TimeSlicedOutput
  include MetricsMixin

  def initialize
    super
    require 'digest/md5'
  end

  def format(tag, time, record)
    result = ''
    if @metrics.length==0
      result = record.to_msgpack
    else
      each_metrics(time, record) {|metrics,keys,value,count,partition|
        key_hash = Hash[ metrics.keys.zip(keys) ]
        obj = {
          "name" => metrics.name,
          "partition" => partition,
          "key" => key_hash,
          "count" => count
        }
        if metrics.value_key
          value_hash = {metrics.value_key => value}
          obj["value"] = value_hash
        end
        result << obj.to_msgpack
      }
    end
    result
  end

  def write(chunk)
    @pre_aggregation = {}
    chunk.open { |io|
      begin
        MessagePack::Unpacker.new(io).each { |record|
          pre_aggregate(record)
        }
      rescue EOFError
        # EOFError always occured when reached end of chunk.
      end
    }
  end

  private
  def pre_aggregate(record)
    uid = record["name"]+record["partition"]+record["key"].values().join()
    uid = Digest::MD5.new.update(uid).to_s
    if @pre_aggregation.include?(uid)
      if values = record["value"]
        values.each{ |k,v|
          @pre_aggregation[uid]["value"][k] += v
        }
      end
      @pre_aggregation[uid]["count"] += record["count"]
    else
      @pre_aggregation[uid] = record
    end
  end
end

end
