
module Fluent

class ForwardAggregationOutput < ForwardOutput
  Plugin.register_output('aggregation_forward', self)
  include MetricsMixin

  def initialize
    super
  end

  def configure(conf)
    super
    if flush_interval = conf['flush_interval']
      @writer.flush_interval = Config.time_value(flush_interval).to_i
    end
    @node_list = []
    @nodes.sort.each{ |sockaddr, node| @node_list << node }
    @num = @node_list.length
  end

  def start
    super
  end

  def shutdown
    super
  end

  def format(tag, time, record)
    result = nil
    if @metrics.length==0
      result = record.to_msgpack
    else
      each_metrics(time, record) {|metrics,keys,value,count,partition|
        key_hash = Hash[ metrics.keys.zip(keys) ]
        partition_id = partition(key_hash)
        obj = {
          "name" => metrics.name,
          "partition_id" => partition_id,
          "partition" => partition,
          "key" => key_hash,
          "count" => count
        }
        if metrics.value_key
          value_hash = {metrics.value_key => value}
          obj["value"] = value_hash
        end
        result = [ partition_id, obj ].to_msgpack
      }
    end
    result
  end

  def format_stream(tag, es)
    out = ''
    es.each {|time,record|
      out << format(tag, time, record)
    }
    out
  end

  def emit(tag, es, chain)
    data = format_stream(tag, es)
    if @buffer.emit(tag, data, chain)
      submit_flush
    end
  end

  def write(chunk)
    @pre_aggregation = {}
    tag = chunk.key
    get_partitioned_records(chunk).each{ |partition_id, record|
      chunk = MemoryBufferChunk.new(tag, record)
      node_index = partition_id
      @num.times do
        node = @node_list[node_index]
        if node.available?
          send_data(node, chunk)
          return
        end
        node_index -= 1
      end
      raise "no nodes are available"  # TODO message
    }
  end

  private
  def partition(key_hash)
    return Digest::MD5.hexdigest(key_hash.values.to_s).to_i(16) % @num
  end

  def pre_aggregate(partition_id, record)
    @pre_aggregation[partition_id] ||= {}
    uid = record["name"]+record["partition"]+record["key"].values().join()
    if @pre_aggregation[partition_id].include?(uid)
      if values = record["value"]
        values.each{ |k,v|
          @pre_aggregation[partition_id][uid]["value"][k] += v
        }
      end
      @pre_aggregation[partition_id][uid]["count"] += record["count"]
    else
      @pre_aggregation[partition_id][uid] = record
    end
  end

  def get_partitioned_records(chunk)
    chunk.open { |io|
      begin
        MessagePack::Unpacker.new(io).each { |record|
          partition_id = record[0]
          pre_aggregate(partition_id, record[1])
        }
      rescue EOFError
        # EOFError always occured when reached end of chunk.
      end
    }
    partitioned_hash = {}
    @pre_aggregation.each{ |partition_id, obj|
      partitioned_hash[partition_id] ||= ''
      obj.each{ |uid, record|
        time = Time.parse(record["partition"]).to_i
        partitioned_hash[partition_id] << [time, record].to_msgpack
      }
    }
    partitioned_hash
  end

end

end
