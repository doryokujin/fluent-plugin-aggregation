
module Fluent

class AggregationMongo  < AggregationBase
  Plugin.register_output('aggregation_mongo', self)
  include MetricsMixin

  def initialize
    super
    require 'mongo'
  end

=begin
  if RUBY_VERSION >= '1.9'
    UTF8_ENCODING   = Encoding.find('utf-8')
    BINARY_ENCODING = Encoding.find('binary')
    def to_utf8_binary(str)
      begin
        str.unpack("U*")
      rescue => ex
        raise InvalidStringEncoding, "String not valid utf-8: #{str.inspect}"
      end
      str.encode(UTF8_ENCODING).force_encoding(BINARY_ENCODING)
    end
  else
    def to_utf8_binary(str)
      begin
        str.unpack("U*")
      rescue => ex
        raise InvalidStringEncoding, "String not valid utf-8: #{str.inspect}"
      end
      str
    end
  end
=end

  def configure(conf)
    super
    raise ConfigError, "'database' parameter is required on MongoDump output" unless @database = conf['database']
    raise ConfigError, "'collectoin' parameter is required on MongoDump output" unless @collection = conf['collection']
    @host = conf['host'] || 'localhost'
    @port = conf['port'] || 27017
    if conf.has_key?('capped')
      @collection_opts = { :capped => true }
      @collectoin_opts[:size] = conf.has_key?('cap_size') ? Config.size_value(cap_size) : Config.size_value('1000m')
      @collection_opts[:max]  = Config.size_value(cap_size) if conf.has_key?('cap_max')
    end
  end

  def start
    @coll = get_collection(@collection)
    super
  end

  def shutdown
    super
    @coll.db.connection.close
  end

  def write(chunk)
    super
    @pre_aggregation.each{ |uid, record|
      inc_hash = { :count => record["count"] }
      if values = record["value"]
        values.each{ |k,v| inc_hash["value."+k.strip] = v }
      end
      @coll.update(
                   {
                     :_id => uid
                     #:_id => to_utf8_binary(uid)
                   }, {
                     "$inc" => inc_hash,
                     "$set" => {
                       :name => record["name"],
                       :key => record["key"],
                       :partition => record["partition"]
                     }
                   }, {
                     :safe => true,
                     :upsert => true
                   })
    }
  end

  def get_collection(collection)
    db = Mongo::Connection.new(@host, @port).db(@database)
    if db.collection_names.include?(collection)
      return db.collection(collection)
    else
      return db.create_collection(collection, @collection_opts)
    end
  end
end

end
