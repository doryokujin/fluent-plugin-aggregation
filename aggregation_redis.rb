
module Fluent

class AggregationRedis  < AggregationBase
  Plugin.register_output('aggregation_redis', self)

  def initialize
    super
    require 'redis'
  end

  def configure(conf)
    super
    @host = conf['host'] || 'localhost'
    @port = conf['port'].to_i || 6379
    @db   = conf['db'].to_i || 0
    @expire = conf['expire'].to_i if conf['expire']
  end

  def start
    @redis = Redis.new(:host => @host, :port => @port, :db => @db, :thread_safe => true)
    super
  end

  def shutdown
    super
    @redis.quit
  end

  def write(chunk)
    super
    @redis.pipelined {
      @pre_aggregation.each{ |uid, record|
        @redis.mapped_hmset uid, record['key']
        @redis.mapped_hmset uid, { :name => record['name'] }
        @redis.mapped_hmset uid, { :partitoin => record['partition'] }
        @redis.hincrby uid, 'count', record['count']
        if values = record["value"]
          values.each{ |k,v| @redis.hincrby uid, k, v }
        end
        if @expire
          # Set volatilty
          @redis.expire uid, @expire
        end
      }
    }
  end
end

end
