#
# Fluent
#
# Copyright (C) 2011 FURUHASHI Sadayuki
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
module Fluent


module MetricsMixin
  def initialize
    super
    @metrics = []
  end

  def configure(conf)
    super

    conf.elements.select {|e|
      e.name == 'metrics'
    }.each {|e|
      add_metrics(e)
    }
  end

  def add_metrics(conf)
    m = Metrics.new
    m.configure(conf)
    @metrics << m
  end

  class Metrics
    include Configurable

    def initialize
      super
      @partition_key = nil
      @localtime = true
    end

    config_param :name, :string

    attr_reader :partition_key
    attr_reader :localtime

    config_param :each_key, :string, :default => nil
    attr_reader :keys  # parsed 'each_key'

    config_param :value_key, :string, :default => nil
    config_param :default_value, :string, :default => nil

    config_param :count_key, :string, :default => nil

    config_param :type, :default => :int do |val|
      case val
      when 'int'
        :int
      when 'float'
        :float
      else
        raise ConfigError, "unexpected 'type' parameter on metrics: expected int or float"
      end
    end

    def configure(conf)
      super

      if localtime = conf['localtime']
        @localtime = true
      elsif utc = conf['utc']
        @localtime = false
      end

      if @each_key
        @keys = @each_key.split(',').map {|e| e.strip }
        @key_proc = create_combined_key_proc(keys)
      else
        @keys = []
        use_keys = [@name]
        @key_proc = Proc.new {|record| use_keys }
      end

      case @type
      when :int
        if vk = @value_key
          dv = @default_value ? @default_value.to_i : nil
          @value_proc = Proc.new {|record| val = record[vk]; val ? val.to_i : dv }
        else
          dv = @default_value ? @default_value.to_i : 1
          @value_proc = Proc.new {|record| dv }
        end
      when :float
        if vk = @value_key
          dv = @default_value ? @default_value.to_f : nil
          @value_proc = Proc.new {|record| val = record[vk]; val ? val.to_f : dv }
        else
          dv = @default_value ? @default_value.to_f : 1.0
          @value_proc = Proc.new {|record| dv }
        end
      end

      if ck = @count_key
        @count_proc = Proc.new {|record| val = record[ck].to_i; val == 0 ? 1 : val }
      else
        @count_proc = Proc.new {|record| 1 }
      end

      if partition_key = conf['partition_key']
        @partition_proc = Proc.new {|time,record| record[partition_key] }
      else
        partition_by = conf['partition_by'] || 'h'
        case partition_by
        when 'daily', 'day', 'd'
          @time_format = "%Y-%m-%d"
        when 'hourly', 'hour', 'h'
          @time_format = "%Y-%m-%d %H"
        when 'minutely', 'minute', 'm'
          @time_format = "%Y-%m-%d %H:%M"
        when 'secondly', 'second', 's'
          @time_format = "%Y-%m-%d $H:$M:%S"
        else
          raise ConfigError, "Unexpected 'partition_by' parameter #{partition_by.dump} on metrics directive: expected daily, hourly, minutely or secondly"
        end
        timef = TimeFormatter.new(@time_format, @localtime)
        @partition_proc = Proc.new {|time,record| timef.format(time) }
      end
    end

    def create_combined_key_proc(keys)
      if keys.length == 1
        key = keys[0]
        Proc.new {|record|
          val = record[key]
          val ? [val] : nil
        }
      else
        Proc.new {|record|
          keys.map {|key|
            val = record[key]
            break nil unless val
            val
          }
        }
      end
    end

    def evaluate(time, record)
      keys = @key_proc.call(record)
      return nil unless keys

      value = @value_proc.call(record)
      return nil unless value

      count = @count_proc.call(record)

      partition = @partition_proc.call(time, record)

      return keys, value, count, partition
    end
  end

  def each_metrics(time, record, &block)
    @metrics.map {|m|
      keys, value, count, partition = m.evaluate(time, record)
      next unless keys
      block.call(m, keys, value, count, partition)
    }
  end
end


## Example
#
#class MetricsOutput < Output
#  Plugin.register_output('metrics', self)
#
#  include MetricsMixin
#
#  def initialize
#    super
#  end
#
#  config_param :tag, :string, :default => nil
#  config_param :tag_prefix, :string, :default => nil
#
#  def configure(conf)
#    super
#
#    if constant_tag = @tag
#      @tag_proc = Proc.new {|name,tag| constant_tag }
#    elsif tag_prefix = @tag_prefix
#      @tag_proc = Proc.new {|name,tag| "#{tag_prefix}.#{name}.#{tag}" }
#    else
#      @tag_proc = Proc.new {|name,tag| "#{name}.#{tag}" }
#    end
#  end
#
#  def emit(tag, es, chain)
#    es.each {|time,record|
#      each_metrics(time, record) {|metrics,keys,value,count,partition|
#        otag = @tag_proc.call(metrics.name, tag)
#        orecord = {
#          'name' => metrics.name,
#          'key' => keys.join(','),
#          'keys' => keys,
#          'key_hash' => Hash[ metrics.keys.zip(keys) ],
#          'value' => value,
#          'value_hash' => {metrics.value_key => value},
#          'count' => count,
#          'partition' => partition,
#        }
#        Engine.emit(otag, time, orecord)
#      }
#    }
#    chain.next
#  end
#end


## Example
#
#require 'metrics'
#
#class ExampleAggregationOutput < BufferedOutput
#  Plugin.register_output('aggregation_example', self)
#
#  include MetricsMixin
#
#  def initialize
#    super
#  end
#
#  def configure(conf)
#    super
#
#    # do something ...
#  end
#
#  def format(tag, time, record)
#    # call each_metrics(time,record):
#    each_metrics(time, record) {|metrics,keys,value,count,partition|
#      # 'metrics.name' is always avaiable
#      name = metrics.name
#
#      # 'keys' is an array of record value if each_key option is specified.
#      # otherwise, 'keys' is same as [metrics.name]
#      key = keys.join(',')
#
#      if !metrics.keys.empty?
#        # you can create Hash of the record using 'metrics.keys'
#        key_hash = Hash[ metrics.keys.zip(keys) ]
#      else
#        # keys is [name]
#      end
#
#      if metrics.value_key
#        # value is parsed record value
#        value_hash = {metrics.value_key => value}
#      else
#        # value is 1 or 1.0
#      end
#
#      if metrics.count_key
#        # value is parsed record value
#        count_hash = {metrics.count_key => count}
#      else
#        # count is 1
#      end
#
#      # ...
#    }
#  end
#end


end
