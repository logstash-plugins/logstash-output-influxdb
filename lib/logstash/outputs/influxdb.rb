# encoding: utf-8
require "logstash/namespace"
require "logstash/outputs/base"
require "logstash/json"
require "stud/buffer"

# This output lets you output Metrics to InfluxDB (>= 0.9.0-rc31)
#
# The configuration here attempts to be as friendly as possible
# and minimize the need for multiple definitions to write to
# multiple measurements and still be efficient
#
# the InfluxDB API let's you do some semblance of bulk operation
# per http call but each call is database-specific
#
# You can learn more at http://influxdb.com[InfluxDB homepage]
class LogStash::Outputs::InfluxDB < LogStash::Outputs::Base
  include Stud::Buffer

  config_name "influxdb"

  # The database to write - supports sprintf formatting
  config :db, :validate => :string, :default => "statistics"

  # The retention policy to use
  config :retention_policy, :validate => :string, :default => "default"

  # The hostname or IP address to reach your InfluxDB instance
  config :host, :validate => :string, :required => true

  # The port for InfluxDB
  config :port, :validate => :number, :default => 8086

  # The user who has access to the named database
  config :user, :validate => :string, :default => nil

  # The password for the user who access to the named database
  config :password, :validate => :password, :default => nil

  # Enable SSL/TLS secured communication to InfluxDB
  config :ssl, :validate => :boolean, :default => false

  # Measurement name - supports sprintf formatting
  config :measurement, :validate => :string, :default => "logstash"

  # Hash of key/value pairs representing data points to send to the named database
  # Example: `{'column1' => 'value1', 'column2' => 'value2'}`
  #
  # Events for the same measurement will be batched together where possible
  # Both keys and values support sprintf formatting
  config :data_points, :validate => :hash, :default => {}, :required => true

  # Allow the override of the `time` column in the event?
  #
  # By default any column with a name of `time` will be ignored and the time will
  # be determined by the value of `@timestamp`.
  #
  # Setting this to `true` allows you to explicitly set the `time` column yourself
  #
  # Note: **`time` must be an epoch value in either seconds, milliseconds or microseconds**
  config :allow_time_override, :validate => :boolean, :default => false

  # Set the level of precision of `time`
  #
  # only useful when overriding the time value
  config :time_precision, :validate => ["n", "u", "ms", "s", "m", "h"], :default => "ms"

  # Allow value coercion
  #
  # this will attempt to convert data point values to the appropriate type before posting
  # otherwise sprintf-filtered numeric values could get sent as strings
  # format is `{'column_name' => 'datatype'}`
  #
  # currently supported datatypes are `integer` and `float`
  #
  config :coerce_values, :validate => :hash, :default => {}

  # Automatically use fields from the event as the data points sent to Influxdb
  config :use_event_fields_for_data_points, :validate => :boolean, :default => false
  
  # An array containing the names of fields from the event to exclude from the
  # data points 
  # 
  # Events, in general, contain keys "@version" and "@timestamp". Other plugins
  # may add others that you'll want to exclude (such as "command" from the 
  # exec plugin).
  # 
  # This only applies when use_event_fields_for_data_points is true.
  config :exclude_fields, :validate => :array, :default => ["@timestamp", "@version", "sequence", "message", "type"]  

  # An array containing the names of fields to send to Influxdb as tags instead 
  # of fields. Influxdb 0.9 convention is that values that do not change every
  # request should be considered metadata and given as tags.
  config :send_as_tags, :validate => :array, :default => ["host"]

  # This setting controls how many events will be buffered before sending a batch
  # of events. Note that these are only batched for the same measurement
  config :flush_size, :validate => :number, :default => 100

  # The amount of time since last flush before a flush is forced.
  #
  # This setting helps ensure slow event rates don't get stuck in Logstash.
  # For example, if your `flush_size` is 100, and you have received 10 events,
  # and it has been more than `idle_flush_time` seconds since the last flush,
  # logstash will flush those 10 events automatically.
  #
  # This helps keep both fast and slow log streams moving along in
  # near-real-time.
  config :idle_flush_time, :validate => :number, :default => 1


  public
  def register
    require 'manticore'
    require 'cgi'
    
    @client = Manticore::Client.new
    @queue = []
    @protocol = @ssl ? "https" : "http"

    buffer_initialize(
      :max_items => @flush_size,
      :max_interval => @idle_flush_time,
      :logger => @logger
    )
  end # def register


  public
  def receive(event)
    @logger.debug? and @logger.debug("Influxdb output: Received event: #{event}")

    # An Influxdb 0.9 event looks like this: 
    # cpu_load_short,host=server01,region=us-west value=0.64 1434055562000000000
    #  ^ measurement  ^ tags (optional)            ^ fields   ^ timestamp (optional)
    # 
    # Since we'll be buffering them to send as a batch, we'll only collect
    # the values going into the points array
    
    time  = timestamp_at_precision(event.timestamp, @time_precision.to_sym)
    point = create_point_from_event(event)

    if point.has_key?('time')
      unless @allow_time_override
        logger.error("Cannot override value of time without 'allow_time_override'. Using event timestamp")
      else
        time = point.delete("time")
      end
    end

    tags, point = extract_tags(point)

    event_hash = {
      "measurement" => event.sprintf(@measurement),
      "time"        => time,
      "fields"      => point
    }
    exclude_fields!(point)
    coerce_values!(point)

    event_hash["tags"] = tags unless tags.empty?

    buffer_receive(event_hash, event.sprintf(@db))
  end # def receive


  def flush(events, database, teardown = false)
    @logger.debug? and @logger.debug("Flushing #{events.size} events to #{database} - Teardown? #{teardown}")
    post(events_to_request_body(events), database)
  end # def flush


  def post(body, database, proto = @protocol)
    begin
      @query_params = "db=#{database}&rp=#{@retention_policy}&precision=#{@time_precision}&u=#{@user}&p=#{@password.value}"
      @base_url = "#{proto}://#{@host}:#{@port}/write"
      @url = "#{@base_url}?#{@query_params}"

      @logger.debug? and @logger.debug("POSTing to #{@url}")
      @logger.debug? and @logger.debug("Post body: #{body}")
      response = @client.post!(@url, :body => body)
  
    rescue EOFError
      @logger.warn("EOF while writing request or reading response header from InfluxDB",
                   :host => @host, :port => @port)
      return # abort this flush
    end

    if read_body?(response)
      # Consume the body for error checking
      # This will also free up the connection for reuse.
      body = ""
      begin
        response.read_body { |chunk| body += chunk }
      rescue EOFError
        @logger.warn("EOF while reading response body from InfluxDB",
                     :host => @host, :port => @port)
        return # abort this flush
      end

      @logger.debug? and @logger.debug("Body: #{body}")
    end

    unless response && (200..299).include?(response.code)
      @logger.error("Error writing to InfluxDB",
                    :response => response, :response_body => body,
                    :request_body => @queue.join("\n"))
      return
    else
      @logger.debug? and @logger.debug("Post response: #{response}")
    end
  end # def post

  def close
    buffer_flush(:final => true)
  end # def teardown


  # A batch POST for InfluxDB 0.9 looks like this:
  # cpu_load_short,host=server01,region=us-west value=0.64 cpu_load_short,host=server02,region=us-west value=0.55 1422568543702900257 cpu_load_short,direction=in,host=server01,region=us-west value=23422.0 1422568543702900257  
  def events_to_request_body(events)
    events.map do |event|
      result = escaped_measurement(event["measurement"].dup)
      result << "," << event["tags"].map { |tag,value| "#{escaped(tag)}=#{escaped(value)}" }.join(',') if event.has_key?("tags")
      result << " " << event["fields"].map { |field,value| "#{escaped(field)}=#{quoted(value)}" }.join(',')
      result << " #{event["time"]}"
    end.join("\n") #each measurement should be on a separate line
  end

  # Create a data point from an event. If @use_event_fields_for_data_points is
  # true, convert the event to a hash. Otherwise, use @data_points. Each key and 
  # value will be run through event#sprintf with the exception of a non-String
  # value (which will be passed through)
  def create_point_from_event(event)
    Hash[ (@use_event_fields_for_data_points ? event.to_hash : @data_points).map do |k,v| 
      [event.sprintf(k), (String === v ? event.sprintf(v) : v)] 
    end ]
  end
  

  # Coerce values in the event data to their appropriate type. This requires 
  # foreknowledge of what's in the data point, which is less than ideal. An 
  # alternative is to use a `code` filter and manipulate the individual point's
  # data before sending to the output pipeline
  def coerce_values!(event_data)
    @coerce_values.each do |column, value_type|
      if event_data.has_key?(column)
        begin
          @logger.debug? and @logger.debug("Converting column #{column} to type #{value_type}: Current value: #{event_data[column]}")
          event_data[column] = coerce_value(value_type, event_data[column])

        rescue => e
          @logger.error("Unhandled exception", :error => e.message)
        end
      end
    end

    event_data
  end


  def coerce_value(value_type, value)
    case value_type.to_sym
    when :integer
      value.to_i
      
    when :float
      value.to_f

    when :string
      value.to_s
    
    else
      @logger.warn("Don't know how to convert to #{value_type}. Returning value unchanged")
      value  
    end
  end


  # Remove a set of fields from the event data before sending it to Influxdb. This
  # is useful for removing @timestamp, @version, etc
  def exclude_fields!(event_data)
    @exclude_fields.each { |field| event_data.delete(field) }
  end


  # Extract tags from a hash of fields. 
  # Returns a tuple containing a hash of tags (as configured by send_as_tags) 
  # and a hash of fields that exclude the tags. If fields contains a key 
  # "tags" with an array, they will be moved to the tags hash (and each will be
  # given a value of true)
  # 
  # Example: 
  #   # Given send_as_tags: ["bar"]
  #   original_fields = {"foo" => 1, "bar" => 2, "tags" => ["tag"]}
  #   tags, fields = extract_tags(original_fields)
  #   # tags: {"bar" => 2, "tag" => "true"} and fields: {"foo" => 1}
  def extract_tags(fields)
    remainder = fields.dup

    tags = if remainder.has_key?("tags") && remainder["tags"].respond_to?(:inject)
      remainder.delete("tags").inject({}) { |tags, tag| tags[tag] = "true"; tags }
    else
      {}
    end
    
    @send_as_tags.each { |key| (tags[key] = remainder.delete(key)) if remainder.has_key?(key) }

    tags.delete_if { |key,value| value.nil? || value == "" }
    remainder.delete_if { |key,value| value.nil? || value == "" }

    [tags, remainder]
  end


  # Returns the numeric value of the given timestamp in the requested precision.
  # precision must be one of the valid values for time_precision
  def timestamp_at_precision( timestamp, precision )
    multiplier = case precision
      when :h  then 1.0/3600
      when :m  then 1.0/60
      when :s  then 1
      when :ms then 1000
      when :u  then 1000000
    end
    
    (timestamp.to_f * multiplier).to_i
  end


  # Only read the response body if its status is not 1xx, 204, or 304. TODO: Should 
  # also not try reading the body if the request was a HEAD
  def read_body?( response )
    ! (response.nil? || [204,304].include?(response.code) || (100..199).include?(response.code))
  end


  # Return a quoted string of the given value if it's not a number
  def quoted(value)
    Numeric === value ? value : %Q|"#{value.gsub('"','\"')}"|
  end


  # Escape tag key, tag value, or field key
  def escaped(value)
    value.gsub(/[ ,=]/, ' ' => '\ ', ',' => '\,', '=' => '\=')
  end


  # Escape measurements note they don't need to worry about the '=' case
  def escaped_measurement(value)
    value.gsub(/[ ,]/, ' ' => '\ ', ',' => '\,')
  end


end # class LogStash::Outputs::InfluxDB
