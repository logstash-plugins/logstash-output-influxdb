# logstash-output-influxdb
This fork permits to create fields on the fly, accrding to fields names and datatypes
that arrives to that output plugin.

I added 2 configuration paramters:

  # This settings revokes the needs to use data_points and coerce_values configuration
  # to create appropriate insert to influxedb. Should be used with fields_to_skip configuration
  # This setting sets data points (column) names as field name from arrived to plugin event,
  # value for data points 
  config :use_event_fields_for_data_points, :validate => :boolean, :default => true
  
  # The array with keys to delete from future processing.
  # By the default event that arrived to the output plugin contains keys "@version", "@timestamp"
  # and can contains another fields like, for example, "command" that added by input plugin EXEC.
  # Of course we doesn't needs those fields to be processed and inserted to influxdb when configuration
  # use_event_fields_for_data_points is true. 
  # We doesn't deletes the keys from event, we creates new Hash from event and after that, we deletes unwanted
  # keys.
  
  config :fields_to_skip, :validate => :array, :default => []
  
  This is my example config file:
  I'm retrieving different number of fields with differnt names from CPU, memory, disks, but 
  I doesn't need defferent configuration per data type as in master branch.
  I'm creating relevant fields names and datatypes on filter stage and just skips the unwanted fields in outputv plugin.
  
  input {
        exec {
              command => "env LANG=C sar -P ALL 1 1|egrep -v '^$|Average|CPU'"
              type => "system.cpu"
              interval => 1
        }
        exec {
              command => "env LANG=C sar -r 1 1|egrep -v '^$|Average|memfree|CPU'"
              type => "system.memory"
              interval => 1
        }
		exec {
              command => "env LANG=C sar -pd 1 1|egrep -v '^$|Average|DEV|CPU'"
              type => "system.disks"
              interval => 1
}

filter {
        if [type] == "system.cpu" {
			split {}
			grok {
					match => { "message" => "\A(?<sar_time>%{HOUR}:%{MINUTE}:%{SECOND})\s+%{DATA:cpu}\s+%{NUMBER:user:float}\s+%{NUMBER:nice:float}\s+%{NUMBER:system:float}\s+%{NUMBER:iowait:float}\s+%{NUMBER:steal:float}\s+%{NUMBER:idle:float}\z" }
					remove_field => [ "message" ]
                    add_field => {"series_name" => "%{host}.%{type}.%{cpu}"}
			}
			ruby {
					code => " event['usage'] = (100 - event['idle']).round(2); event['usage-io'] = event['usage'] - event['iowait']"
			}
        }
        if [type] == "system.memory" {
			split {}
			grok {
					match => { "message" => "\A(?<sar_time>%{HOUR}:%{MINUTE}:%{SECOND})\s+%{NUMBER:kbmemfree:float}\s+%{NUMBER:kbmemused:float}\s+%{NUMBER:percenmemused:float}\s+%{NUMBER:kbbuffers:float}\s+%{NUMBER:kbcached:float}\s+%{NUMBER:kbcommit:float}\s+%{NUMBER:kpercentcommit:float}\z" }
					remove_field => [ "message" ]
					add_field => {"series_name" => "%{host}.%{type}"}
			}
			ruby {
					code => " event['kbtotalmemory'] = (event['kbmemfree'] + event['kbmemused']);event['kbnetoused'] = (event['kbmemused'] - (event['kbbuffers'] + event['kbcached']));event['kbnetofree'] = (event['kbmemfree'] + (event['kbbuffers'] + event['kbcached']))"
			}
        }
        if [type] == "system.disks" {
			split {}
			grok {
					match => { "message" => "\A(?<sar_time>%{HOUR}:%{MINUTE}:%{SECOND})\s+%{DATA:disk}\s+%{NUMBER:tps:float}\s+%{NUMBER:rd_sec_s:float}\s+%{NUMBER:wr_sec_s:float}\s+%{NUMBER:avgrq-sz:float}\s+%{NUMBER:avgqu-sz:float}\s+%{NUMBER:await:float}\s+%{NUMBER:svctm:float}\s+%{NUMBER:percenutil:float}\z" }
					remove_field => [ "message" ]
					add_field => {"series_name" => "%{host}.%{type}.%{disk}"}
			}

        }
		ruby {
			code => "event['time'] = (DateTime.parse(event['sar_time']).to_time.to_i ) - 7200"
		}		
}
output {

                       influxdb {
                        host => "172.20.90.72"
                        password => "root"
                        user => "root"
                        db => "metrics"
						allow_time_override => true
                        time_precision => "s"
						series => "%{series_name}"
                        use_event_fields_for_data_points => true
                        fields_to_skip => ["@version","@timestamp","type","host","command","sar_time","series_name"]
                        }

        stdout { codec => rubydebug
                 workers => 4
        }
}
