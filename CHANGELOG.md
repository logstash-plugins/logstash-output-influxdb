## 3.0.0
 - Update to work with influxdb 0.9
 - breaking change, renaming 'series' to 'measurement'
 - breaking change for values of time_precision

## 2.0.0
 - Plugins were updated to follow the new shutdown semantic, this mainly allows Logstash to instruct input plugins to terminate gracefully, 
   instead of using Thread.raise on the plugins' threads. Ref: https://github.com/elastic/logstash/pull/3895
 - Dependency on logstash-core update to 2.0

