## 5.0.0 
 - Use the official influxdb client. This doesn't change the config options, but has some
   breakage potential, so this is a major version bump.

## 4.0.0
 - Breaking: Updated plugin to use new Java Event APIs
 - Fix the suite related to the new java event and the ordering of the keys in the hash.
 - Relax logstash-core-plugin-api constraints
 - update .travis.yml

## 3.1.2
 - Depend on logstash-core-plugin-api instead of logstash-core, removing the need to mass update plugins on major releases of logstash

## 3.1.1
 - New dependency requirements for logstash-core for the 5.0 release

## 3.1.0
 - New option to enable SSL/TLS encrypted communication to InfluxDB
 - DB parameter now supports sprintf formatting

## 3.0.0
 - Update to work with influxdb 0.9
 - breaking change, renaming 'series' to 'measurement'
 - breaking change for values of time_precision
 - Special characters now properly escaped in tag key/value, field key, measurement

## 2.0.0
 - Plugins were updated to follow the new shutdown semantic, this mainly allows Logstash to instruct input plugins to terminate gracefully, 
   instead of using Thread.raise on the plugins' threads. Ref: https://github.com/elastic/logstash/pull/3895
 - Dependency on logstash-core update to 2.0

