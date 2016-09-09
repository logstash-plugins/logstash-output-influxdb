# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/influxdb"
require "manticore"

describe LogStash::Outputs::InfluxDB do

  let(:pipeline) { LogStash::Pipeline.new(config) }

  context "complete pipeline run with 2 events" do

    let(:config) do
      {
        "host" => "localhost",
        "user" => "someuser",
        "password" => "somepwd",
        "allow_time_override" => true,
        "data_points" => {
          "foo" => "%{foo}",
          "bar" => "%{bar}",
          "time" => "%{time}"
        }
      }
    end

    subject { LogStash::Outputs::InfluxDB.new(config) }

    before do
      subject.register
      # Added db name parameter to post - M.Laws
      allow(subject).to receive(:post).with(result, "statistics")

      2.times do
        subject.receive(LogStash::Event.new("foo" => "1", "bar" => "2", "time" => "3", "type" => "generator"))
      end

      # Close / flush the buffer
      subject.close
    end

    let(:result) { "logstash foo=\"1\",bar=\"2\" 3\nlogstash foo=\"1\",bar=\"2\" 3" }

    it "should receive 2 events, flush and call post with 2 items json array" do
      expect(subject).to have_received(:post).with(result, "statistics")
    end

  end

  context "using event fields as data points" do
    let(:config) do <<-CONFIG
        input {
           generator {
             message => "foo=1 bar=2 time=3"
             count => 1
             type => "generator"
           }
         }

         filter {
           kv { }
         }

         output {
           influxdb {
             host => "localhost"
             measurement => "my_series"
             allow_time_override => true
             use_event_fields_for_data_points => true
             exclude_fields => ["@version", "@timestamp", "sequence", "message", "type", "host"]
           }
         }
      CONFIG
    end

    let(:expected_url)  { 'http://localhost:8086/write?db=statistics&rp=default&precision=ms&u=&p='}
    let(:expected_body) { 'my_series bar="2",foo="1" 3' }

    it "should use the event fields as the data points, excluding @version and @timestamp by default as well as any fields configured by exclude_fields" do
      expect_any_instance_of(Manticore::Client).to receive(:post!).with(expected_url, body: expected_body)
      pipeline.run
    end
  end

  context "sending some fields as Influxdb tags" do
    let(:config) do <<-CONFIG
        input {
           generator {
             message => "foo=1 bar=2 baz=3 time=4"
             count => 1
             type => "generator"
           }
         }

         filter {
           kv { }
         }

         output {
           influxdb {
             host => "localhost"
             measurement => "my_series"
             allow_time_override => true
             use_event_fields_for_data_points => true
             exclude_fields => ["@version", "@timestamp", "sequence", "message", "type", "host"]
             send_as_tags => ["bar", "baz", "qux"]
           }
         }
      CONFIG
    end

    let(:expected_url)  { 'http://localhost:8086/write?db=statistics&rp=default&precision=ms&u=&p='}
    let(:expected_body) { 'my_series,bar=2,baz=3 foo="1" 4' }

    it "should send the specified fields as tags" do
      expect_any_instance_of(Manticore::Client).to receive(:post!).with(expected_url, body: expected_body)
      pipeline.run
    end
  end

  context "Escapeing space characters" do
    let(:config) do <<-CONFIG
        input {
           generator {
             message => "foo=1 bar=2 baz=3 time=4"
             count => 1
             type => "generator"
           }
         }

         filter {
           kv { 
	      add_field => {
		"test1" => "yellow cat"
		"test space" => "making life hard"
		"feild space" => "pink dog"
	      }
	   }
         }

         output {
           influxdb {
             host => "localhost"
             measurement => "my series"
             allow_time_override => true
             use_event_fields_for_data_points => true
             exclude_fields => ["@version", "@timestamp", "sequence", "message", "type", "host"]
             send_as_tags => ["bar", "baz", "test1", "test space"]
           }
         }
      CONFIG
    end

    let(:expected_url)  { 'http://localhost:8086/write?db=statistics&rp=default&precision=ms&u=&p='}
    let(:expected_body) { 'my\ series,bar=2,baz=3,test1=yellow\ cat,test\ space=making\ life\ hard foo="1",feild\ space="pink dog" 4' }

    it "should send the specified fields as tags" do
      expect_any_instance_of(Manticore::Client).to receive(:post!).with(expected_url, body: expected_body)
      pipeline.run
    end
  end

  context "Escapeing comma characters" do
    let(:config) do <<-CONFIG
        input {
           generator {
             message => "foo=1 bar=2 baz=3 time=4"
             count => 1
             type => "generator"
           }
         }

         filter {
           kv {
              add_field => {
                "test1" => "yellow, cat"
                "test, space" => "making, life, hard"
                "feild, space" => "pink, dog"
              }
           }
         }

         output {
           influxdb {
             host => "localhost"
             measurement => "my, series"
             allow_time_override => true
             use_event_fields_for_data_points => true
             exclude_fields => ["@version", "@timestamp", "sequence", "message", "type", "host"]
             send_as_tags => ["bar", "baz", "test1", "test, space"]
           }
         }
      CONFIG
    end

    let(:expected_url)  { 'http://localhost:8086/write?db=statistics&rp=default&precision=ms&u=&p='}
    let(:expected_body) { 'my\,\ series,bar=2,baz=3,test1=yellow\,\ cat,test\,\ space=making\,\ life\,\ hard foo="1",feild\,\ space="pink, dog" 4' }

    it "should send the specified fields as tags" do
      expect_any_instance_of(Manticore::Client).to receive(:post!).with(expected_url, body: expected_body)
      pipeline.run
    end
  end

  context "Escapeing equal characters" do
    let(:config) do <<-CONFIG
        input {
           generator {
             message => "foo=1 bar=2 baz=3 time=4"
             count => 1
             type => "generator"
           }
         }

         filter {
           kv {
              add_field => {
                "test1" => "yellow=cat"
                "test=space" => "making= life=hard"
                "feild= space" => "pink= dog"
              }
           }
         }

         output {
           influxdb {
             host => "localhost"
             measurement => "my=series"
             allow_time_override => true
             use_event_fields_for_data_points => true
             exclude_fields => ["@version", "@timestamp", "sequence", "message", "type", "host"]
             send_as_tags => ["bar", "baz", "test1", "test=space"]
           }
         }
      CONFIG
    end

    let(:expected_url)  { 'http://localhost:8086/write?db=statistics&rp=default&precision=ms&u=&p='}
    let(:expected_body) { 'my=series,bar=2,baz=3,test1=yellow\=cat,test\=space=making\=\ life\=hard foo="1",feild\=\ space="pink= dog" 4' }

    it "should send the specified fields as tags" do
      expect_any_instance_of(Manticore::Client).to receive(:post!).with(expected_url, body: expected_body)
      pipeline.run
    end
  end

  context "testing backslash characters" do
    let(:config) do <<-CONFIG
        input {
           generator {
             message => 'foo\\=1 bar=2 baz=3 time=4'
             count => 1
             type => "generator"
           }
         }

         filter {
           kv {
              add_field => {
                "test1" => "yellow=cat"
                "test=space" => "making=, life=hard"
                "feildspace" => 'C:\\Griffo'
              }
           }
         }

         output {
           influxdb {
             host => "localhost"
             measurement => 'my\\series'
             allow_time_override => true
             use_event_fields_for_data_points => true
             exclude_fields => ["@version", "@timestamp", "sequence", "message", "type", "host"]
             send_as_tags => ['bar', "baz", "test1", "test=space"]
           }
         }
      CONFIG
    end

    let(:expected_url)  { 'http://localhost:8086/write?db=statistics&rp=default&precision=ms&u=&p='}
    let(:expected_body) { 'my\series,bar=2,baz=3,test1=yellow\=cat,test\=space=making\=\,\ life\=hard foo\="1",feildspace="C:\Griffo" 4' }

    it "should send the specified fields as tags" do
      expect_any_instance_of(Manticore::Client).to receive(:post!).with(expected_url, body: expected_body)
      pipeline.run
    end
  end


  context "when fields data contains a list of tags" do
    let(:config) do <<-CONFIG
        input {
           generator {
             message => "foo=1 time=2"
             count => 1
             type => "generator"
           }
         }

         filter {
           kv { add_tag => [ "tagged" ] }
         }

         output {
           influxdb {
             host => "localhost"
             measurement => "my_series"
             allow_time_override => true
             use_event_fields_for_data_points => true
             exclude_fields => ["@version", "@timestamp", "sequence", "message", "type", "host"]
           }
         }
      CONFIG
    end

    let(:expected_url)  { 'http://localhost:8086/write?db=statistics&rp=default&precision=ms&u=&p='}
    let(:expected_body) { 'my_series,tagged=true foo="1" 2' }

    it "should move them to the tags data" do
      expect_any_instance_of(Manticore::Client).to receive(:post!).with(expected_url, body: expected_body)
      pipeline.run
    end
  end

  context "when fields are coerced to numerics" do
    let(:config) do <<-CONFIG
        input {
           generator {
             message => "foo=1 bar=2 baz=\\\"quotes\\\" time=3"
             count => 1
             type => "generator"
           }
         }

         filter {
           kv { }
         }

         output {
           influxdb {
             host => "localhost"
             measurement => "my_series"
             allow_time_override => true
             use_event_fields_for_data_points => true
             exclude_fields => ["@version", "@timestamp", "sequence", "message", "type", "host"]
             coerce_values => { "foo" => "integer" "bar" => "float" }
           }
         }
      CONFIG
    end

    let(:expected_url)  { 'http://localhost:8086/write?db=statistics&rp=default&precision=ms&u=&p='}
    let(:expected_body) { 'my_series bar=2.0,foo=1,baz="\\\"quotes\\\"" 3' } # We want the backslash and the escaped-quote in the request body

    it "should quote all other values (and escaping double quotes)" do
      expect_any_instance_of(Manticore::Client).to receive(:post!).with(expected_url, body: expected_body)
      pipeline.run
    end
  end

  # Test issue #32 - Add support for HTTPS via configuration
  # --------------------------------------------------------
  # A simple test to verify that setting the ssl configuration option works
  # similar to other Logstash output plugins (specifically the Elasticsearch
  # output plugin). 
  context "setting the ssl configuration option to true" do
    let(:config) do <<-CONFIG
        input {
           generator {
             message => "foo=1 bar=2 baz=3 time=4"
             count => 1
             type => "generator"
           }
         }

         filter {
           kv { }
         }

         output {
           influxdb {
             host => "localhost"
             ssl => true
             measurement => "barfoo"
             allow_time_override => true
             use_event_fields_for_data_points => true
             exclude_fields => [ "@version", "@timestamp", "sequence",
                                 "message", "type", "host" ]
           }
         }
      CONFIG
    end

    let(:expected_url)  { 'https://localhost:8086/write?db=statistics&rp=default&precision=ms&u=&p=' }
    let(:expected_body) { 'barfoo bar="2",foo="1",baz="3" 4' }

    it "should POST to an https URL" do
      expect_any_instance_of(Manticore::Client).to receive(:post!).with(expected_url, body: expected_body)
      pipeline.run
    end
  end

  # Test issue #31 - Run "db" parameter through event.sprintf() to support...
  # -------------------------------------------------------------------------
  # This test is intended to verify writes to multiple measurements in A SINGLE
  # DATABASE continue to work *after* implementing #31.  Also verifies that
  # sprintf formatting is supported in the measurement name.
  context "receiving 3 points between 2 measurements in 1 database" do
    let(:config) do <<-CONFIG
        input {
           generator {
             lines => [
               "foo=1 bar=2 baz=m1 time=1",
               "foo=3 bar=4 baz=m2 time=2",
               "foo=5 bar=6 baz=m2 time=3"
             ]
             count => 1
             type => "generator"
           }
         }

         filter {
           kv { }
         }

         output {
           influxdb {
             host => "localhost"
             db => "barfoo"
             measurement => "%{baz}"
             allow_time_override => true
             use_event_fields_for_data_points => true
             exclude_fields => [ "@version", "@timestamp", "sequence",
                                 "message", "type", "host" ]
           }
         }
      CONFIG
    end

    let(:expected_url)  { 'http://localhost:8086/write?db=barfoo&rp=default&precision=ms&u=&p=' }
    let(:expected_body) { "m1 bar=\"2\",foo=\"1\",baz=\"m1\" 1\nm2 bar=\"4\",foo=\"3\",baz=\"m2\" 2\nm2 bar=\"6\",foo=\"5\",baz=\"m2\" 3" }

    it "should result in a single POST (one per database)" do
      expect_any_instance_of(Manticore::Client).to receive(:post!).once
      pipeline.run
    end

    it "should POST in bulk format" do
      expect_any_instance_of(Manticore::Client).to receive(:post!).with(expected_url, body: expected_body)
      pipeline.run
    end
  end

  # Test issue #31 - Run "db" parameter through event.sprintf() to support...
  # -------------------------------------------------------------------------
  # This test is intended to verify writes to multiple measurements in MULTIPLE
  # DATABASES result in separate bulk POSTs (one for each database in the
  # buffer), and the correct measurements being written to the correct db.
  # Also verifies that sprintf formatting is correctly supported in the
  # database name.
  context "receiving 4 points between 2 measurements in 2 databases" do
    let(:config) do <<-CONFIG
        input {
           generator {
             lines => [
               "foo=1 bar=db1 baz=m1 time=1",
               "foo=2 bar=db1 baz=m2 time=2",
               "foo=3 bar=db2 baz=m1 time=3",
               "foo=4 bar=db2 baz=m2 time=4"
             ]
             count => 1
             type => "generator"
           }
         }

         filter {
           kv { }
         }

         output {
           influxdb {
             host => "localhost"
             db => "%{bar}"
             measurement => "%{baz}"
             allow_time_override => true
             use_event_fields_for_data_points => true
             exclude_fields => [ "@version", "@timestamp", "sequence",
                                 "message", "type", "host" ]
           }
         }
      CONFIG
    end

    let(:expected_url_db1)  { 'http://localhost:8086/write?db=db1&rp=default&precision=ms&u=&p=' }
    let(:expected_url_db2)  { 'http://localhost:8086/write?db=db2&rp=default&precision=ms&u=&p=' }
    let(:expected_body_db1) { "m1 bar=\"db1\",foo=\"1\",baz=\"m1\" 1\nm2 bar=\"db1\",foo=\"2\",baz=\"m2\" 2" }
    let(:expected_body_db2) { "m1 bar=\"db2\",foo=\"3\",baz=\"m1\" 3\nm2 bar=\"db2\",foo=\"4\",baz=\"m2\" 4" }

    it "should result in two POSTs (one per database)" do
      expect_any_instance_of(Manticore::Client).to receive(:post!).twice
      pipeline.run
    end

    it "should post in bulk format" do
      expect_any_instance_of(Manticore::Client).to receive(:post!).with(expected_url_db1, body: expected_body_db1)
      expect_any_instance_of(Manticore::Client).to receive(:post!).with(expected_url_db2, body: expected_body_db2)
      pipeline.run
    end
  end
end
