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
      allow(subject).to receive(:post).with(json_result)

      2.times do
        subject.receive(LogStash::Event.new("foo" => "1", "bar" => "2", "time" => "3", "type" => "generator"))
      end

      # Close / flush the buffer
      subject.close
    end

    let(:json_result) { "[{\"name\":\"logstash\",\"columns\":[\"foo\",\"bar\",\"time\"],\"points\":[[\"1\",\"2\",\"3\"],[\"1\",\"2\",\"3\"]]}]" }

    it "should receive 2 events, flush and call post with 2 items json array" do
      expect(subject).to have_received(:post).with(json_result)
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
    let(:expected_body) { 'my_series foo="1",bar="2" 3' }

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
    let(:expected_body) { 'my_series foo=1,bar=2.0,baz="\\\"quotes\\\"" 3' } # We want the backslash and the escaped-quote in the request body

    it "should quote all other values (and escaping double quotes)" do
      expect_any_instance_of(Manticore::Client).to receive(:post!).with(expected_url, body: expected_body)
      pipeline.run
    end
  end
end
