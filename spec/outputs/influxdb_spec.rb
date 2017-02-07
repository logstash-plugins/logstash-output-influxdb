# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/influxdb"

describe LogStash::Outputs::InfluxDB do

  subject { LogStash::Outputs::InfluxDB.new(config) }
    
  context "validate minimal default config" do

    let(:config) do
    {
      "host" => "testhost",
      "use_event_fields_for_data_points" => true
    }
    end

    before do
      subject.register
      subject.close
    end

    it "sets correct influx client settings" do

      config = subject.instance_variable_get(:@influxdbClient).config

      expect(config.next_host).to eq "testhost"  
      expect(config.instance_variable_get(:@port)).to eq 8086 
      expect(config.instance_variable_get(:@time_precision)).to eq "ms" 
      expect(config.instance_variable_get(:@auth_method)).to eq "none".freeze 
      expect(config.instance_variable_get(:@initial_delay)).to eq 1
      expect(config.instance_variable_get(:@retry)).to eq 3
      expect(config.instance_variable_get(:@use_ssl)).to eq false
      expect(config.instance_variable_get(:@username)).to eq nil 
      expect(config.instance_variable_get(:@password)).to eq nil 

    end

  end

  context "validate non default config" do

    let(:config) do
    {
      "host" => "localhost",
      "use_event_fields_for_data_points" => true,
      "port" => 9999,
      "ssl" => true,
      "user" => "my_user",
      "password" => "my_pass",
      "initial_delay" => 5,
      "max_retries" => 8,
      "time_precision" => "s"
    }
    end

    before do
      subject.register
      subject.close
    end

    it "sets correct influx client settings" do
      config = subject.instance_variable_get(:@influxdbClient).config
      expect(config.instance_variable_get(:@port)).to eq 9999
      expect(config.instance_variable_get(:@time_precision)).to eq "s"   
      expect(config.instance_variable_get(:@initial_delay)).to eq 5
      expect(config.instance_variable_get(:@retry)).to eq 8
      expect(config.instance_variable_get(:@use_ssl)).to eq true    
      expect(config.instance_variable_get(:@username)).to eq "my_user"
      expect(config.instance_variable_get(:@password)).to eq "my_pass"  
      expect(config.instance_variable_get(:@auth_method)).to eq "params".freeze           
    end

  end
  
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

    before do
      subject.register
      # Added db name parameter to post - M.Laws
      allow(subject).to receive(:dowrite).with(result, "statistics")

      2.times do
        subject.receive(LogStash::Event.new("foo" => "1", "bar" => "2", "time" => "3", "type" => "generator"))
      end

      # Close / flush the buffer
      subject.close
    end

    #let(:result) { "logstash foo=\"1\",bar=\"2\" 3\nlogstash foo=\"1\",bar=\"2\" 3" }

    let(:result) {[{:series=>"logstash", :timestamp=>"3", :values=>{"foo"=>"1", "bar"=>"2"}}, {:series=>"logstash", :timestamp=>"3", :values=>{"foo"=>"1", "bar"=>"2"}}]}

    it "should receive 2 events, flush and call post with 2 items json array" do
      expect(subject).to have_received(:dowrite).with(result, "statistics")
    end

  end

  context "using event fields as data points" do

    let(:config) do
    {
      "host" => "localhost",
      "measurement" => "my_series",
      "allow_time_override" => true,
      "use_event_fields_for_data_points" => true
    }
    end

    before do
      subject.register
      # Added db name parameter to post - M.Laws
      allow(subject).to receive(:dowrite).with(result, "statistics")

      subject.receive(LogStash::Event.new("foo" => "1", "bar" => "2", "time" => "3", "type" => "generator"))

      # Close / flush the buffer
      subject.close
    end

    let(:result) {[{:series=>"my_series", :timestamp=>"3", :values=>{"foo"=>"1", "bar"=>"2"}}]}

    it "should use the event fields as the data points, excluding @version and @timestamp by default as well as any fields configured by exclude_fields" do
      expect(subject).to have_received(:dowrite).with(result, "statistics")
    end

  end


  context "sending some fields as Influxdb tags" do

    let(:config) do
    {
      "host" => "localhost",
      "measurement" => "my_series",
      "allow_time_override" => true,
      "use_event_fields_for_data_points" => true,
      "send_as_tags" => ["bar", "baz", "qux"]
    }
    end

    before do
      subject.register
      # Added db name parameter to post - M.Laws
      allow(subject).to receive(:dowrite).with(result, "statistics")

      subject.receive(LogStash::Event.new("foo" => "1", "bar" => "2", "baz" => "3", "time" => "4", "type" => "generator"))

      # Close / flush the buffer
      subject.close
    end

    let(:result) {[{:series=>"my_series", :timestamp=>"4", :tags=>{"bar"=>"2", "baz"=>"3"}, :values=>{"foo"=>"1"}}]}

    it "should use the event fields as the data points, excluding @version and @timestamp by default as well as any fields configured by exclude_fields" do
      expect(subject).to have_received(:dowrite).with(result, "statistics")
    end

  end

  context "when fields data contains a list of tags" do

    let(:config) do
    {
      "host" => "localhost",
      "measurement" => "my_series",
      "allow_time_override" => true,
      "use_event_fields_for_data_points" => true,
    }
    end

    before do
      subject.register
      # Added db name parameter to post - M.Laws
      allow(subject).to receive(:dowrite).with(result, "statistics")

      subject.receive(event)

      # Close / flush the buffer
      subject.close
    end
    
    let(:event) {LogStash::Event.new("foo" => "1", "time" => "2", "tags" => ["tagged"], "type" => "generator")}
    let(:result) {[{:series=>"my_series", :timestamp=>"2", :tags=>{"tagged"=>"true"}, :values=>{"foo"=>"1"}}]}

    it "should use the event fields as the data points, excluding @version and @timestamp by default as well as any fields configured by exclude_fields" do
      expect(subject).to have_received(:dowrite).with(result, "statistics")
    end

  end

  context "when fields are coerced to numerics" do

    let(:config) do
    {
      "host" => "localhost",
      "measurement" => "my_series",
      "allow_time_override" => true,
      "use_event_fields_for_data_points" => true,
      "coerce_values" => { "foo" => "integer", "bar" => "float" }
    }
    end

    before do
      subject.register
      # Added db name parameter to post - M.Laws
      allow(subject).to receive(:dowrite).with(result, "statistics")

      subject.receive(LogStash::Event.new("foo" => "1", "bar" => "2.0", "baz"=>"\\\"quotes\\\"", "time"=>3, "type" => "generator"))

      # Close / flush the buffer
    subject.close
    end

    let(:result) {[{:series=>"my_series", :timestamp=>3, :values=>{"foo"=>1, "bar"=>2.0, "baz"=>"\\\"quotes\\\"" }}]}

    it "should use the event fields as the data points, excluding @version and @timestamp by default as well as any fields configured by exclude_fields" do
      expect(subject).to have_received(:dowrite).with(result, "statistics")
    end

  end

  # Test issue #31 - Run "db" parameter through event.sprintf() to support...
  # -------------------------------------------------------------------------
  # This test is intended to verify writes to multiple measurements in A SINGLE
  # DATABASE continue to work *after* implementing #31.  Also verifies that
  # sprintf formatting is supported in the measurement name.
  context "receiving 3 points between 2 measurements in 1 database" do

    let(:config) do
    {
      "host" => "localhost",
      "measurement" => "%{baz}",
      "allow_time_override" => true,
      "use_event_fields_for_data_points" => true
    }
    end

    before do
      subject.register
      # Added db name parameter to post - M.Laws
      allow(subject).to receive(:dowrite)

      subject.receive(LogStash::Event.new("foo"=>"1", "bar"=>"2", "baz" => "m1", "time" => "1", "type" => "generator"))
      subject.receive(LogStash::Event.new("foo"=>"3", "bar"=>"4", "baz" => "m2", "time" => "2", "type" => "generator"))
      subject.receive(LogStash::Event.new("foo"=>"5", "bar"=>"6", "baz" => "m2", "time" => "3", "type" => "generator"))

      # Close / flush the buffer
      subject.close
    end

    let(:result) {[{:series=>"m1", :timestamp=>"1", :values=>{"foo"=>"1", "bar"=>"2", "baz" => "m1" }},{:series=>"m2", :timestamp=>"2", :values=>{"foo"=>"3", "bar"=>"4", "baz" => "m2"}},{:series=>"m2", :timestamp=>"3", :values=>{"foo"=>"5", "bar"=>"6", "baz" => "m2" }}]}

    it "should use the event fields as the data points, excluding @version and @timestamp by default as well as any fields configured by exclude_fields" do
      expect(subject).to have_received(:dowrite).with(result, "statistics")
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

    let(:config) do
    {
      "host" => "localhost",
      "db" => "%{bar}",
      "measurement" => "%{baz}",
      "allow_time_override" => true,
      "use_event_fields_for_data_points" => true,
    }
    end

    before do
      subject.register
      # Added db name parameter to post - M.Laws
      allow(subject).to receive(:dowrite)

      subject.receive(LogStash::Event.new("foo"=>"1", "bar"=>"db1", "baz" => "m1", "time" => "1", "type" => "generator"))
      subject.receive(LogStash::Event.new("foo"=>"2", "bar"=>"db1", "baz" => "m1", "time" => "2", "type" => "generator"))
      subject.receive(LogStash::Event.new("foo"=>"3", "bar"=>"db2", "baz" => "m2", "time" => "3", "type" => "generator"))
      subject.receive(LogStash::Event.new("foo"=>"4", "bar"=>"db2", "baz" => "m2", "time" => "4", "type" => "generator"))
      # Close / flush the buffer
      subject.close
    end

    let(:resultdb1) {[{:series=>"m1", :timestamp=>"1", :values=>{"foo"=>"1", "bar"=>"db1", "baz" => "m1" }},{:series=>"m1", :timestamp=>"2", :values=>{"foo"=>"2", "bar"=>"db1", "baz" => "m1" }}]}
    let(:resultdb2) {[{:series=>"m2", :timestamp=>"3", :values=>{"foo"=>"3", "bar"=>"db2", "baz" => "m2" }},{:series=>"m2", :timestamp=>"4", :values=>{"foo"=>"4", "bar"=>"db2", "baz" => "m2" }}]}

    it "should use the event fields as the data points, excluding @version and @timestamp by default as well as any fields configured by exclude_fields" do
      expect(subject).to have_received(:dowrite).with(resultdb1, "db1").once
      expect(subject).to have_received(:dowrite).with(resultdb2, "db2").once
    end


  end
end
