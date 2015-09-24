require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/influxdb"

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
end
