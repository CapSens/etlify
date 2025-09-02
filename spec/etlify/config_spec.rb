# frozen_string_literal: true

require "rails_helper"

RSpec.describe Etlify::Config do
  it "sets sane defaults and is mutable" do
    conf = described_class.new

    # Defaults: digest strategy, queue name, cache store.
    expect(conf.digest_strategy).to be_a(Method)
    expect(conf.job_queue_name).to eq("low")
    expect(conf.cache_store).to respond_to(:read)

    # Mutability check
    conf.job_queue_name = "default"
    expect(conf.job_queue_name).to eq("default")
  end

  it "is exposed via Etlify.config and configurable via .configure" do
    expect(Etlify.config).to be_a(described_class)

    Etlify.configure do |c|
      c.job_queue_name = "critical"
    end
    expect(Etlify.config.job_queue_name).to eq("critical")
  end
end
