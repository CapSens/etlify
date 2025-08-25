# frozen_string_literal: true

require "rails_helper"
require "fileutils"
require "tmpdir"

RSpec.describe Etlify::Generators::InstallGenerator, type: :generator do
  around do |example|
    Dir.mktmpdir do |dir|
      @tmp = dir
      Dir.chdir(@tmp) { example.run }
    end
  end

  def build_generator
    described_class.new(
      [],                      # args
      {},                      # options
      destination_root: @tmp   # thor config
    )
  end

  def generated_initializer_path
    File.join(@tmp, "config/initializers/etlify.rb")
  end

  it "creates config/initializers/etlify.rb",
     :aggregate_failures do
    gen = build_generator
    gen.invoke_all

    expect(File.exist?(generated_initializer_path)).to be(true)

    content = File.read(generated_initializer_path)
    expect(content).to include("Etlify.configure do |config|")
    expect(content).to include("Etlify::CRM.register(")
    expect(content).to include("adapter: Etlify::Adapters::HubspotV3Adapter")
    expect(content).to include("options: { job_class: Etlify::SyncJob }")
    expect(content).to include("# @job_queue_name = \"low\"")
    expect(content).to include(
      "# @digest_strategy = Etlify::Digest.method(:stable_sha256)"
    )
    expect(content).to include("# @cache_store = Rails.cache")
  end

  it "the generated initializer configures default HubSpot mapping by " \
     "calling Etlify::CRM.register",
     :aggregate_failures do
    gen = build_generator
    gen.invoke_all

    # Préserve et restaure le registre pour ne pas polluer d'autres specs
    begin
      original = Etlify::CRM.registry.dup

      expect(Etlify::CRM).to receive(:register).with(
        :hubspot,
        adapter: Etlify::Adapters::HubspotV3Adapter,
        options: {job_class: Etlify::SyncJob}
      )

      # Charge le fichier généré (exécute Etlify.configure + register)
      load generated_initializer_path
    ensure
      Etlify::CRM.registry.clear
      Etlify::CRM.registry.merge!(original)
    end
  end

  it "est chargeable plusieurs fois sans exploser (idempotent à l'exécution)",
     :aggregate_failures do
    gen = build_generator
    gen.invoke_all

    # On autorise plusieurs register identiques ; on stub pour le vérifier
    allow(Etlify::CRM).to receive(:register)

    expect do
      load generated_initializer_path
      load generated_initializer_path
    end.not_to raise_error

    expect(Etlify::CRM).to have_received(:register).twice
  end
end
