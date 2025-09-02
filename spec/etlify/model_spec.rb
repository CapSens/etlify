require "rails_helper"

RSpec.describe Etlify::Model do
  # Simple fake adapter used by the registry
  let(:dummy_adapter) do
    Class.new
  end

  # Minimal serializer returning a hash payload
  let(:dummy_serializer) do
    Class.new do
      def initialize(record)
        @record = record
      end

      def as_crm_payload
        {ok: true, id: (@record.respond_to?(:id) ? @record.id : nil)}
      end
    end
  end

  before do
    # Stub CRM registry for deterministic behavior in all examples
    reg_item = Etlify::CRM::RegistryItem.new(
      name: :hubspot,
      adapter: dummy_adapter,
      options: {job_class: "DefaultJobFromRegistry"}
    )
    allow(Etlify::CRM).to receive(:fetch).with(:hubspot).and_return(reg_item)
    allow(Etlify::CRM).to receive(:names).and_return([:hubspot])
  end

  # Helper: build a plain class including the concern
  def build_including_class(&blk)
    Class.new do
      include Etlify::Model
      class_eval(&blk) if blk
    end
  end

  describe "included hook" do
    it "tracks including classes and defines class_attribute" do
      klass = build_including_class
      expect(Etlify::Model.__included_klasses__).to include(klass)
      expect(klass.respond_to?(:etlify_crms)).to be true
      expect(klass.etlify_crms).to eq({})
    end

    it "defines instance helpers for already-registered CRMs" do
      klass = build_including_class
      expect(klass.instance_methods).to include(:hubspot_build_payload)
      expect(klass.instance_methods).to include(:hubspot_sync!)
      expect(klass.instance_methods).to include(:hubspot_delete!)
    end
  end

  describe ". __included_klasses__" do
    it "returns the same memoized array across calls" do
      arr1 = described_class.__included_klasses__
      arr2 = described_class.__included_klasses__
      expect(arr1.object_id).to eq(arr2.object_id)
    end
  end

  describe ".install_dsl_for_crm" do
    it "reinstalls DSL and helpers on all previous classes" do
      klass = build_including_class

      # Make the call deterministic: only our klass is considered
      allow(described_class).to receive(:__included_klasses__)
        .and_return([klass])

      # We expect the installer to call the two Model methods, even if
      # they early-return because definitions already exist.
      expect(described_class).to receive(:define_crm_dsl_on)
        .with(klass, :hubspot).and_call_original
      expect(described_class).to receive(:define_crm_instance_helpers_on)
        .with(klass, :hubspot).and_call_original

      described_class.install_dsl_for_crm(:hubspot)

      # Methods should still be present after reinstall.
      expect(klass.respond_to?(:hubspot_etlified_with)).to be true
      expect(klass.instance_methods).to include(:hubspot_build_payload)
    end
  end

  describe ".define_crm_dsl_on" do
    it "is a no-op if the DSL method already exists" do
      klass = Class.new do
        def self.hubspot_etlified_with(**)
        end
      end
      expect do
        described_class.define_crm_dsl_on(klass, :hubspot)
      end.not_to change {
        klass.singleton_methods.include?(:hubspot_etlified_with)
      }
    end

    it "defines <crm>_etlified_with and stores full configuration" do
      klass = build_including_class
      described_class.define_crm_dsl_on(klass, :hubspot)

      klass.hubspot_etlified_with(
        serializer: dummy_serializer,
        crm_object_type: :contact,
        id_property: :external_id,
        dependencies: %w[company owner],
        sync_if: ->(r) { r.respond_to?(:active?) ? r.active? : true },
        job_class: "OverrideJob"
      )

      conf = klass.etlify_crms[:hubspot]
      expect(conf[:serializer]).to eq(dummy_serializer)
      expect(conf[:guard]).to be_a(Proc)
      expect(conf[:crm_object_type]).to eq(:contact)
      expect(conf[:id_property]).to eq(:external_id)
      expect(conf[:dependencies]).to eq(%i[company owner])
      expect(conf[:adapter]).to eq(dummy_adapter)
      expect(conf[:job_class]).to eq("OverrideJob")
    end

    it "defaults sync_if to a proc returning true when not provided" do
      klass = build_including_class
      described_class.define_crm_dsl_on(klass, :hubspot)

      # Call DSL without sync_if keyword
      klass.hubspot_etlified_with(
        serializer: dummy_serializer,
        crm_object_type: :contact,
        id_property: :external_id
      )

      conf = klass.etlify_crms[:hubspot]
      expect(conf[:guard]).to be_a(Proc)

      # By default, it should always return true
      expect(conf[:guard].call(double("record"))).to be true
      expect(klass.new.send(:allow_sync_for?, :hubspot)).to be true
    end

    it "does not clobber other CRM entries in etlify_crms" do
      klass = build_including_class
      # Use the real writer so the attribute can be updated by the DSL
      klass.etlify_crms = {salesforce: {anything: 1}}

      described_class.define_crm_dsl_on(klass, :hubspot)
      klass.hubspot_etlified_with(
        serializer: dummy_serializer,
        crm_object_type: :contact,
        id_property: :external_id
      )

      expect(klass.etlify_crms.keys).to include(:salesforce, :hubspot)
    end

    it "propagates errors from Etlify::CRM.fetch" do
      klass = build_including_class
      allow(Etlify::CRM).to receive(:fetch).and_raise("boom")
      described_class.define_crm_dsl_on(klass, :hubspot)
      expect do
        klass.hubspot_etlified_with(
          serializer: dummy_serializer,
          crm_object_type: :contact,
          id_property: :external_id
        )
      end.to raise_error(RuntimeError, "boom")
    end
  end

  describe ".define_crm_instance_helpers_on" do
    it "creates helpers only if missing (idempotent)" do
      klass = build_including_class
      methods_before = klass.instance_methods.grep(/hubspot_/)
      described_class.define_crm_instance_helpers_on(klass, :hubspot)
      methods_after = klass.instance_methods.grep(/hubspot_/)
      expect(methods_after).to include(*methods_before)
    end

    it "delegates payload helper with crm_name keyword" do
      # Capture the keywords passed to build_crm_payload
      klass = build_including_class do
        attr_reader :seen_kw
        def build_crm_payload(**kw)
          @seen_kw = kw
          :ok
        end
      end
      inst = klass.new
      expect(inst.hubspot_build_payload).to eq(:ok)
      expect(inst.seen_kw).to eq({crm_name: :hubspot})
    end

    it "delegates sync helper with crm_name, async, job_class" do
      klass = build_including_class do
        attr_reader :seen_sync
        def crm_sync!(**kw)
          @seen_sync = kw
          :done
        end
      end
      inst = klass.new
      expect(
        inst.hubspot_sync!(async: false, job_class: "X")
      ).to eq(:done)
      expect(inst.seen_sync).to eq(
        {crm_name: :hubspot, async: false, job_class: "X"}
      )
    end

    it "delegates delete helper with crm_name keyword" do
      klass = build_including_class do
        attr_reader :seen_del
        def crm_delete!(**kw)
          @seen_del = kw
          :deleted
        end
      end
      inst = klass.new
      expect(inst.hubspot_delete!).to eq(:deleted)
      expect(inst.seen_del).to eq({crm_name: :hubspot})
    end
  end

  describe "#build_crm_payload" do
    it "raises when CRM is not configured" do
      klass = build_including_class
      inst = klass.new
      expect do
        inst.build_crm_payload(crm_name: :hubspot)
      end.to raise_error(ArgumentError, /crm not configured/)
    end

    it "works with crm_name: and returns serializer payload" do
      klass = build_including_class do
        def self.etlify_crms
          {
            hubspot: {
              serializer: Class.new do
                def initialize(_)
                end

                def as_crm_payload
                  {email: "x@y", ok: true}
                end
              end,
              guard: ->(_r) { true },
              crm_object_type: :contact,
              id_property: :external_id,
              adapter: Class.new,
            },
          }
        end
      end
      inst = klass.new
      expect(inst.build_crm_payload(crm_name: :hubspot))
        .to eq(email: "x@y", ok: true)
    end

    it "also accepts legacy crm: keyword" do
      klass = build_including_class do
        def self.etlify_crms
          {
            hubspot: {
              serializer: Class.new do
                def initialize(_)
                end

                def as_crm_payload
                  {legacy: true}
                end
              end,
              guard: ->(_r) { true },
              crm_object_type: :contact,
              id_property: :external_id,
              adapter: Class.new,
            },
          }
        end
      end
      inst = klass.new
      expect(inst.build_crm_payload(crm: :hubspot)).to eq(legacy: true)
    end
  end

  describe "#crm_sync!" do
    let(:klass) do
      build_including_class do
        attr_reader :id
        def initialize
          @id = 42
        end

        def self.etlify_crms
          {
            hubspot: {
              serializer: Class.new do
  def initialize(*)
  end

  def as_crm_payload
    {}
  end
end,
              guard: ->(_r) { true },
              crm_object_type: :contact,
              id_property: :external_id,
              adapter: Class.new,
            },
          }
        end
      end
    end

    it "returns false when guard forbids sync" do
      inst = klass.new
      allow(inst).to receive(:allow_sync_for?).with(:hubspot).and_return(false)
      expect(inst.crm_sync!(crm_name: :hubspot)).to be false
    end

    it "enqueues with perform_later when available" do
      job = Class.new do
  def self.perform_later(*)
  end
end
      inst = klass.new
      allow(inst).to receive(:allow_sync_for?).and_return(true)
      allow(inst).to receive(:resolve_job_class_for).and_return(job)
      expect(job).to receive(:perform_later)
        .with(klass.name, 42, "hubspot")
      inst.crm_sync!(crm_name: :hubspot, async: true)
    end

    it "enqueues with perform_async when available" do
      job = Class.new do
  def self.perform_async(*)
  end
end
      inst = klass.new
      allow(inst).to receive(:allow_sync_for?).and_return(true)
      allow(inst).to receive(:resolve_job_class_for).and_return(job)
      expect(job).to receive(:perform_async)
        .with(klass.name, 42, "hubspot")
      inst.crm_sync!(crm_name: :hubspot, async: true)
    end

    it "raises when no job API is available" do
      job = Class.new
      inst = klass.new
      allow(inst).to receive(:allow_sync_for?).and_return(true)
      allow(inst).to receive(:resolve_job_class_for).and_return(job)
      expect do
        inst.crm_sync!(crm_name: :hubspot, async: true)
      end.to raise_error(ArgumentError, /No job class available/)
    end

    it "runs inline with Synchronizer when async: false" do
      inst = klass.new
      allow(inst).to receive(:allow_sync_for?).and_return(true)
      expect(Etlify::Synchronizer).to receive(:call)
        .with(inst, crm_name: :hubspot)
      inst.crm_sync!(crm_name: :hubspot, async: false)
    end

    it "accepts job_class override as String and constantizes it" do
      job = Class.new do
  def self.perform_later(*)
  end
end
      stub_const("MyInlineJob", job)
      inst = klass.new
      allow(inst).to receive(:allow_sync_for?).and_return(true)
      expect(job).to receive(:perform_later)
      inst.crm_sync!(crm_name: :hubspot, async: true, job_class: "MyInlineJob")
    end

    it "also accepts legacy crm: keyword" do
      job = Class.new do
  def self.perform_later(*)
  end
end
      inst = klass.new
      allow(inst).to receive(:allow_sync_for?).and_return(true)
      allow(inst).to receive(:resolve_job_class_for).and_return(job)
      expect(job).to receive(:perform_later)
        .with(klass.name, 42, "hubspot")
      inst.crm_sync!(crm: :hubspot, async: true)
    end
  end

  describe "#crm_delete!" do
    it "delegates to Etlify::Deleter.call" do
      klass = build_including_class
      inst = klass.new
      expect(Etlify::Deleter).to receive(:call).with(inst, crm_name: :hubspot)
      inst.crm_delete!(crm_name: :hubspot)
    end

    it "also accepts legacy crm: keyword" do
      klass = build_including_class
      inst = klass.new
      expect(Etlify::Deleter).to receive(:call).with(inst, crm_name: :hubspot)
      inst.crm_delete!(crm: :hubspot)
    end
  end

  describe "#allow_sync_for?" do
    it "returns false when CRM conf is missing" do
      klass = build_including_class
      expect(klass.new.send(:allow_sync_for?, :hubspot)).to be false
    end

    it "returns true when guard is nil" do
      klass = build_including_class do
        def self.etlify_crms
          {hubspot: {guard: nil}}
        end
      end
      expect(klass.new.send(:allow_sync_for?, :hubspot)).to be true
    end

    it "evaluates guard proc with the instance" do
      klass = build_including_class
      outer = klass
      klass.define_singleton_method(:etlify_crms) do
        {hubspot: {guard: ->(r) { r.is_a?(outer) }}}
      end
      expect(klass.new.send(:allow_sync_for?, :hubspot)).to be true
    end

    it "returns false when guard returns false" do
      klass = build_including_class do
        def self.etlify_crms
          {hubspot: {guard: ->(_r) { false }}}
        end
      end
      expect(klass.new.send(:allow_sync_for?, :hubspot)).to be false
    end
  end

  describe "#resolve_job_class_for and #constantize_if_needed" do
    it "returns override class as-is" do
      klass = build_including_class
      job = Class.new
      out = klass.new.send(:resolve_job_class_for, :hubspot, override: job)
      expect(out).to eq(job)
    end

    it "falls back to Etlify::SyncJob when no override or conf job_class is given" do
      klass = build_including_class do
        def self.etlify_crms
          {hubspot: {}}
        end
      end
      stub_const("Etlify::SyncJob", Class.new)
      out = klass.new.send(:resolve_job_class_for, :hubspot, override: nil)
      expect(out).to eq(Etlify::SyncJob)
    end

    it "constantizes override String" do
      klass = build_including_class
      stub_const("MyConstJob", Class.new)
      out = klass.new.send(:resolve_job_class_for, :hubspot,
                           override: "MyConstJob")
      expect(out).to eq(MyConstJob)
    end

    it "uses conf job_class String, else falls back to SyncJob" do
      klass = build_including_class do
        def self.etlify_crms
          {hubspot: {job_class: "Etlify::SyncJob"}}
        end
      end
      stub_const("Etlify::SyncJob", Class.new)
      out = klass.new.send(:resolve_job_class_for, :hubspot, override: nil)
      expect(out).to eq(Etlify::SyncJob)
    end
  end

  describe "#raise_unless_crm_is_configured" do
    it "raises with informative message when not configured" do
      klass = build_including_class
      expect do
        klass.new.send(:raise_unless_crm_is_configured, :hubspot)
      end.to raise_error(ArgumentError, /crm not configured for hubspot/)
    end

    it "does not raise when configuration is present" do
      klass = build_including_class do
        def self.etlify_crms
          {hubspot: {}}
        end
      end
      expect do
        klass.new.send(:raise_unless_crm_is_configured, :hubspot)
      end.not_to raise_error
    end
  end
end
