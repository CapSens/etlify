require "rails_helper"

RSpec.describe Etlify::Model do
  # ----------------------- Shared test doubles -----------------------
  let(:dummy_adapter) { Class.new }

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

  # ----------------------- Existing test suite -----------------------
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

      allow(described_class).to receive(:__included_klasses__)
        .and_return([klass])

      expect(described_class).to receive(:define_crm_dsl_on)
        .with(klass, :hubspot).and_call_original
      expect(described_class).to receive(:define_crm_instance_helpers_on)
        .with(klass, :hubspot).and_call_original

      described_class.install_dsl_for_crm(:hubspot)

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
        sync_dependencies: %w[company],
        sync_if: ->(r) { r.respond_to?(:active?) ? r.active? : true },
        job_class: "OverrideJob"
      )

      conf = klass.etlify_crms[:hubspot]
      expect(conf[:serializer]).to eq(dummy_serializer)
      expect(conf[:guard]).to be_a(Proc)
      expect(conf[:crm_object_type]).to eq(:contact)
      expect(conf[:id_property]).to eq(:external_id)
      expect(conf[:dependencies]).to eq(%i[company owner])
      expect(conf[:sync_dependencies]).to eq(%i[company])
      expect(conf[:adapter]).to eq(dummy_adapter)
      expect(conf[:job_class]).to eq("OverrideJob")
    end

    it "defaults sync_dependencies to an empty array when not provided" do
      klass = build_including_class
      described_class.define_crm_dsl_on(klass, :hubspot)

      klass.hubspot_etlified_with(
        serializer: dummy_serializer,
        crm_object_type: :contact,
        id_property: :external_id
      )

      conf = klass.etlify_crms[:hubspot]
      expect(conf[:sync_dependencies]).to eq([])
    end

    it "defaults sync_if to a proc returning true when not provided" do
      klass = build_including_class
      described_class.define_crm_dsl_on(klass, :hubspot)

      klass.hubspot_etlified_with(
        serializer: dummy_serializer,
        crm_object_type: :contact,
        id_property: :external_id
      )

      conf = klass.etlify_crms[:hubspot]
      expect(conf[:guard]).to be_a(Proc)
      expect(conf[:guard].call(double("record"))).to be true
      expect(klass.new.send(:allow_sync_for?, :hubspot)).to be true
    end

    it "does not clobber other CRM entries in etlify_crms" do
      klass = build_including_class
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

    it "stores stale_scope in configuration when provided" do
      klass = build_including_class
      described_class.define_crm_dsl_on(klass, :hubspot)

      scope_lambda = -> { where(active: true) }
      klass.hubspot_etlified_with(
        serializer: dummy_serializer,
        crm_object_type: :contact,
        id_property: :external_id,
        stale_scope: scope_lambda
      )

      conf = klass.etlify_crms[:hubspot]
      expect(conf[:stale_scope]).to eq(scope_lambda)
    end

    it "defaults stale_scope to nil when not provided" do
      klass = build_including_class
      described_class.define_crm_dsl_on(klass, :hubspot)

      klass.hubspot_etlified_with(
        serializer: dummy_serializer,
        crm_object_type: :contact,
        id_property: :external_id
      )

      conf = klass.etlify_crms[:hubspot]
      expect(conf[:stale_scope]).to be_nil
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
      expect(inst.hubspot_sync!(async: false, job_class: "X")).to eq(:done)
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

    it "defines registered_crms only if missing" do
      klass = build_including_class do
        # Predefine to assert no overwrite happens.
        def registered_crms
          ["predefined"]
        end
      end

      described_class.define_crm_instance_helpers_on(klass, :hubspot)
      expect(klass.new.registered_crms).to eq(["predefined"])
    end
  end

  describe "#build_crm_payload" do
    it "raises when crm_name is missing" do
      klass = build_including_class
      inst = klass.new
      expect { inst.build_crm_payload }.to(
        raise_error(ArgumentError, /crm_name is required/)
      )
    end

    it "raises when CRM is not configured" do
      klass = build_including_class
      inst = klass.new
      expect do
        inst.build_crm_payload(crm_name: :hubspot)
      end.to raise_error(ArgumentError, /crm not configured/)
    end

    it "uses as_crm_payload when available" do
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
      expect(inst.build_crm_payload(crm_name: :hubspot)).to(
        eq(email: "x@y", ok: true)
      )
    end

    it "falls back to to_h when as_crm_payload is missing" do
      serializer = Class.new do
        def initialize(_)
        end

        def to_h
          {via: :to_h}
        end
      end

      klass = build_including_class do
        define_singleton_method(:etlify_crms) do
          {
            hubspot: {
              serializer: serializer,
              guard: ->(_r) { true },
              crm_object_type: :contact,
              id_property: :external_id,
              adapter: Class.new,
            },
          }
        end
      end

      expect(klass.new.build_crm_payload(crm_name: :hubspot)).to(
        eq(via: :to_h)
      )
    end

    it "raises when serializer has neither as_crm_payload nor to_h" do
      # Define a constant so the class body can reference it reliably.
      stub_const("BadSerializer", Class.new do
        def initialize(_)
        end
      end)

      klass = build_including_class do
        def self.etlify_crms
          {
            hubspot: {
              serializer: BadSerializer,
              guard: ->(_r) { true },
              crm_object_type: :contact,
              id_property: :external_id,
              adapter: Class.new,
            },
          }
        end
      end

      expect do
        klass.new.build_crm_payload(crm_name: :hubspot)
      end.to raise_error(ArgumentError, /Serializer must implement/)
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

    it "requires crm_name" do
      inst = klass.new
      expect { inst.crm_sync! }.to(
        raise_error(ArgumentError, /crm_name is required/)
      )
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
      expect(job).to receive(:perform_later).with(klass.name, 42, "hubspot")
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
      expect(job).to receive(:perform_async).with(klass.name, 42, "hubspot")
      inst.crm_sync!(crm_name: :hubspot, async: true)
    end

    it "accepts override job_class as String and constantizes it" do
      stub_const("MyInlineJob", Class.new do
        def self.perform_later(*)
        end
      end)
      inst = klass.new
      allow(inst).to receive(:allow_sync_for?).and_return(true)

      expect(MyInlineJob).to receive(:perform_later)
        .with(klass.name, 42, "hubspot")

      inst.crm_sync!(
        crm_name: :hubspot,
        async: true,
        job_class: "MyInlineJob"
      )
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
  end

  describe "#crm_delete!" do
    it "requires crm_name" do
      klass = build_including_class
      inst = klass.new
      expect { inst.crm_delete! }.to(
        raise_error(ArgumentError, /crm_name is required/)
      )
    end

    it "delegates to Etlify::Deleter.call" do
      klass = build_including_class
      inst = klass.new
      expect(Etlify::Deleter).to receive(:call).with(inst, crm_name: :hubspot)
      inst.crm_delete!(crm_name: :hubspot)
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

    it "uses conf job_class Class without constantizing" do
      job_class = Class.new
      klass = build_including_class do
        define_singleton_method(:etlify_crms) do
          {hubspot: {job_class: job_class}}
        end
      end
      out = klass.new.send(:resolve_job_class_for, :hubspot, override: nil)
      expect(out).to eq(job_class)
    end

    it "falls back to Etlify::SyncJob when no override/conf job_class" do
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

  # ----------------------- AR-specific branches -----------------------
  context "with ActiveRecord models" do
    before(:all) do
      # Create a dedicated table for the AR model defined below.
      ActiveRecord::Schema.define do
        create_table :etlify_things, force: true do |t|
          t.string :name
          t.timestamps
        end
      end
    end

    after(:all) do
      # Keep the in-memory DB clean across random order runs.
      ActiveRecord::Base.connection.drop_table(:etlify_things)
    end

    class EtlifyThing < ApplicationRecord
      self.table_name = "etlify_things"
      include Etlify::Model
    end

    it "adds has_many :crm_synchronisations when included" do
      assoc = EtlifyThing.reflect_on_association(:crm_synchronisations)
      expect(assoc).not_to be_nil
      expect(assoc.macro).to eq(:has_many)

      # Verify the default scope actually orders by id ASC.
      thing = EtlifyThing.create!(name: "x")

      older = CrmSynchronisation.create!(
        crm_name: "hubspot",
        resource: thing
      )
      newer = CrmSynchronisation.create!(
        crm_name: "salesforce", # different crm_name to satisfy uniqueness
        resource: thing
      )

      # Should return records in ascending id order.
      expect(thing.crm_synchronisations.pluck(:id)).to eq([older.id, newer.id])
    end

    it "defines a filtered has_one for each CRM" do
      klass = Class.new(ApplicationRecord) do
        self.table_name = "etlify_things"
        include Etlify::Model
      end

      assoc = klass.reflect_on_association(:hubspot_crm_synchronisation)
      expect(assoc).not_to be_nil
      expect(assoc.macro).to eq(:has_one)

      expect(assoc.scope).to be_a(Proc)
    end

    it "crm_synced? reflects presence of a singular association" do
      inst = EtlifyThing.new
      # Simulate a singular method returning nil then a stub value.
      expect(inst.crm_synced?).to be false

      inst.define_singleton_method(:crm_synchronisation) { Object.new }
      expect(inst.crm_synced?).to be true
    end
  end
end
