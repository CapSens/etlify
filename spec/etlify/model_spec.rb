# frozen_string_literal: true

require "rails_helper"

RSpec.describe Etlify::Model do
  # -- Test doubles -----------------------------------------------------------
  class TestAdapter
    def initialize(*)
    end
  end

  class TestSerializer
    def initialize(record)
      @record = record
    end

    def as_crm_payload
      {id: @record.id, kind: @record.class.name}
    end
  end

  class AltJob
    class << self
      attr_accessor :calls
      def perform_later(*args)
        (self.calls ||= []) << args
      end

      def reset!
        self.calls = []
      end
    end
  end

  # -- Registry isolation -----------------------------------------------------
  before do
    @registry_backup = Etlify::CRM.registry.dup
    Etlify::CRM.registry.clear
  end

  after do
    Etlify::CRM.registry.clear
    Etlify::CRM.registry.merge!(@registry_backup)
  end

  # Build a fresh anonymous model class and include the concern each time.
  def new_model_class
    Class.new do
      include Etlify::Model
      attr_reader :id
      def initialize(id:)
        @id = id
      end
    end
  end

  # Helper to register a CRM named :alpha.
  def register_alpha
    Etlify::CRM.register(
      :alpha,
      adapter: TestAdapter,
      options: {job_class: AltJob}
    )
  end

  # Helper to apply the DSL on a given class for :alpha.
  def dsl_apply(klass)
    klass.alpha_etlified_with(
      serializer: TestSerializer,
      crm_object_type: "contacts",
      id_property: "id",
      dependencies: %i[name email],
      sync_if: ->(r) { r.id.odd? },
      job_class: nil
    )
  end

  describe "inclusion and DSL installation" do
    it "installs DSL on include when a CRM is already registered",
       :aggregate_failures do
      register_alpha
      klass = new_model_class

      expect(klass).to respond_to(:alpha_etlified_with)
      inst = klass.new(id: 1)
      expect(inst).to respond_to(:alpha_build_payload)
      expect(inst).to respond_to(:alpha_sync!)
      expect(inst).to respond_to(:alpha_delete!)
    end

    it "installs DSL on classes already including the concern when a CRM " \
       "is registered afterwards", :aggregate_failures do
      klass = new_model_class
      expect(klass).not_to respond_to(:alpha_etlified_with)

      register_alpha

      expect(klass).to respond_to(:alpha_etlified_with)
      inst = klass.new(id: 1)
      expect(inst).to respond_to(:alpha_build_payload)
      expect(inst).to respond_to(:alpha_sync!)
      expect(inst).to respond_to(:alpha_delete!)
    end

    it "tracks including classes in __included_klasses__",
       :aggregate_failures do
      before_list = Etlify::Model.__included_klasses__.dup
      klass = new_model_class

      expect(Etlify::Model.__included_klasses__).to include(klass)
      expect(Etlify::Model.__included_klasses__.size)
        .to eq(before_list.size + 1)
    end
  end

  describe "DSL method <crm>_etlified_with" do
    it "stores per-CRM config on class.etlify_crms and symbolized deps",
       :aggregate_failures do
      register_alpha
      klass = new_model_class

      dsl_apply(klass)

      conf = klass.etlify_crms[:alpha]
      expect(conf[:serializer]).to eq(TestSerializer)
      expect(conf[:crm_object_type]).to eq("contacts")
      expect(conf[:id_property]).to eq("id")
      expect(conf[:dependencies]).to eq(%i[name email])
      expect(conf[:adapter]).to eq(TestAdapter)
      # job_class from registry when nil in DSL
      expect(conf[:job_class]).to eq(AltJob)
      # guard should be installed
      expect(conf[:guard]).to be_a(Proc)
    end
  end

  describe "instance helpers creation and delegation" do
    it "defines <crm>_build_payload / <crm>_sync! / <crm>_delete!",
       :aggregate_failures do
      register_alpha
      klass = new_model_class
      dsl_apply(klass)
      inst = klass.new(id: 7)

      expect(inst).to respond_to(:alpha_build_payload)
      expect(inst).to respond_to(:alpha_sync!)
      expect(inst).to respond_to(:alpha_delete!)
    end

    it "delegates to build_crm_payload / crm_sync! / crm_delete! with " \
       "the CRM name", :aggregate_failures do
      register_alpha
      klass = Class.new do
        include Etlify::Model
        attr_reader :id
        def initialize(id:)
          @id = id
        end

        # The generated helper calls build_crm_payload(crm: ...)
        def build_crm_payload(crm:)
          [:payload_called, crm]
        end

        def crm_sync!(crm:, async:, job_class:)
          [:sync_called, crm, async]
        end

        def crm_delete!(crm:)
          [:delete_called, crm]
        end
      end

      dsl_apply(klass)
      inst = klass.new(id: 3)

      expect(inst.alpha_build_payload).to eq([:payload_called, :alpha])
      expect(inst.alpha_sync!(async: false))
        .to eq([:sync_called, :alpha, false])
      expect(inst.alpha_delete!).to eq([:delete_called, :alpha])
    end
  end

  describe "#build_crm_payload via configured serializer" do
    it "uses serializer.as_crm_payload(record)", :aggregate_failures do
      register_alpha
      klass = new_model_class
      dsl_apply(klass)
      inst = klass.new(id: 11)

      out = inst.build_crm_payload(crm_name: :alpha)
      expect(out).to eq({id: 11, kind: klass.name})
    end
  end

  describe "#crm_sync! job dispatch and guards" do
    it "returns false when guard denies synchronization",
       :aggregate_failures do
      register_alpha
      klass = new_model_class
      # Guard false for even ids
      klass.alpha_etlified_with(
        serializer: TestSerializer,
        crm_object_type: "contacts",
        id_property: "id",
        dependencies: [],
        sync_if: ->(r) { r.id.odd? },
        job_class: nil
      )

      even = klass.new(id: 2)
      expect(even.crm_sync!(crm_name: :alpha, async: true)).to eq(false)
    end

    it "uses override job_class when provided", :aggregate_failures do
      register_alpha
      klass = new_model_class
      dsl_apply(klass)
      inst = klass.new(id: 5)

      AltJob.reset!
      class CustomJob
        class << self
          attr_accessor :args
          def perform_later(*a)
            self.args = a
          end
        end
      end

      inst.crm_sync!(
        crm_name: :alpha,
        async: true,
        job_class: CustomJob
      )

      expect(CustomJob.args).to eq([klass.name, 5, "alpha"])
    end

    it "falls back to registry job_class when no override is provided",
       :aggregate_failures do
      register_alpha
      klass = new_model_class
      dsl_apply(klass)
      inst = klass.new(id: 7)

      AltJob.reset!
      inst.crm_sync!(
        crm_name: :alpha,
        async: true
      )

      expect(AltJob.calls).to eq([[klass.name, 7, "alpha"]])
    end

    it "runs inline when async: false by calling Synchronizer",
       :aggregate_failures do
      register_alpha
      klass = new_model_class
      dsl_apply(klass)
      inst = klass.new(id: 9)

      expect(Etlify::Synchronizer).to receive(:call).with(
        inst,
        crm_name: :alpha
      ).and_return(:synced)

      res = inst.crm_sync!(
        crm_name: :alpha,
        async: false
      )
      expect(res).to eq(:synced)
    end
  end
end
