require "rails_helper"

RSpec.describe Etlify::Model do
  include ActiveJob::TestHelper

  before do
    ActiveJob::Base.queue_adapter = :test
    clear_enqueued_jobs
    clear_performed_jobs
  end

  module Etlify
    module Serializers
      class TestUserSerializer < BaseSerializer
        def as_crm_payload
          { id: record.id, email: record.email }
        end
      end

      class TestUserToHSerializer < BaseSerializer
        def to_h
          { id: record.id, email: record.email }
        end
      end

      class TestUserNoPayloadSerializer < BaseSerializer
      end
    end
  end

  class TestUser < ActiveRecord::Base
    self.table_name = "users"
    include Etlify::Model
    belongs_to :company, optional: true

    etlified_with(
      serializer: Etlify::Serializers::TestUserSerializer,
      crm_object_type: "contacts",
      id_property: :id,
      sync_if: ->(u) { u.email.present? }
    )

    def crm_object_type
      "contacts"
    end
  end

  class TestUserToH < ActiveRecord::Base
    self.table_name = "users"
    include Etlify::Model
    belongs_to :company, optional: true

    etlified_with(
      serializer: Etlify::Serializers::TestUserToHSerializer,
      crm_object_type: "contacts",
      id_property: :id,
      sync_if: ->(u) { u.email.present? }
    )

    def crm_object_type
      "contacts"
    end
  end

  class TestUserNoPayload < ActiveRecord::Base
    self.table_name = "users"
    include Etlify::Model
    belongs_to :company, optional: true

    etlified_with(
      serializer: Etlify::Serializers::TestUserNoPayloadSerializer,
      crm_object_type: "contacts",
      id_property: :id,
      sync_if: ->(u) { u.email.present? }
    )

    def crm_object_type
      "contacts"
    end
  end

  class GuardedUser < ActiveRecord::Base
    self.table_name = "users"
    include Etlify::Model

    etlified_with(
      serializer: Etlify::Serializers::TestUserSerializer,
      crm_object_type: "contacts",
      id_property: :id,
      sync_if: ->(_u) { false }
    )

    def crm_object_type
      "contacts"
    end
  end

  let!(:user) { TestUser.create!(email: "john@example.com", full_name: "John") }

  describe ".crm_synced" do
    it "declares the class_attributes and the has_one association correctly" do
      expect(TestUser.respond_to?(:etlify_serializer)).to be true
      expect(TestUser.etlify_serializer).to eq(Etlify::Serializers::TestUserSerializer)

      expect(TestUser.respond_to?(:etlify_guard)).to be true
      expect(TestUser.etlify_guard).to be_a(Proc)

      reflection = TestUser.reflect_on_association(:crm_synchronisation)
      expect(reflection.macro).to eq(:has_one)
      expect(reflection.options[:as]).to eq(:resource)
      expect(reflection.options[:dependent]).to eq(:destroy)
      expect(reflection.options[:class_name]).to eq("CrmSynchronisation")
    end
  end

  describe "#crm_synced?" do
    it "returns false without a sync record, then true after creation" do
      expect(user.crm_synced?).to be false

      CrmSynchronisation.create!(
        resource_type: "TestUser",
        resource_id: user.id
      )

      expect(user.reload.crm_synced?).to be true
    end
  end

  describe "#build_crm_payload" do
    it "uses the configured serializer and returns a stable Hash" do
      payload = user.build_crm_payload
      expect(payload).to include(id: user.id, email: "john@example.com")
    end

    it "falls back to to_h when as_crm_payload is not implemented" do
      user = TestUserToH.create!(email: "toh@example.com", full_name: "To H")
      payload = user.build_crm_payload
      expect(payload).to include(id: user.id, email: "toh@example.com", source: "to_h")
    end

    it "raises an error if neither as_crm_payload nor to_h are implemented" do
      user = TestUserNoPayload.create!(email: "nopayload@example.com", full_name: "No Payload")
      expect {
        user.build_crm_payload
      }.to raise_error(NoMethodError, /must implement "as_crm_payload" or "to_h"/)
    end

    it "raises an error if crm_synced is not configured (documentation test)", :aggregate_failures do
      klass = Class.new(ActiveRecord::Base) do
        self.table_name = "users"
        include Etlify::Model
        # Note: no call to crm_synced here
      end

      rec = klass.create!(email: "nope@example.com", full_name: "Nope")

      expect {
        rec.build_crm_payload
      }.to raise_error(ArgumentError, /crm_synced not configured/)
    end
  end

  describe "#crm_sync!" do
    it "enqueues a job when async=true (default)", :aggregate_failures do
      expect {
        user.crm_sync! # async defaults to true
      }.to change { ActiveJob::Base.queue_adapter.enqueued_jobs.size }.by(1)

      job = ActiveJob::Base.queue_adapter.enqueued_jobs.last
      expect(job[:job].to_s).to eq(Etlify.config.sync_job_class.to_s)
      expect(job[:args]).to include("TestUser", user.id)
    end

    it "calls the synchronizer inline when async=false" do
      allow(Etlify::Synchronizer).to receive(:call).and_return(:synced)
      result = user.crm_sync!(async: false)
      expect(result).to eq(:synced)
      expect(Etlify::Synchronizer).to have_received(:call).with(instance_of(TestUser))
    end

    it "respects the guard and does not sync if sync_if returns false" do
      guarded = GuardedUser.create!(email: "guarded@example.com", full_name: "G")
      allow(Etlify::Synchronizer).to receive(:call)

      expect(guarded.crm_sync!(async: false)).to be false
      expect(Etlify::Synchronizer).not_to have_received(:call)
      expect {
        guarded.crm_sync! # async true
      }.not_to change { ActiveJob::Base.queue_adapter.enqueued_jobs.size }
    end
  end

  describe "#crm_delete!" do
    it "delegates to Etlify::Deleter" do
      unless defined?(Etlify::Deleter)
        stub_const("Etlify::Deleter", Class.new do
          def self.call(_); :deleted; end
        end)
      end

      expect(Etlify::Deleter).to receive(:call).with(instance_of(TestUser)).and_return(:deleted)
      expect(user.crm_delete!).to eq(:deleted)
    end
  end
end
