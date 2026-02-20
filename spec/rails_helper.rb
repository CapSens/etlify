# frozen_string_literal: true

require "simplecov"
SimpleCov.start "rails"

require "bundler/setup"

require "rails"
require "active_record"
require "active_job"
require "logger"
require "active_support"
require "active_support/time" # for Time.current / time zone
require "support/time_helpers"
require "support/aj_test_adapter_helpers"
require "timecop"

require "etlify"

class ApplicationRecord < ActiveRecord::Base
  self.abstract_class = true
end

require_relative "../app/models/crm_synchronisation"
require "etlify/serializers/base_serializer"
require "etlify/serializers/user_serializer"
require "etlify/serializers/company_serializer"

RSpec.configure do |config|
  config.include RSpecTimeHelpers
  config.include AJTestAdapterHelpers
  config.order = :random
  Kernel.srand config.seed

  # Use transactions for a clean state
  config.around(:each) do |example|
    ActiveRecord::Base.connection.transaction do
      example.run
      raise ActiveRecord::Rollback
    end
  end

  # Reset DependencyResolver table existence cache between tests
  config.after(:each) do
    Etlify::DependencyResolver.reset_table_exists_cache!
  end

  # suppress ActiveJob and Thor output
  ActiveJob::Base.logger = Logger.new(nil)
  config.before(type: :generator) do
    allow_any_instance_of(Thor::Shell::Basic).to(
      receive(:say_status)
    )
    allow_any_instance_of(Thor::Shell::Basic).to(
      receive(:say)
    )
  end

  # Setup in-memory SQLite once
  config.before(:suite) do
    ActiveRecord::Base.establish_connection(
      adapter: "sqlite3",
      database: ":memory:"
    )
    ActiveRecord::Migration.verbose = false

    ActiveRecord::Schema.define do
      create_table :crm_synchronisations, force: true do |t|
        t.string  :crm_name, null: false
        t.string  :crm_id
        t.string  :last_digest
        t.datetime :last_synced_at
        t.text    :last_error
        t.string  :resource_type, null: false
        t.integer :resource_id, null: false
        t.timestamps
      end

      create_table :companies, force: true do |t|
        t.string :name
        t.string :domain
        t.timestamps
      end

      create_table :users, force: true do |t|
        t.string  :email
        t.string  :full_name
        t.integer :company_id
        t.timestamps
      end

      create_table :contacts, force: true do |t|
        t.string  :email
        t.string  :full_name
        t.integer :company_id
        t.timestamps
      end

      create_table :investments, force: true do |t|
        t.integer :contact_id
        t.integer :company_id
        t.decimal :amount
        t.string  :reference
        t.timestamps
      end

      create_table :etlify_sync_dependencies, force: true do |t|
        t.string  :resource_type,        null: false
        t.bigint  :resource_id,          null: false
        t.string  :parent_resource_type, null: false
        t.bigint  :parent_resource_id,   null: false
        t.string  :crm_name,             null: false
        t.timestamps
      end
    end

    class Company < ApplicationRecord # rubocop:disable Lint/ConstantDefinitionInBlock
      has_many :crm_synchronisations, as: :resource, dependent: :destroy

      def build_crm_payload(crm_name:)
        conf = self.class.etlify_crms.fetch(crm_name.to_sym)
        conf[:serializer].new(self).as_crm_payload
      end
    end

    class User < ApplicationRecord # rubocop:disable Lint/ConstantDefinitionInBlock
      belongs_to :company, optional: true
      has_many :crm_synchronisations, as: :resource, dependent: :destroy

      def build_crm_payload(crm_name:)
        Etlify::Serializers::UserSerializer.new(self).as_crm_payload
      end

      def self.etlify_crms
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "id",
            crm_object_type: "contacts",
          },
        }
      end
    end

    class Contact < ApplicationRecord # rubocop:disable Lint/ConstantDefinitionInBlock
      belongs_to :company, optional: true
      has_many :crm_synchronisations, as: :resource, dependent: :destroy
      has_many :investments

      def build_crm_payload(crm_name:)
        conf = self.class.etlify_crms.fetch(crm_name.to_sym)
        conf[:serializer].new(self).as_crm_payload
      end
    end

    class Investment < ApplicationRecord # rubocop:disable Lint/ConstantDefinitionInBlock
      belongs_to :contact, optional: true
      belongs_to :company, optional: true
      has_many :crm_synchronisations, as: :resource, dependent: :destroy

      def build_crm_payload(crm_name:)
        conf = self.class.etlify_crms.fetch(crm_name.to_sym)
        conf[:serializer].new(self).as_crm_payload
      end
    end
  end
end
