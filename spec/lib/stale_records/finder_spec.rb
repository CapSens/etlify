require "rails_helper"

RSpec.describe Etlify::StaleRecords::Finder do
  include_context "with companies and users"

  def ids_for(rel)
    rel.pluck(rel.klass.primary_key)
  end

  describe ".call" do
    it "returns a Hash of { ModelClass => Relation }", :aggregate_failures do
      result = described_class.call
      expect(result).to be_a(Hash)
      expect(result.keys).to include(Company, User)
      expect(result[Company]).to be_a(ActiveRecord::Relation)
      expect(result[User]).to be_a(ActiveRecord::Relation)
    end

    it "lists records with no crm_synchronisation as stale", :aggregate_failures do
      result = described_class.call
      expect(ids_for(result[Company])).to contain_exactly(company.id)
      expect(ids_for(result[User])).to contain_exactly(user.id)
    end

    it "excludes records whose sync is up to date vs self updated_at" do
      Timecop.freeze do
        synced_at = Time.current
        create_sync_for!(company, last_synced_at: synced_at)
        create_sync_for!(user, last_synced_at: synced_at)

        result = described_class.call

        aggregate_failures do
          expect(ids_for(result[Company])).to be_empty
          expect(ids_for(result[User])).to be_empty
        end
      end
    end

    it "includes records when self updated_at is newer than last_synced_at" do
      Timecop.freeze do
        create_sync_for!(company, last_synced_at: Time.current)
        create_sync_for!(user, last_synced_at: 2.hours.ago)

        user.touch

        result = described_class.call

        aggregate_failures do
          expect(ids_for(result[Company])).to be_empty
          expect(ids_for(result[User])).to contain_exactly(user.id)
        end
      end
    end

    it "includes records when a dependency updated_at is newer (User depends on Company)" do
      Timecop.freeze do
        synced_at = 2.hours.ago
        create_sync_for!(company, last_synced_at: synced_at)
        create_sync_for!(user, last_synced_at: synced_at)

        company.update!(name: "CapSens Updated")

        result = described_class.call
        expect(ids_for(result[Company])).to contain_exactly(company.id)
        expect(ids_for(result[User])).to contain_exactly(user.id)
      end
    end

    it "selects only the id column in relations (memory efficient)" do
      result = described_class.call
      sql = result[User].to_sql
      expect(sql).to match(/\ASELECT\s+"users"\."id"\s+FROM\s+"users"/i)
    end

    it "respects models: option to restrict searched models" do
      result = described_class.call(models: [User])
      expect(result.keys).to contain_exactly(User)
    end

    context "when has_many :through with FK on source " \
        "(subscriptions via profiles)" do
      before(:all) do
        # Create missing tables for this focused scenario
        ActiveRecord::Schema.define do
          create_table :users_profiles, force: true do |t|
            t.integer :user_id
            t.timestamps
          end
          add_index :users_profiles, :user_id

          create_table :subscriptions, force: true do |t|
            t.integer :users_profile_id
            t.timestamps
          end
          add_index :subscriptions, :users_profile_id
        end

        # Define minimal namespaced models
        module Users; end

        class Users::Profile < ApplicationRecord
          self.table_name = "users_profiles"
          belongs_to :user
          has_many :subscriptions,
                   class_name: "Subscription",
                   foreign_key: :users_profile_id,
                   inverse_of: :profile,
                   dependent: :destroy
        end

        class Subscription < ApplicationRecord
          belongs_to :profile,
                     class_name: "Users::Profile",
                     foreign_key: :users_profile_id,
                     inverse_of: :subscriptions
        end

        # Extend User for this spec: add through association and dependency
        User.class_eval do
          has_many :profiles,
                   class_name: "Users::Profile",
                   inverse_of: :user,
                   dependent: :destroy
          has_many :subscriptions, through: :profiles

          # Add :subscriptions to the dependencies list for the Finder
          def self.etlify_dependencies
            [:company, :teams, :subscriptions]
          end
        end
      end

      it "uses the correct join and marks User as stale" do
        user = User.create!(email: "a@b.c")
        profile = Users::Profile.create!(user: user)
        subscription = Subscription.create!(profile: profile)

        # Fresh sync at t0
        CrmSynchronisation.create!(
          resource_type: "User",
          resource_id: user.id,
          last_synced_at: Time.current
        )
        Timecop.travel(1.second.from_now)

        # Updating a source row (subscription) should stale the owner (user)
        subscription.touch

        result = described_class.call
        expect(result[User].pluck("users.id")).to include(user.id)

        # Assert join direction in the subquery is correct after the fix:
        # subscriptions.users_profile_id = users_profiles.id
        sql = result[User].to_sql
        expect(sql).to include(
          "INNER JOIN \"subscriptions\" ON " \
          "\"subscriptions\".\"users_profile_id\" = " \
          "\"users_profiles\".\"id\""
        )
      end
    end

    context "when a record has a recent sync but dependency changes later" do
      it "marks it stale based on the greatest updated_at among deps" do
        Timecop.freeze do
          create_sync_for!(user, last_synced_at: 1.hour.ago)
          create_sync_for!(company, last_synced_at: 1.hour.ago)

          company.touch

          result = described_class.call
          expect(ids_for(result[User])).to contain_exactly(user.id)
        end
      end
    end

    context "when a record regains freshness after sync" do
      it "excludes it after last_synced_at catches up" do
        Timecop.freeze do
          expect(ids_for(described_class.call[User])).to contain_exactly(user.id)

          create_sync_for!(user, last_synced_at: Time.current)

          result = described_class.call
          expect(ids_for(result[User])).to be_empty
        end
      end
    end

    # --- discovery: only etlified models are listed -----------------------------
    it "discovers only models that were etlified", :aggregate_failures do
      begin
        class Widget < ActiveRecord::Base; end
      rescue
        nil
      end
      # Widget is not defined in schema; we only assert that not all descendants
      # are returned, but known etlified models are.
      result = described_class.call
      expect(result.keys).to include(User, Company, Task)
      # Should not contain arbitrary non-etlified classes
      expect(result.keys.grep(Class).all? { |k| k.respond_to?(:etlify_crm_object_type) }).to be true
    end

    # --- has_many direct dependency (Company depends on users) -------------------
    it "marks Company stale when a dependent user updates (direct has_many)" do
      Timecop.freeze do
        # Fresh syncs for both
        create_sync_for!(company, last_synced_at: Time.current)
        create_sync_for!(user,    last_synced_at: Time.current)
        Timecop.travel(1.second.from_now)

        # Update a user only -> company should become stale due to deps [:users]
        user.update!(full_name: "John V2")
        result = described_class.call
        expect(ids_for(result[Company])).to contain_exactly(company.id)
      end
    end

    # --- has_many :through dependency (User depends on teams) --------------------
    it "marks User stale when a through dependency changes (teams via memberships)" do
      Timecop.freeze do
        team = Team.create!(name: "Core")
        Membership.create!(user: user, team: team)
        create_sync_for!(user, last_synced_at: Time.current)
        Timecop.travel(1.second.from_now)

        # Change the through target (team) -> should stale the user
        team.update!(name: "Core v2")

        result = described_class.call
        expect(ids_for(result[User])).to contain_exactly(user.id)
      end
    end

    # --- polymorphic belongs_to dependency (Task depends on owner: Company/User) -
    it "marks Task stale when its polymorphic owner changes (owner_type Company)" do
      Timecop.freeze do
        task = Task.create!(title: "T1", owner: company)
        create_sync_for!(task, last_synced_at: Time.current)
        Timecop.travel(1.second.from_now)

        # Change company -> task becomes stale due to polymorphic belongs_to
        company.touch
        result = described_class.call
        expect(ids_for(result[Task])).to contain_exactly(task.id)
      end
    end

    it "polymorphic owner types are handled per concrete class (owner_type User)" do
      Timecop.freeze do
        task = Task.create!(title: "T2", owner: user)
        create_sync_for!(task, last_synced_at: Time.current)
        Timecop.travel(1.second.from_now)

        user.touch
        result = described_class.call
        expect(ids_for(result[Task])).to include(task.id)
      end
    end

    # --- id-only selection also for Company -------------------------------------
    it "selects only id for Company as well" do
      result = described_class.call
      sql = result[Company].to_sql
      expect(sql).to match(/\ASELECT\s+"companies"\."id"\s+FROM\s+"companies"/i)
    end

    # --- adapter portability helpers (greatest / epoch_literal) ------------------
    # We hit the private class methods via .send and a tiny connection double.
    class ConnDouble
      def initialize(name)
        @name = name
      end

      def adapter_name
        @name
      end

      def quote_table_name(x)
        %("#{x}")
      end

      def quote_column_name(x)
        %("#{x}")
      end
    end

    it "uses MAX on non-Postgres adapters and GREATEST on Postgres", :aggregate_failures do
      parts = ["1", "2"]
      expect(described_class.send(:greatest, parts, ConnDouble.new("SQLite"))).to eq("MAX(1, 2)")
      expect(described_class.send(:greatest, parts, ConnDouble.new("PostgreSQL")))
        .to eq("GREATEST(1, 2)")
    end

    it "returns the single part unwrapped to avoid SQLite aggregate misuse" do
      single = ["42"]
      expect(described_class.send(:greatest, single, ConnDouble.new("SQLite")))
        .to eq("42")
    end

    it "uses adapter-specific epoch literals", :aggregate_failures do
      expect(described_class.send(:epoch_literal, ConnDouble.new("PostgreSQL")))
        .to eq("TIMESTAMP '1970-01-01 00:00:00'")
      expect(described_class.send(:epoch_literal, ConnDouble.new("SQLite")))
        .to eq("DATETIME('1970-01-01 00:00:00')")
    end
  end
end
