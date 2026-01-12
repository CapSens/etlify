require "rails_helper"

RSpec.describe Etlify::StaleRecords::Finder do
  def create_sync!(resource, crm:, last_synced_at:)
    CrmSynchronisation.create!(
      crm_name: crm.to_s,
      resource_type: resource.class.name,
      resource_id: resource.id,
      last_synced_at: last_synced_at
    )
  end

  def now
    Time.now
  end

  def user_ids_for(crm)
    described_class.call(crm_name: crm)[User][crm].pluck(:id)
  end

  describe ".call" do
    context "model discovery" do
      it "includes AR descendants with etlify_crms and existing table" do
        user = User.create!(email: "a@b.c")
        result = described_class.call

        expect(result.keys).to include(User)
        expect(result[User].keys).to include(:hubspot)
        expect(user.id).to be_a(Integer)
      end

      it "filters by crm_name when provided" do
        result = described_class.call(crm_name: :hubspot)

        expect(result.keys).to include(User)
        expect(result[User].keys).to eq([:hubspot])
      end

      it "restricts to given models when provided" do
        result = described_class.call(models: [User])

        expect(result.keys).to eq([User])
      end

      it "returns empty hash when no model qualifies" do
        allow(User).to receive(:etlify_crms).and_return({})

        result = described_class.call(models: [User])

        expect(result).to eq({})
      end
    end

    context "return shape" do
      it "returns { Model => { crm => relation } }" do
        result = described_class.call(crm_name: :hubspot)

        expect(result).to be_a(Hash)
        expect(result[User]).to be_a(Hash)
        expect(result[User][:hubspot]).to be_a(ActiveRecord::Relation)
      end

      it "relations select only id column" do
        relation = described_class.call[User][:hubspot]

        expect(relation.select_values).to include(:id)
      end

      it "orders ids ascending for stable batching" do
        first_user = User.create!(email: "a@x.x")
        second_user = User.create!(email: "b@x.x")

        ids = user_ids_for(:hubspot)

        expect(ids).to eq(ids.sort)
        expect(ids).to include(first_user.id, second_user.id)
      end
    end

    context "stale_scope integration" do
      it "uses the stale_scope to find stale records" do
        user = User.create!(email: "x@x.x", updated_at: now)

        expect(user_ids_for(:hubspot)).to include(user.id)
      end

      it "excludes synced records when last_synced_at >= updated_at" do
        user = User.create!(email: "x@x.x", updated_at: now - 10)
        create_sync!(user, crm: :hubspot, last_synced_at: now)

        expect(user_ids_for(:hubspot)).not_to include(user.id)
      end

      it "includes records when last_synced_at < updated_at" do
        user = User.create!(email: "x@x.x", updated_at: now)
        create_sync!(user, crm: :hubspot, last_synced_at: now - 10)

        expect(user_ids_for(:hubspot)).to include(user.id)
      end

      it "includes records with no sync record" do
        user = User.create!(email: "x@x.x")

        expect(user_ids_for(:hubspot)).to include(user.id)
      end
    end

    context "stale_scope validation" do
      it "raises when stale_scope returns non-relation" do
        allow(User).to receive(:etlify_crms).and_return(
          {
            hubspot: {
              adapter: Etlify::Adapters::NullAdapter.new,
              id_property: "id",
              crm_object_type: "contacts",
              stale_scope: ->(_model, _crm) { [1, 2, 3] },
            },
          }
        )

        expect do
          described_class.call(crm_name: :hubspot)
        end.to raise_error(ArgumentError, /must return an ActiveRecord::Relation/)
      end
    end

    context "with stale_scope as query object" do
      let(:query_object) do
        Class.new do
          def self.call(model, crm_name)
            stale_sql = <<-SQL.squish
              crm_synchronisations.id IS NULL
              OR crm_synchronisations.crm_name != ?
              OR crm_synchronisations.last_synced_at < users.updated_at
            SQL
            model
              .left_joins(:crm_synchronisations)
              .where(stale_sql, crm_name.to_s)
          end
        end
      end

      before do
        allow(User).to receive(:etlify_crms).and_return(
          {
            hubspot: {
              adapter: Etlify::Adapters::NullAdapter.new,
              id_property: "id",
              crm_object_type: "contacts",
              stale_scope: query_object,
            },
          }
        )
      end

      it "works with a query object class responding to .call" do
        user = User.create!(email: "query@object.test", updated_at: now)

        expect(user_ids_for(:hubspot)).to include(user.id)
      end

      it "excludes synced records" do
        user = User.create!(email: "query@object.test", updated_at: now - 10)
        create_sync!(user, crm: :hubspot, last_synced_at: now)

        expect(user_ids_for(:hubspot)).not_to include(user.id)
      end
    end

    context "multi-CRM support" do
      let(:stale_scope) do
        ->(model, crm_name) do
          stale_sql = <<-SQL.squish
            crm_synchronisations.id IS NULL
            OR crm_synchronisations.crm_name != ?
            OR crm_synchronisations.last_synced_at < users.updated_at
          SQL
          model.left_joins(:crm_synchronisations).where(stale_sql, crm_name.to_s)
        end
      end

      before do
        allow(User).to receive(:etlify_crms).and_return(
          {
            hubspot: {
              adapter: Etlify::Adapters::NullAdapter.new,
              id_property: "id",
              crm_object_type: "contacts",
              stale_scope: stale_scope,
            },
            salesforce: {
              adapter: Etlify::Adapters::NullAdapter.new,
              id_property: "Id",
              crm_object_type: "Lead",
              stale_scope: stale_scope,
            },
          }
        )
      end

      it "returns entries for each configured CRM" do
        result = described_class.call

        expect(result[User].keys).to contain_exactly(:hubspot, :salesforce)
      end

      it "scopes staleness per CRM independently" do
        user = User.create!(email: "x@x.x", updated_at: now)
        create_sync!(user, crm: :hubspot, last_synced_at: now + 10)

        results = described_class.call

        expect(results[User][:hubspot].pluck(:id)).not_to include(user.id)
        expect(results[User][:salesforce].pluck(:id)).to include(user.id)
      end
    end
  end
end
