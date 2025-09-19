require "rails_helper"

RSpec.describe Etlify::StaleRecords::Finder do
  # Build extra schema for dependency scenarios
  before(:all) do
    ActiveRecord::Schema.define do
      create_table :profiles, force: true do |t|
        t.integer :user_id
        t.timestamps null: true
      end

      create_table :notes, force: true do |t|
        t.integer :user_id
        t.string :body
        t.timestamps null: true
      end

      create_table :projects, force: true do |t|
        t.string :name
        t.timestamps null: true
      end

      create_table :memberships, force: true do |t|
        t.integer :user_id
        t.integer :project_id
        t.timestamps null: true
      end

      create_table :uploads, force: true do |t|
        t.string :owner_type
        t.integer :owner_id
        t.string :path
        t.timestamps null: true
      end

      create_table :activities, force: true do |t|
        t.string :subject_type
        t.integer :subject_id
        t.timestamps null: true
      end

      # Polymorphic belongs_to on users (owner side)
      unless ActiveRecord::Base.connection.column_exists?(:users, :avatarable_type)
        add_column :users, :avatarable_type, :string
      end

      unless ActiveRecord::Base.connection.column_exists?(:users, :avatarable_id)
        add_column :users, :avatarable_id, :integer
      end

      # Concrete targets for avatarable
      create_table :photos, force: true do |t|
        t.timestamps null: true
      end

      create_table :documents, force: true do |t|
        t.timestamps null: true
      end

      # HABTM to cover join-table branch
      create_table :tags, force: true do |t|
        t.string :name
        t.timestamps null: true
      end

      create_table :tags_users, id: false, force: true do |t|
        t.integer :tag_id
        t.integer :user_id
      end

      create_table :linkages, force: true do |t|
        t.string  :owner_type
        t.integer :owner_id
        t.integer :project_id
        t.timestamps null: true
      end

      create_table :subscriptions, force: true do |t|
        # FK lives on the source table -> references profiles.id
        t.integer :users_profile_id
        t.timestamps null: true
      end
    end

    User.reset_column_information

    stub_models!
  end

  # ----------------- Helpers to define models/constants -----------------

  def define_model_const(name)
    Object.send(:remove_const, name) if Object.const_defined?(name)
    klass = Class.new(ApplicationRecord)
    klass.table_name = name.to_s.underscore.pluralize
    yield klass if block_given?
    Object.const_set(name, klass)
  end

  def stub_models!
    define_model_const("Profile") do |k|
      k.belongs_to :user, optional: true
    end

    define_model_const("Note") do |k|
      k.belongs_to :user, optional: true
    end

    define_model_const("Project") do |k|
      k.has_many :memberships, dependent: :destroy
      k.has_many :users, through: :memberships
    end

    define_model_const("Membership") do |k|
      k.belongs_to :user
      k.belongs_to :project
    end

    define_model_const("Upload") do |k|
      k.belongs_to :owner, polymorphic: true, optional: true
    end

    define_model_const("Activity") do |k|
      k.belongs_to :subject, polymorphic: true, optional: true
    end

    define_model_const("Linkage") do |k|
      k.belongs_to :owner, polymorphic: true
      k.belongs_to :project
    end

    define_model_const("Photo")
    define_model_const("Document")
    define_model_const("Tag")

    define_model_const("Subscription") do |k|
      k.belongs_to :profile,
                   foreign_key: "users_profile_id",
                   optional: true
    end

    Profile.class_eval do
      has_many :subscriptions,
               class_name: "Subscription",
               foreign_key: "users_profile_id",
               dependent: :destroy
    end

    # Reopen User to add associations used by tests
    User.class_eval do
      has_one :profile, dependent: :destroy
      has_many :notes, dependent: :destroy
      has_many :memberships, dependent: :destroy
      has_many :projects, through: :memberships
      has_many :uploads, as: :owner, dependent: :destroy
      has_many :activities, as: :subject, dependent: :destroy
      belongs_to :avatarable, polymorphic: true, optional: true
      has_and_belongs_to_many :tags, join_table: "tags_users"
      has_many :linkages, as: :owner, dependent: :destroy
      has_many :poly_projects, through: :linkages, source: :project
      has_many :subscriptions, through: :profile
    end
  end

  # ------------------------------ Helpers ------------------------------

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

  # Default multi-CRM configuration for User in these specs
  before do
    allow(User).to receive(:etlify_crms).and_return(
      {
        hubspot: {
          adapter: Etlify::Adapters::NullAdapter,
          id_property: "id",
          crm_object_type: "contacts",
          dependencies: [
            :company, :notes, :profile, :projects, :uploads, :activities
          ]
        },
        salesforce: {
          adapter: Etlify::Adapters::NullAdapter,
          id_property: "Id",
          crm_object_type: "Lead",
          dependencies: [:company]
        }
      }
    )
  end

  # ---------------- A. Model discovery / filtering ----------------

  describe ".call model discovery" do
    it "includes AR descendants with config and existing table" do
      u = User.create!(email: "a@b.c")
      res = described_class.call
      expect(res.keys).to include(User)
      expect(res[User].keys).to include(:hubspot, :salesforce)
      expect(res[User][:hubspot].arel.projections.size).to eq(1)
      expect(res[User][:salesforce].arel.projections.size).to eq(1)
      expect(u.id).to be_a(Integer)
    end

    it "when crm_name is given, keeps only models configured for it" do
      res = described_class.call(crm_name: :hubspot)
      expect(res.keys).to include(User)
      expect(res[User].keys).to eq([:hubspot])
    end

    it "when models: is given, restricts to that subset" do
      res = described_class.call(models: [User])
      expect(res.keys).to eq([User])
    end
  end

  # ----------------------- B. Return shape -----------------------

  describe ".call return shape" do
    it "returns { Model => { crm => relation } } for single CRM" do
      res = described_class.call(crm_name: :hubspot)
      expect(res).to be_a(Hash)
      expect(res[User]).to be_a(Hash)
      expect(res[User][:hubspot]).to be_a(ActiveRecord::Relation)
    end

    it "includes one entry per CRM when multiple configured" do
      res = described_class.call
      expect(res[User].keys).to contain_exactly(:hubspot, :salesforce)
    end

    it "relations select only primary key" do
      rel = described_class.call[User][:hubspot]
      cols = rel.arel.projections
      expect(cols.size).to eq(1)
    end
  end

  # --------------- C. Join scoping to crm_name ----------------

  describe "JOIN scoped to crm_name" do
    it "treats missing row for given crm as stale" do
      u = User.create!(email: "x@x.x")
      create_sync!(u, crm: :salesforce, last_synced_at: now)
      res = described_class.call(crm_name: :hubspot)[User][:hubspot]
      expect(res.pluck(:id)).to include(u.id)
    end

    it "stale only for the outdated CRM" do
      u = User.create!(email: "x@x.x")
      create_sync!(u, crm: :hubspot, last_synced_at: now - 3600)
      create_sync!(u, crm: :salesforce, last_synced_at: now + 3600)
      res_all = described_class.call
      expect(res_all[User][:hubspot].pluck(:id)).to include(u.id)
      expect(res_all[User][:salesforce].pluck(:id)).not_to include(u.id)
    end

    it "fresh for both CRMs yields no ids" do
      u = User.create!(email: "x@x.x", updated_at: now - 10)
      create_sync!(u, crm: :hubspot, last_synced_at: now)
      create_sync!(u, crm: :salesforce, last_synced_at: now)
      res = described_class.call
      expect(res[User][:hubspot].pluck(:id)).not_to include(u.id)
      expect(res[User][:salesforce].pluck(:id)).not_to include(u.id)
    end
  end

  # -------------------- D. Staleness logic --------------------

  describe "staleness threshold" do
    it "missing crm_synchronisation row => stale" do
      u = User.create!(email: "x@x.x")
      ids = described_class.call(crm_name: :hubspot)[User][:hubspot]
      expect(ids.pluck(:id)).to include(u.id)
    end

    it "NULL last_synced_at acts like epoch and becomes stale" do
      u = User.create!(email: "x@x.x")
      CrmSynchronisation.create!(
        crm_name: "hubspot", resource_type: "User",
        resource_id: u.id, last_synced_at: nil
      )
      ids = described_class.call(crm_name: :hubspot)[User][:hubspot]
      expect(ids.pluck(:id)).to include(u.id)
    end

    it "compares strictly: < stale, == not stale, > not stale" do
      t0 = now
      u = User.create!(email: "x@x.x", updated_at: t0)
      create_sync!(u, crm: :hubspot, last_synced_at: t0 - 1)
      expect(described_class.call(crm_name: :hubspot)[User][:hubspot]
        .pluck(:id)).to include(u.id)

      CrmSynchronisation.where(
        resource_id: u.id, crm_name: "hubspot"
      ).update_all(last_synced_at: t0)
      expect(described_class.call(crm_name: :hubspot)[User][:hubspot]
        .pluck(:id)).not_to include(u.id)

      CrmSynchronisation.where(
        resource_id: u.id, crm_name: "hubspot"
      ).update_all(last_synced_at: t0 + 1)
      expect(described_class.call(crm_name: :hubspot)[User][:hubspot]
        .pluck(:id)).not_to include(u.id)
    end

    it "no dependencies => threshold is owner's updated_at" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter,
            id_property: "id",
            crm_object_type: "contacts",
            dependencies: []
          }
        }
      )
      u = User.create!(email: "x@x.x", updated_at: now)
      create_sync!(u, crm: :hubspot, last_synced_at: now - 1)
      res = described_class.call(crm_name: :hubspot)[User][:hubspot]
      expect(res.pluck(:id)).to include(u.id)
    end
  end

  # ----------------- E. Direct dependencies -----------------

  describe "dependencies direct associations" do
    it "belongs_to: updating company makes user stale" do
      c = Company.create!(name: "ACME")
      u = User.create!(email: "u@x.x", company: c)
      create_sync!(u, crm: :hubspot, last_synced_at: now)
      c.update!(updated_at: now + 10)
      res = described_class.call(crm_name: :hubspot)[User][:hubspot]
      expect(res.pluck(:id)).to include(u.id)
    end

    it "belongs_to missing target falls back to epoch, not crashing" do
      u = User.create!(email: "u@x.x", company: nil)
      create_sync!(u, crm: :hubspot, last_synced_at: now + 10)
      res = described_class.call(crm_name: :hubspot)[User][:hubspot]
      expect(res.pluck(:id)).not_to include(u.id)
    end

    it "has_one: updating profile makes user stale" do
      u = User.create!(email: "u@x.x")
      p = u.create_profile!
      create_sync!(u, crm: :hubspot, last_synced_at: now)
      p.update!(updated_at: now + 10)
      res = described_class.call(crm_name: :hubspot)[User][:hubspot]
      expect(res.pluck(:id)).to include(u.id)
    end

    it "has_many: newest note updated makes user stale" do
      u = User.create!(email: "u@x.x")
      u.notes.create!(body: "a", updated_at: now)
      u.notes.create!(body: "b", updated_at: now + 20)
      create_sync!(u, crm: :hubspot, last_synced_at: now + 5)
      res = described_class.call(crm_name: :hubspot)[User][:hubspot]
      expect(res.pluck(:id)).to include(u.id)
    end

    it "polymorphic has_many via :as ignores unrelated rows" do
      u1 = User.create!(email: "u1@x.x")
      u2 = User.create!(email: "u2@x.x")
      u1.uploads.create!(path: "p1", updated_at: now)
      u2.uploads.create!(path: "p2", updated_at: now + 60)
      create_sync!(u1, crm: :hubspot, last_synced_at: now + 10)
      res = described_class.call(crm_name: :hubspot)[User][:hubspot]
      expect(res.pluck(:id)).not_to include(u1.id)
    end
  end

  # -------- F. Through / polymorphic belongs_to (child side) --------

  describe "through and polymorphic belongs_to" do
    it "has_many :through: source newer marks user stale" do
      u = User.create!(email: "u@x.x")
      p = Project.create!(name: "P")
      Membership.create!(user: u, project: p)
      create_sync!(u, crm: :hubspot, last_synced_at: now)
      p.update!(updated_at: now + 30)
      res = described_class.call(crm_name: :hubspot)[User][:hubspot]
      expect(res.pluck(:id)).to include(u.id)
    end

    it "polymorphic child: newest concrete subject wins" do
      u = User.create!(email: "u@x.x")
      act = Activity.create!(subject: u, updated_at: now)
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter,
            id_property: "id",
            crm_object_type: "contacts",
            dependencies: [:activities]
          }
        }
      )
      create_sync!(u, crm: :hubspot, last_synced_at: now + 1)
      expect(described_class.call(crm_name: :hubspot)[User][:hubspot]
        .pluck(:id)).not_to include(u.id)
      act.update!(updated_at: now + 10)
      expect(described_class.call(crm_name: :hubspot)[User][:hubspot]
        .pluck(:id)).to include(u.id)
    end

    it "polymorphic with non-constantizable type is ignored safely" do
      u = User.create!(email: "u@x.x")
      ts = now.utc.strftime("%Y-%m-%d %H:%M:%S")
      Activity.connection.execute(
        "INSERT INTO activities (subject_type, subject_id, created_at," \
        " updated_at) VALUES ('Nope::Missing', 123, '#{ts}', '#{ts}')"
      )
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter,
            id_property: "id",
            crm_object_type: "contacts",
            dependencies: [:activities]
          }
        }
      )
      create_sync!(u, crm: :hubspot, last_synced_at: now + 5)
      res = described_class.call(crm_name: :hubspot)[User][:hubspot]
      expect(res.pluck(:id)).not_to include(u.id)
    end

    it "has_many :through with polymorphic through (as:) adds type predicate" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter,
            id_property: "id",
            crm_object_type: "contacts",
            dependencies: [:poly_projects]
          }
        }
      )

      u = User.create!(email: "t@x.x")
      p = Project.create!(name: "P", updated_at: now)
      Linkage.create!(owner: u, project: p)

      create_sync!(u, crm: :hubspot, last_synced_at: now + 1)

      expect(described_class.call(crm_name: :hubspot)[User][:hubspot]
        .pluck(:id)).not_to include(u.id)

      p.update!(updated_at: now + 20)
      expect(described_class.call(crm_name: :hubspot)[User][:hubspot]
        .pluck(:id)).to include(u.id)
    end

    describe "has_many :through where FK lives on source table" do
      it "marks owner stale when a source row becomes newer" do
        allow(User).to receive(:etlify_crms).and_return(
          {
            hubspot: {
              adapter: Etlify::Adapters::NullAdapter,
              id_property: "id",
              crm_object_type: "contacts",
              dependencies: [:subscriptions]
            }
          }
        )

        u = User.create!(email: "x@x.x")
        p = Profile.create!(user: u, updated_at: now)
        s = Subscription.create!(users_profile_id: p.id, updated_at: now)

        create_sync!(u, crm: :hubspot, last_synced_at: now + 1)
        rel = described_class.call(crm_name: :hubspot)[User][:hubspot]
        expect(rel.pluck(:id)).not_to include(u.id)

        s.update!(updated_at: now + 20)
        rel = described_class.call(crm_name: :hubspot)[User][:hubspot]
        expect(rel.pluck(:id)).to include(u.id)
      end
    end
  end

  # --------- owner belongs_to polymorphic (avatarable) ----------

  describe "owner belongs_to polymorphic dependency" do
    it "uses concrete target updated_at when avatarable is set" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter,
            id_property: "id",
            crm_object_type: "contacts",
            dependencies: [:avatarable]
          }
        }
      )
      u = User.create!(email: "p@x.x")
      p = Photo.create!(updated_at: now)
      u.avatarable = p
      u.updated_at = now
      u.save!

      create_sync!(u, crm: :hubspot, last_synced_at: now + 1)
      expect(described_class.call(crm_name: :hubspot)[User][:hubspot]
        .pluck(:id)).not_to include(u.id)

      p.update!(updated_at: now + 20)
      expect(described_class.call(crm_name: :hubspot)[User][:hubspot]
        .pluck(:id)).to include(u.id)
    end

    it "returns epoch when no concrete types exist (parts empty)" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter,
            id_property: "id",
            crm_object_type: "contacts",
            dependencies: [:avatarable]
          }
        }
      )
      u = User.create!(email: "q@x.x")
      create_sync!(u, crm: :hubspot, last_synced_at: now + 10)
      expect(described_class.call(crm_name: :hubspot)[User][:hubspot]
        .pluck(:id)).not_to include(u.id)
    end
  end

  # ------------- HABTM -------------

  describe "HABTM dependency" do
    it "marque stale quand un tag devient plus récent que le last_sync" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter,
            id_property: "id",
            crm_object_type: "contacts",
            dependencies: [:tags]
          }
        }
      )
      u = User.create!(email: "habtm@x.x", updated_at: now)
      t = Tag.create!(name: "x", updated_at: now)
      u.tags << t

      # sync frais -> pas stale
      create_sync!(u, crm: :hubspot, last_synced_at: now + 10)
      expect(described_class.call(crm_name: :hubspot)[User][:hubspot]
        .pluck(:id)).not_to include(u.id)

      # un tag devient plus récent -> stale
      t.update!(updated_at: now + 30)
      expect(described_class.call(crm_name: :hubspot)[User][:hubspot]
        .pluck(:id)).to include(u.id)
    end
  end

  # --------------- G. Timestamp edge cases ----------------

  describe "timestamp edge cases" do
    it "NULL updated_at are treated as epoch (no crash)" do
      u = User.create!(email: "u@x.x")
      n = u.notes.create!(body: "n")
      Note.where(id: n.id).update_all(updated_at: nil)
      create_sync!(u, crm: :hubspot, last_synced_at: now + 10)
      res = described_class.call(crm_name: :hubspot)[User][:hubspot]
      expect(res.pluck(:id)).not_to include(u.id)
    end

    it "children NULL updated_at does not mark stale unless owner newer" do
      u = User.create!(email: "u@x.x", updated_at: now)
      n = u.notes.create!(body: "n")
      Note.where(id: n.id).update_all(updated_at: nil)
      create_sync!(u, crm: :hubspot, last_synced_at: now + 5)
      res = described_class.call(crm_name: :hubspot)[User][:hubspot]
      expect(res.pluck(:id)).not_to include(u.id)
    end
  end

  # ------------- Adapter portability (integration-level) -------------

  describe "adapter portability (integration)" do
    it "utilise GREATEST sur Postgres et MAX sur SQLite quand plusieurs deps" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter,
            id_property: "id",
            crm_object_type: "contacts",
            dependencies: [:notes, :profile]
          }
        }
      )
      rel = described_class.call(crm_name: :hubspot)[User][:hubspot]
      sql = rel.to_sql
      adapter = ActiveRecord::Base.connection.adapter_name.to_s.downcase
      if adapter.include?("postgres")
        expect(sql).to match(/GREATEST\(/i)
      else
        expect(sql).to match(/MAX\(/i)
      end
    end

    it "génère un SQL exécutable (quoting correct) sur le SGBD courant" do
      rel = described_class.call(crm_name: :hubspot)[User][:hubspot]
      expect { rel.to_a }.not_to raise_error
    end
  end

  # ----------- I. CRM-specific dependencies isolation -----------

  describe "CRM-specific dependencies isolation" do
    it "changing a dep for CRM A does not mark CRM B stale" do
      u = User.create!(email: "a@x.x")
      c = Company.create!(name: "ACME")
      u.update!(company: c)
      create_sync!(u, crm: :hubspot, last_synced_at: now)
      create_sync!(u, crm: :salesforce, last_synced_at: now)
      u.notes.create!(body: "x", updated_at: now + 30)
      res = described_class.call
      expect(res[User][:hubspot].pluck(:id)).to include(u.id)
      expect(res[User][:salesforce].pluck(:id)).not_to include(u.id)
    end

    it "changing a dep for CRM B marks stale only for CRM B" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter,
            id_property: "id",
            crm_object_type: "contacts",
            dependencies: [:notes]
          },
          salesforce: {
            adapter: Etlify::Adapters::NullAdapter,
            id_property: "Id",
            crm_object_type: "Lead",
            dependencies: [:company]
          }
        }
      )
      u = User.create!(email: "b@x.x")
      c = Company.create!(name: "ACME")
      u.update!(company: c)
      create_sync!(u, crm: :hubspot, last_synced_at: now + 30)
      create_sync!(u, crm: :salesforce, last_synced_at: now)
      c.update!(updated_at: now + 60)
      res = described_class.call
      expect(res[User][:salesforce].pluck(:id)).to include(u.id)
      expect(res[User][:hubspot].pluck(:id)).not_to include(u.id)
    end
  end

  # ------------- J. Empty results / absent CRM ----------------

  describe "empty and absent CRM cases" do
    it "omits models not configured for targeted crm_name" do
      allow(User).to receive(:etlify_crms).and_return(
        { hubspot: User.etlify_crms[:hubspot] }
      )
      res = described_class.call(crm_name: :salesforce)
      expect(res).to eq({})
    end

    it "returns {} when no model qualifies" do
      klass = Class.new(ApplicationRecord) do
        self.table_name = "projects"
        def self.etlify_crms = {}
      end
      Object.const_set("NopeModel", klass)
      res = described_class.call(models: [NopeModel])
      expect(res).to eq({})
    ensure
      Object.send(:remove_const, "NopeModel") if Object.const_defined?("NopeModel")
    end

    it "relation exists but may be empty when nothing is stale" do
      u = User.create!(email: "ok@x.x", updated_at: now - 1)
      create_sync!(u, crm: :hubspot, last_synced_at: now)
      rel = described_class.call(crm_name: :hubspot)[User][:hubspot]
      expect(rel).to be_a(ActiveRecord::Relation)
      expect(rel.pluck(:id)).to be_empty
    end
  end

  # ----------------- Robustness -----------------

  describe "robustness" do
    it "ignores unknown dependency names" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: User.etlify_crms[:hubspot].merge(
            dependencies: [:does_not_exist]
          )
        }
      )
      u = User.create!(email: "u@x.x", updated_at: now)
      create_sync!(u, crm: :hubspot, last_synced_at: now + 10)
      res = described_class.call(crm_name: :hubspot)[User][:hubspot]
      expect(res.pluck(:id)).not_to include(u.id)
    end

    it "uses a single LEFT OUTER JOIN per CRM and selects id only" do
      rel = described_class.call(crm_name: :hubspot)[User][:hubspot]
      sql = rel.to_sql
      expect(sql.scan(/LEFT OUTER JOIN/i).size).to eq(1)
      expect(sql).to include('SELECT "users"."id"')
    end

    it "quotes names safely to avoid crashes with reserved words" do
      rel = described_class.call(crm_name: :hubspot)[User][:hubspot]
      expect { rel.to_a }.not_to raise_error
    end
  end
end
