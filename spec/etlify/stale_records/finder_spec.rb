require "rails_helper"

RSpec.describe Etlify::StaleRecords::Finder do
  # ---------------- Schema bootstrap for dependency scenarios ----------------

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

      create_table :follows, force: true do |t|
        t.integer :follower_id, null: false
        t.integer :followee_id, null: false
        t.timestamps null: true
      end

      unless ActiveRecord::Base.connection
                               .column_exists?(:users, :avatarable_type)
        add_column :users, :avatarable_type, :string
      end

      unless ActiveRecord::Base.connection
                               .column_exists?(:users, :avatarable_id)
        add_column :users, :avatarable_id, :integer
      end

      create_table :photos, force: true do |t|
        t.timestamps null: true
      end

      create_table :documents, force: true do |t|
        t.timestamps null: true
      end

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
        # FK lives on source table -> references profiles.id
        t.integer :users_profile_id
        t.string :type # STI column
        t.timestamps null: true
      end
    end

    User.reset_column_information
    stub_models!
  end

  # ----------------------------- Model helpers ------------------------------

  def define_model_const(name)
    Object.send(:remove_const, name) if Object.const_defined?(name)
    klass = Class.new(ApplicationRecord)
    klass.table_name = name.to_s.underscore.pluralize
    yield klass if block_given?
    Object.const_set(name, klass)
  end

  def stub_models!
    define_model_const("Profile") do |klass|
      klass.belongs_to :user, optional: true
    end

    define_model_const("Note") do |klass|
      klass.belongs_to :user, optional: true
    end

    define_model_const("Project") do |klass|
      klass.has_many :memberships, dependent: :destroy
      klass.has_many :users, through: :memberships
    end

    define_model_const("Membership") do |klass|
      klass.belongs_to :user
      klass.belongs_to :project
    end

    define_model_const("Upload") do |klass|
      klass.belongs_to :owner, polymorphic: true, optional: true
    end

    define_model_const("Activity") do |klass|
      klass.belongs_to :subject, polymorphic: true, optional: true
    end

    define_model_const("Linkage") do |klass|
      klass.belongs_to :owner, polymorphic: true
      klass.belongs_to :project
    end

    define_model_const("Photo")
    define_model_const("Document")
    define_model_const("Tag")

    define_model_const("Subscription") do |klass|
      klass.belongs_to :profile,
                       foreign_key: "users_profile_id",
                       optional: true
    end

    define_model_const("Follow") do |klass|
      klass.belongs_to :follower, class_name: "User", optional: false
      klass.belongs_to :followee, class_name: "User", optional: false
    end

    # STI subclass for testing Finder with Single Table Inheritance
    Object.send(:remove_const, "LendingSubscription") \
      if Object.const_defined?("LendingSubscription")
    Object.const_set(
      "LendingSubscription",
      Class.new(Subscription)
    )

    Profile.class_eval do
      has_many :subscriptions,
               class_name: "Subscription",
               foreign_key: "users_profile_id",
               dependent: :destroy
    end

    # Extend User with associations needed by tests
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
      has_many :follows, class_name: "Follow",
                         foreign_key: "follower_id",
                         dependent: :destroy
      has_many :followees, through: :follows, source: :followee
    end
  end

  # --------------------------------- Utils ----------------------------------

  def create_sync!(resource, crm:, last_synced_at:, error_count: 0)
    CrmSynchronisation.create!(
      crm_name: crm.to_s,
      resource_type: resource.class.name,
      resource_id: resource.id,
      last_synced_at: last_synced_at,
      error_count: error_count
    )
  end

  def now
    Time.now
  end

  # Small helper to read ids for a given CRM on User
  def user_ids_for(crm)
    described_class.call(crm_name: crm)[User][crm].pluck(:id)
  end

  # ---------------- Default multi-CRM config stub for User ------------------

  before do
    allow(User).to receive(:etlify_crms).and_return(
      {
        hubspot: {
          adapter: Etlify::Adapters::NullAdapter.new,
          id_property: "id",
          crm_object_type: "contacts",
          dependencies: [
            :company, :notes, :profile, :projects, :uploads, :activities,
          ],
        },
        salesforce: {
          adapter: Etlify::Adapters::NullAdapter.new,
          id_property: "Id",
          crm_object_type: "Lead",
          dependencies: [:company],
        },
      }
    )
  end

  # --------------------------- A. Model discovery ---------------------------

  describe ".call model discovery" do
    it "includes AR descendants with config and existing table" do
      user = User.create!(email: "a@b.c")
      result = described_class.call
      expect(result.keys).to include(User)
      expect(result[User].keys).to include(:hubspot, :salesforce)
      expect(result[User][:hubspot].arel.projections.size).to eq(1)
      expect(result[User][:salesforce].arel.projections.size).to eq(1)
      expect(user.id).to be_a(Integer)
    end

    it "when crm_name is given, keeps only models configured for it" do
      result = described_class.call(crm_name: :hubspot)
      expect(result.keys).to include(User)
      expect(result[User].keys).to eq([:hubspot])
    end

    it "when models: is given, restricts to that subset" do
      result = described_class.call(models: [User])
      expect(result.keys).to eq([User])
    end

    it "skips STI subclasses that only inherited etlify_crms from the base class" do
      config = {
        hubspot: {
          adapter: Etlify::Adapters::NullAdapter.new,
          id_property: "id",
          crm_object_type: "deals",
          dependencies: [],
        },
      }
      allow(Subscription).to receive(:etlify_crms).and_return(config)
      allow(LendingSubscription).to receive(:etlify_crms).and_return(config)

      result = described_class.call
      expect(result.keys).to include(Subscription)
      expect(result.keys).not_to include(LendingSubscription)
    end

    it "processes STI subclasses that have their own etlify_crms config" do
      config = {
        hubspot: {
          adapter: Etlify::Adapters::NullAdapter.new,
          id_property: "id",
          crm_object_type: "deals",
          dependencies: [],
        },
      }
      allow(LendingSubscription).to receive(:etlify_crms).and_return(config)

      Subscription.create!(users_profile_id: nil, type: "LendingSubscription")
      Subscription.create!(users_profile_id: nil, type: nil)

      result = described_class.call
      expect(result.keys).to include(LendingSubscription)

      ids = result[LendingSubscription][:hubspot].pluck(:id)
      lending_ids = Subscription.where(type: "LendingSubscription").pluck(:id)
      other_ids = Subscription.where(type: nil).pluck(:id)

      expect(ids).to match_array(lending_ids)
      expect(ids).not_to include(*other_ids)
    end
  end

  # ------------------------------ B. Shape ----------------------------------

  describe ".call return shape" do
    it "returns { Model => { crm => relation } } for single CRM" do
      result = described_class.call(crm_name: :hubspot)
      expect(result).to be_a(Hash)
      expect(result[User]).to be_a(Hash)
      expect(result[User][:hubspot]).to be_a(ActiveRecord::Relation)
    end

    it "includes one entry per CRM when multiple configured" do
      result = described_class.call
      expect(result[User].keys).to contain_exactly(:hubspot, :salesforce)
    end

    it "relations select only primary key" do
      relation = described_class.call[User][:hubspot]
      projections = relation.arel.projections
      expect(projections.size).to eq(1)
    end
  end

  # ----------------------- C. JOIN scoped to crm_name -----------------------

  describe "JOIN scoped to crm_name" do
    it "treats missing row for given crm as stale" do
      user = User.create!(email: "x@x.x")
      create_sync!(user, crm: :salesforce, last_synced_at: now)
      expect(user_ids_for(:hubspot)).to include(user.id)
    end

    it "stale only for the outdated CRM" do
      user = User.create!(email: "x@x.x")
      create_sync!(user, crm: :hubspot, last_synced_at: now - 3600)
      create_sync!(user, crm: :salesforce, last_synced_at: now + 3600)
      all_results = described_class.call
      expect(all_results[User][:hubspot].pluck(:id)).to include(user.id)
      expect(all_results[User][:salesforce].pluck(:id))
        .not_to include(user.id)
    end

    it "fresh for both CRMs yields no ids" do
      user = User.create!(email: "x@x.x", updated_at: now - 10)
      create_sync!(user, crm: :hubspot, last_synced_at: now)
      create_sync!(user, crm: :salesforce, last_synced_at: now)
      results = described_class.call
      expect(results[User][:hubspot].pluck(:id)).not_to include(user.id)
      expect(results[User][:salesforce].pluck(:id)).not_to include(user.id)
    end
  end

  # --------------------------- D. Staleness logic ---------------------------

  describe "staleness threshold" do
    it "missing crm_synchronisation row => stale" do
      user = User.create!(email: "x@x.x")
      expect(user_ids_for(:hubspot)).to include(user.id)
    end

    it "NULL last_synced_at acts like epoch and becomes stale" do
      user = User.create!(email: "x@x.x")
      CrmSynchronisation.create!(
        crm_name: "hubspot",
        resource_type: "User",
        resource_id: user.id,
        last_synced_at: nil
      )
      expect(user_ids_for(:hubspot)).to include(user.id)
    end

    it "compares strictly: < stale, == ok, > ok" do
      time_zero = now
      user = User.create!(email: "x@x.x", updated_at: time_zero)
      create_sync!(user, crm: :hubspot, last_synced_at: time_zero - 1)
      expect(user_ids_for(:hubspot)).to include(user.id)

      CrmSynchronisation.where(
        resource_id: user.id, crm_name: "hubspot"
      ).update_all(last_synced_at: time_zero)
      expect(user_ids_for(:hubspot)).not_to include(user.id)

      CrmSynchronisation.where(
        resource_id: user.id, crm_name: "hubspot"
      ).update_all(last_synced_at: time_zero + 1)
      expect(user_ids_for(:hubspot)).not_to include(user.id)
    end

    it "no dependencies => threshold is owner's updated_at" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "id",
            crm_object_type: "contacts",
            dependencies: [],
          },
        }
      )
      user = User.create!(email: "x@x.x", updated_at: now)
      create_sync!(user, crm: :hubspot, last_synced_at: now - 1)
      expect(user_ids_for(:hubspot)).to include(user.id)
    end
  end

  # -------------------------- E. Direct dependencies ------------------------

  describe "dependencies direct associations" do
    it "belongs_to: updating company makes user stale" do
      company = Company.create!(name: "ACME")
      user = User.create!(email: "u@x.x", company: company)
      create_sync!(user, crm: :hubspot, last_synced_at: now)
      company.update!(updated_at: now + 10)
      expect(user_ids_for(:hubspot)).to include(user.id)
    end

    it "belongs_to missing target falls back to epoch safely" do
      user = User.create!(email: "u@x.x", company: nil)
      create_sync!(user, crm: :hubspot, last_synced_at: now + 10)
      expect(user_ids_for(:hubspot)).not_to include(user.id)
    end

    it "has_one: updating profile makes user stale" do
      user = User.create!(email: "u@x.x")
      profile = user.create_profile!
      create_sync!(user, crm: :hubspot, last_synced_at: now)
      profile.update!(updated_at: now + 10)
      expect(user_ids_for(:hubspot)).to include(user.id)
    end

    it "has_many: newest note updated makes user stale" do
      user = User.create!(email: "u@x.x")
      user.notes.create!(body: "a", updated_at: now)
      user.notes.create!(body: "b", updated_at: now + 20)
      create_sync!(user, crm: :hubspot, last_synced_at: now + 5)
      expect(user_ids_for(:hubspot)).to include(user.id)
    end

    it "polymorphic has_many via :as ignores unrelated rows" do
      first_user = User.create!(email: "u1@x.x")
      second_user = User.create!(email: "u2@x.x")
      first_user.uploads.create!(path: "p1", updated_at: now)
      second_user.uploads.create!(path: "p2", updated_at: now + 60)
      create_sync!(first_user, crm: :hubspot, last_synced_at: now + 10)
      expect(user_ids_for(:hubspot)).not_to include(first_user.id)
    end
  end

  # ----------- F. Through / polymorphic belongs_to (child side) -------------

  describe "through and polymorphic belongs_to" do
    it "has_many :through: source newer marks user stale" do
      user = User.create!(email: "u@x.x")
      project = Project.create!(name: "P")
      Membership.create!(user: user, project: project)
      create_sync!(user, crm: :hubspot, last_synced_at: now)
      project.update!(updated_at: now + 30)
      expect(user_ids_for(:hubspot)).to include(user.id)
    end

    it "polymorphic child: newest concrete subject wins" do
      user = User.create!(email: "u@x.x")
      activity = Activity.create!(subject: user, updated_at: now)
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "id",
            crm_object_type: "contacts",
            dependencies: [:activities],
          },
        }
      )
      create_sync!(user, crm: :hubspot, last_synced_at: now + 1)
      expect(user_ids_for(:hubspot)).not_to include(user.id)
      activity.update!(updated_at: now + 10)
      expect(user_ids_for(:hubspot)).to include(user.id)
    end

    it "polymorphic with non-constantizable type is ignored safely" do
      user = User.create!(email: "u@x.x")
      timestamp_str = now.utc.strftime("%Y-%m-%d %H:%M:%S")
      Activity.connection.execute(
        "INSERT INTO activities (subject_type, subject_id, created_at, " \
        "updated_at) VALUES ('Nope::Missing', 123, '#{timestamp_str}', " \
        "'#{timestamp_str}')"
      )
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "id",
            crm_object_type: "contacts",
            dependencies: [:activities],
          },
        }
      )
      create_sync!(user, crm: :hubspot, last_synced_at: now + 5)
      expect(user_ids_for(:hubspot)).not_to include(user.id)
    end

    it "has_many :through with polymorphic through adds type predicate" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "id",
            crm_object_type: "contacts",
            dependencies: [:poly_projects],
          },
        }
      )

      user = User.create!(email: "t@x.x")
      project = Project.create!(name: "P", updated_at: now)
      Linkage.create!(owner: user, project: project)

      create_sync!(user, crm: :hubspot, last_synced_at: now + 1)
      expect(user_ids_for(:hubspot)).not_to include(user.id)

      project.update!(updated_at: now + 20)
      expect(user_ids_for(:hubspot)).to include(user.id)
    end

    describe "has_many :through where FK lives on source table" do
      it "marks owner stale when a source row becomes newer" do
        allow(User).to receive(:etlify_crms).and_return(
          {
            hubspot: {
              adapter: Etlify::Adapters::NullAdapter.new,
              id_property: "id",
              crm_object_type: "contacts",
              dependencies: [:subscriptions],
            },
          }
        )

        user = User.create!(email: "x@x.x")
        profile = Profile.create!(user: user, updated_at: now)
        subscription = Subscription.create!(
          users_profile_id: profile.id,
          updated_at: now
        )

        create_sync!(user, crm: :hubspot, last_synced_at: now + 1)
        expect(user_ids_for(:hubspot)).not_to include(user.id)

        subscription.update!(updated_at: now + 20)
        expect(user_ids_for(:hubspot)).to include(user.id)
      end
    end
  end

  # --------------- Owner side belongs_to polymorphic (avatarable) -----------

  describe "owner belongs_to polymorphic dependency" do
    it "ignores owner-side polymorphic belongs_to (falls back to epoch)" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "id",
            crm_object_type: "contacts",
            dependencies: [:avatarable],
          },
        }
      )

      user = User.create!(email: "p@x.x")
      photo = Photo.create!(updated_at: now)
      user.avatarable = photo
      user.updated_at = now
      user.save!

      # Since owner-side polymorphic belongs_to is ignored (epoch),
      # updating the target should NOT make the owner stale.
      create_sync!(user, crm: :hubspot, last_synced_at: now + 1)
      expect(user_ids_for(:hubspot)).not_to include(user.id)

      photo.update!(updated_at: now + 20)
      expect(user_ids_for(:hubspot)).not_to include(user.id)
    end

    it "returns epoch when no concrete types exist (parts empty)" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "id",
            crm_object_type: "contacts",
            dependencies: [:avatarable],
          },
        }
      )
      user = User.create!(email: "q@x.x")
      create_sync!(user, crm: :hubspot, last_synced_at: now + 10)
      expect(user_ids_for(:hubspot)).not_to include(user.id)
    end
  end

  # -------------------------------- HABTM -----------------------------------

  describe "HABTM dependency" do
    it "marks stale when a tag becomes newer than last_sync" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "id",
            crm_object_type: "contacts",
            dependencies: [:tags],
          },
        }
      )
      user = User.create!(email: "habtm@x.x", updated_at: now)
      tag = Tag.create!(name: "x", updated_at: now)
      user.tags << tag

      create_sync!(user, crm: :hubspot, last_synced_at: now + 10)
      expect(user_ids_for(:hubspot)).not_to include(user.id)

      tag.update!(updated_at: now + 30)
      expect(user_ids_for(:hubspot)).to include(user.id)
    end
  end

  # ---------------- Self-join has_many :through aliasing -------------------

  describe "self-join has_many :through aliasing" do
    it "aliases source table to avoid PG::DuplicateAlias on Postgres" do
      # Configure Finder to scan the self-join dependency
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "id",
            crm_object_type: "contacts",
            # The dependency below triggers a users->users self-join
            dependencies: [:followees],
          },
        }
      )

      # Minimal data to build an executable relation
      follower = User.create!(email: "f@x.x", updated_at: now)
      followee = User.create!(email: "g@x.x", updated_at: now)
      Follow.create!(
        follower_id: follower.id,
        followee_id: followee.id,
        updated_at: now
      )
      create_sync!(follower, crm: :hubspot, last_synced_at: now + 1)

      relation = described_class.call(crm_name: :hubspot)[User][:hubspot]
      sql = relation.to_sql

      # The SQL must alias the source users table (e.g. "users" AS "users_src")
      expect(sql).to match(/INNER\s+JOIN\s+"users"\s+AS\s+"users_src"/i)

      # And it must be executable (no PG::DuplicateAlias at runtime)
      expect { relation.to_a }.not_to raise_error
    end
  end

  # ---------------- Nested has_many :through (profile -> join -> questionnaire) --

  describe "nested has_many :through (Profile -> Join -> SuitabilityQuestionnaire)" do
    before(:context) do
      conn = ActiveRecord::Base.connection

      unless conn.data_source_exists?("users_profiles_suitability_questionnaires")
        ActiveRecord::Schema.define do
          create_table :users_profiles_suitability_questionnaires, force: true do |t|
            t.integer :users_profile_id, null: false
            t.integer :suitability_questionnaire_id, null: false
            t.timestamps null: true
          end
          add_index :users_profiles_suitability_questionnaires,
                    [:users_profile_id, :suitability_questionnaire_id],
                    unique: true,
                    name: "idx_upsq_on_profile_and_questionnaire"
        end
      end

      unless conn.data_source_exists?("capsens_suitability_questionnaire_questionnaires")
        ActiveRecord::Schema.define do
          create_table :capsens_suitability_questionnaire_questionnaires, force: true do |t|
            t.timestamps null: true
          end
        end
      end

      # === Model stubs (table names alignés sur la prod) ======================

      Object.send(:remove_const, "SuitabilityQuestionnaire") \
        if Object.const_defined?("SuitabilityQuestionnaire")
      klass = Class.new(ApplicationRecord) do
        self.table_name = "capsens_suitability_questionnaire_questionnaires"
      end
      Object.const_set("SuitabilityQuestionnaire", klass)

      Object.send(:remove_const, "ProfilesSuitabilityQuestionnaire") \
        if Object.const_defined?("ProfilesSuitabilityQuestionnaire")
      klass = Class.new(ApplicationRecord) do
        self.table_name = "users_profiles_suitability_questionnaires"

        belongs_to :profile,
                   class_name: "Profile",
                   foreign_key: "users_profile_id",
                   optional: false
        belongs_to :suitability_questionnaire,
                   class_name: "SuitabilityQuestionnaire",
                   optional: false
      end
      Object.const_set("ProfilesSuitabilityQuestionnaire", klass)

      # === Missing associations on Profile / User =========================

      Profile.class_eval do
        has_many :profiles_suitability_questionnaires,
                 class_name: "ProfilesSuitabilityQuestionnaire",
                 foreign_key: "users_profile_id",
                 dependent: :destroy

        has_many :suitability_questionnaires,
                 through: :profiles_suitability_questionnaires,
                 source: :suitability_questionnaire
      end

      User.class_eval do
        has_many :suitability_questionnaires,
                 through: :profile,
                 source: :suitability_questionnaires
      end
    end

    it "marks user stale when a nested-through suitability questionnaire updates" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "id",
            crm_object_type: "contacts",
            dependencies: [:suitability_questionnaires],
          },
        }
      )

      t0 = now
      user = User.create!(email: "nested@x.x", updated_at: t0)
      profile = Profile.create!(user: user, updated_at: t0)

      questionnaire = SuitabilityQuestionnaire.create!(created_at: t0, updated_at: t0)
      ProfilesSuitabilityQuestionnaire.create!(
        users_profile_id: profile.id,
        suitability_questionnaire_id: questionnaire.id,
        created_at: t0,
        updated_at: t0
      )

      # Fresh sync at t0 + 1 → not stale
      create_sync!(user, crm: :hubspot, last_synced_at: t0 + 1)
      expect(user_ids_for(:hubspot)).not_to include(user.id)

      # Update questionnaire at t0 + 20 → user becomes stale
      questionnaire.update!(updated_at: t0 + 20)
      expect(user_ids_for(:hubspot)).to include(user.id)
    end
  end

  # -------------------------- G. Timestamp edge cases -----------------------

  describe "timestamp edge cases" do
    it "NULL updated_at are treated as epoch (no crash)" do
      user = User.create!(email: "u@x.x")
      note = user.notes.create!(body: "n")
      Note.where(id: note.id).update_all(updated_at: nil)
      create_sync!(user, crm: :hubspot, last_synced_at: now + 10)
      expect(user_ids_for(:hubspot)).not_to include(user.id)
    end

    it "children NULL updated_at does not mark stale unless owner newer" do
      user = User.create!(email: "u@x.x", updated_at: now)
      note = user.notes.create!(body: "n")
      Note.where(id: note.id).update_all(updated_at: nil)
      create_sync!(user, crm: :hubspot, last_synced_at: now + 5)
      expect(user_ids_for(:hubspot)).not_to include(user.id)
    end
  end

  # ---------------- Adapter portability (integration-level) -----------------

  describe "adapter portability (integration)" do
    it "uses GREATEST on Postgres and MAX on SQLite with multiple deps" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "id",
            crm_object_type: "contacts",
            dependencies: [:notes, :profile],
          },
        }
      )
      relation = described_class.call(crm_name: :hubspot)[User][:hubspot]
      sql_query = relation.to_sql
      adapter_name = ActiveRecord::Base.connection.adapter_name.to_s.downcase
      if adapter_name.include?("postgres")
        expect(sql_query).to match(/GREATEST\(/i)
      else
        expect(sql_query).to match(/MAX\(/i)
      end
    end

    it "generates executable SQL (proper quoting) on current DB" do
      relation = described_class.call(crm_name: :hubspot)[User][:hubspot]
      expect { relation.to_a }.not_to raise_error
    end
  end

  # -------------------- I. CRM-specific dependencies isolation --------------

  describe "CRM-specific dependencies isolation" do
    it "changing a dep for CRM A does not mark CRM B stale" do
      user = User.create!(email: "a@x.x")
      company = Company.create!(name: "ACME")
      user.update!(company: company)
      create_sync!(user, crm: :hubspot, last_synced_at: now)
      create_sync!(user, crm: :salesforce, last_synced_at: now)
      user.notes.create!(body: "x", updated_at: now + 30)
      results = described_class.call
      expect(results[User][:hubspot].pluck(:id)).to include(user.id)
      expect(results[User][:salesforce].pluck(:id)).not_to include(user.id)
    end

    it "changing a dep for CRM B marks stale only for CRM B" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "id",
            crm_object_type: "contacts",
            dependencies: [:notes],
          },
          salesforce: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "Id",
            crm_object_type: "Lead",
            dependencies: [:company],
          },
        }
      )
      user = User.create!(email: "b@x.x")
      company = Company.create!(name: "ACME")
      user.update!(company: company)
      create_sync!(user, crm: :hubspot, last_synced_at: now + 30)
      create_sync!(user, crm: :salesforce, last_synced_at: now)
      company.update!(updated_at: now + 60)
      results = described_class.call
      expect(results[User][:salesforce].pluck(:id)).to include(user.id)
      expect(results[User][:hubspot].pluck(:id)).not_to include(user.id)
    end
  end

  # --------------------------- J. Empty / absent CRM ------------------------

  describe "empty and absent CRM cases" do
    it "omits models not configured for targeted crm_name" do
      allow(User).to receive(:etlify_crms).and_return(
        {hubspot: User.etlify_crms[:hubspot]}
      )
      results = described_class.call(crm_name: :salesforce)
      expect(results).to eq({})
    end

    it "returns {} when no model qualifies" do
      klass = Class.new(ApplicationRecord) do
        self.table_name = "projects"
        def self.etlify_crms
          {}
        end
      end
      Object.const_set("NopeModel", klass)
      results = described_class.call(models: [NopeModel])
      expect(results).to eq({})
    ensure
      if Object.const_defined?("NopeModel")
        Object.send(:remove_const, "NopeModel")
      end
    end

    it "relation exists but can be empty when nothing is stale" do
      user = User.create!(email: "ok@x.x", updated_at: now - 1)
      create_sync!(user, crm: :hubspot, last_synced_at: now)
      relation = described_class.call(crm_name: :hubspot)[User][:hubspot]
      expect(relation).to be_a(ActiveRecord::Relation)
      expect(relation.pluck(:id)).to be_empty
    end
  end

  # ------------------------------ Robustness --------------------------------

  describe "robustness" do
    it "ignores unknown dependency names" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: User.etlify_crms[:hubspot].merge(
            dependencies: [:does_not_exist]
          ),
        }
      )
      user = User.create!(email: "u@x.x", updated_at: now)
      create_sync!(user, crm: :hubspot, last_synced_at: now + 10)
      expect(user_ids_for(:hubspot)).not_to include(user.id)
    end

    it "uses a single LEFT OUTER JOIN and exposes a single id column" do
      relation = described_class.call(crm_name: :hubspot)[User][:hubspot]
      sql_query = relation.to_sql

      # Inner subquery has exactly one LEFT OUTER JOIN on crm_synchronisations
      expect(sql_query.scan(/LEFT OUTER JOIN/i).size).to eq(1)

      # Outer select exposes a single 'id' column from the subquery alias
      expect(relation.arel.projections.size).to eq(1)
      expect(sql_query).to match(/SELECT\s+"users"\."id"/i)
    end

    it "quotes names safely to avoid crashes with reserved words" do
      relation = described_class.call(crm_name: :hubspot)[User][:hubspot]
      expect { relation.to_a }.not_to raise_error
    end

    it "orders ids ascending for stable batching" do
      first_user = User.create!(email: "a@x.x")
      second_user = User.create!(email: "b@x.x")
      user_ids = described_class.call(crm_name: :hubspot)[User][:hubspot]
                                .pluck(:id)
      expect(user_ids).to eq(user_ids.sort)
      expect(user_ids).to include(first_user.id, second_user.id)
    end

    it "skips models that define etlify_crms but have no table" do
      klass = Class.new(ApplicationRecord) do
        self.table_name = "nope_table"
        def self.etlify_crms
          {
            hubspot: {
              adapter: Etlify::Adapters::NullAdapter.new,
              id_property: "id",
              crm_object_type: "contacts",
              dependencies: [],
            },
          }
        end
      end
      Object.const_set("PhantomModel", klass)
      results = described_class.call(models: [PhantomModel], crm_name: :hubspot)
      expect(results).to eq({})
    ensure
      if Object.const_defined?("PhantomModel")
        Object.send(:remove_const, "PhantomModel")
      end
    end
  end

  # -------------------- error_count filtering -------------------------

  describe "error_count filtering" do
    before do
      Etlify.configure { |c| c.max_sync_errors = 3 }
    end

    it "excludes records with error_count >= max_sync_errors" do
      user = User.create!(email: "err@x.x")
      create_sync!(
        user, crm: :hubspot, last_synced_at: now - 3600, error_count: 3
      )
      expect(user_ids_for(:hubspot)).not_to include(user.id)
    end

    it "includes records with error_count < max_sync_errors" do
      user = User.create!(email: "retry@x.x")
      create_sync!(
        user, crm: :hubspot, last_synced_at: now - 3600, error_count: 2
      )
      expect(user_ids_for(:hubspot)).to include(user.id)
    end

    it "includes records with no sync line (never synced)" do
      user = User.create!(email: "new@x.x")
      expect(user_ids_for(:hubspot)).to include(user.id)
    end

    it "includes records with error_count = 0" do
      user = User.create!(email: "ok@x.x")
      create_sync!(
        user, crm: :hubspot, last_synced_at: now - 3600, error_count: 0
      )
      expect(user_ids_for(:hubspot)).to include(user.id)
    end

    it "respects per-CRM max_sync_errors override" do
      Etlify::CRM.register(
        :hubspot,
        adapter: Etlify::Adapters::NullAdapter.new,
        options: {max_sync_errors: 5}
      )

      user = User.create!(email: "custom@x.x")
      create_sync!(
        user, crm: :hubspot, last_synced_at: now - 3600, error_count: 4
      )
      # 4 < 5 (per-CRM limit), so still included
      expect(user_ids_for(:hubspot)).to include(user.id)

      CrmSynchronisation.where(
        resource_id: user.id, crm_name: "hubspot"
      ).update_all(error_count: 5)
      # 5 >= 5, now excluded
      expect(user_ids_for(:hubspot)).not_to include(user.id)
    ensure
      Etlify::CRM.registry.delete(:hubspot)
    end
  end

  # -------------------- stale_scope filtering -------------------------

  describe "stale_scope filtering" do
    it "restricts stale records to those matching the scope" do
      marketplace_user = User.create!(email: "market@x.x")
      other_user = User.create!(email: "other@x.x")

      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "id",
            crm_object_type: "contacts",
            dependencies: [],
            stale_scope: -> { where("email LIKE ?", "%market%") },
          },
        }
      )

      ids = user_ids_for(:hubspot)
      expect(ids).to include(marketplace_user.id)
      expect(ids).not_to include(other_user.id)
    end

    it "returns all stale records when stale_scope is nil" do
      user_a = User.create!(email: "a-nil@x.x")
      user_b = User.create!(email: "b-nil@x.x")

      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "id",
            crm_object_type: "contacts",
            dependencies: [],
            stale_scope: nil,
          },
        }
      )

      ids = user_ids_for(:hubspot)
      expect(ids).to include(user_a.id, user_b.id)
    end

    it "combines stale_scope with staleness threshold" do
      marketplace_user = User.create!(email: "market-combo@x.x", updated_at: now)
      other_user = User.create!(email: "other-combo@x.x", updated_at: now)

      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "id",
            crm_object_type: "contacts",
            dependencies: [],
            stale_scope: -> { where("email LIKE ?", "%market%") },
          },
        }
      )

      # Both synced fresh => neither stale
      create_sync!(marketplace_user, crm: :hubspot, last_synced_at: now + 10)
      create_sync!(other_user, crm: :hubspot, last_synced_at: now + 10)

      ids = user_ids_for(:hubspot)
      expect(ids).not_to include(marketplace_user.id)
      expect(ids).not_to include(other_user.id)

      # Make marketplace_user stale, other_user also stale but excluded by scope
      marketplace_user.update!(updated_at: now + 20)
      other_user.update!(updated_at: now + 20)

      ids = user_ids_for(:hubspot)
      expect(ids).to include(marketplace_user.id)
      expect(ids).not_to include(other_user.id)
    end
  end

  # -------------------- STI (Single Table Inheritance) ----------------------

  describe "STI model support" do
    before do
      allow(LendingSubscription).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "id",
            crm_object_type: "deals",
            dependencies: [],
          },
        }
      )
    end

    def lending_ids_for(crm)
      described_class
        .call(models: [LendingSubscription], crm_name: crm)
        .dig(LendingSubscription, crm)
        &.pluck(:id) || []
    end

    it "does not crash on STI subclass (no PG::UndefinedColumn)" do
      sub = LendingSubscription.create!(updated_at: now)
      relation = described_class.call(
        models: [LendingSubscription],
        crm_name: :hubspot
      ).dig(LendingSubscription, :hubspot)

      expect(relation).to be_a(ActiveRecord::Relation)
      expect { relation.to_a }.not_to raise_error
      expect(relation.pluck(:id)).to include(sub.id)
    end

    it "returns only STI subclass records, not the whole table" do
      base_record = Subscription.create!(updated_at: now)
      lending = LendingSubscription.create!(updated_at: now)

      ids = lending_ids_for(:hubspot)
      expect(ids).to include(lending.id)
      expect(ids).not_to include(base_record.id)
    end

    it "detects stale STI records correctly" do
      sub = LendingSubscription.create!(updated_at: now)
      create_sync!(sub, crm: :hubspot, last_synced_at: now + 10)

      # Fresh => not stale
      expect(lending_ids_for(:hubspot)).not_to include(sub.id)

      # Update makes it stale
      sub.update!(updated_at: now + 20)
      expect(lending_ids_for(:hubspot)).to include(sub.id)
    end
  end
end
