module Etlify
  class Config
    DEFAULT_MAX_SYNC_ERRORS = 3

    attr_accessor(
      :digest_strategy,
      :job_queue_name,
      :cache_store,
      :max_sync_errors
    )

    def initialize
      @digest_strategy = Etlify::Digest.method(:stable_sha256)
      @job_queue_name  = "low"
      @cache_store = Rails.cache || ActiveSupport::Cache::MemoryStore.new
      @max_sync_errors = DEFAULT_MAX_SYNC_ERRORS
    end
  end
end
