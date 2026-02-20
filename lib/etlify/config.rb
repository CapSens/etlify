module Etlify
  class Config
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
      @max_sync_errors = 3
    end
  end
end
