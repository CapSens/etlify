module Etlify
  class Config
    attr_accessor(
      :digest_strategy,
      :logger,
      :job_queue_name,
      :cache_store
    )

    def initialize
      @digest_strategy = Etlify::Digest.method(:stable_sha256)
      @job_queue_name  = "low"
      @logger      = Rails.logger || Logger.new($stdout)
      @cache_store = Rails.cache || ActiveSupport::Cache::MemoryStore.new
    end
  end
end
