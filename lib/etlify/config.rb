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

      rails_logger = (defined?(Rails) && Rails.respond_to?(:logger)) ? Rails.logger : nil
      @logger      = rails_logger || Logger.new($stdout)

      rails_cache  = (defined?(Rails) && Rails.respond_to?(:cache)) ? Rails.cache : nil
      @cache_store = rails_cache || ActiveSupport::Cache::MemoryStore.new
    end
  end
end
