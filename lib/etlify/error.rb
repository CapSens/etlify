module Etlify
  class Error < StandardError
    attr_reader(
      :status,
      :code,
      :category,
      :correlation_id,
      :details,
      :raw
    )

    def initialize(
      message,
      status:,
      code: nil,
      category: nil,
      correlation_id: nil,
      details: nil,
      raw: nil
    )
      super(message)
      @status         = status
      @code           = code
      @category       = category
      @correlation_id = correlation_id
      @details        = details
      @raw            = raw
    end
  end

  # Network / transport errors (DNS, TLS, timeouts, etc.)
  class TransportError < Error; end

  # HTTP errors
  class ApiError < Error; end
  class Unauthorized < ApiError; end            # 401/403
  class NotFound < ApiError; end                # 404
  # 429
  class RateLimited < ApiError
    # Seconds to wait before retrying, when the raiser knows the quota
    # window better than BatchSyncJob's default (nil otherwise).
    attr_reader :retry_after

    def initialize(message, retry_after: nil, **options)
      super(message, **options)
      @retry_after = retry_after
    end
  end
  class ValidationFailed < ApiError; end        # 409/422

  # Internal errors
  class SyncError < StandardError; end

  # Configuration error (update)
  class MissingColumnError < StandardError; end
end
