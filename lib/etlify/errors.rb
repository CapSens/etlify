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

  class RateLimited < ApiError; end             # 429

  class ValidationFailed < ApiError; end        # 409/422

  # Configuration error (update)
  class MissingColumnError < StandardError; end
end
