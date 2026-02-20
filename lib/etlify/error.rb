# frozen_string_literal: true

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

  # 401/403
  class Unauthorized < ApiError; end

  # 404
  class NotFound < ApiError; end

  # 429
  class RateLimited < ApiError; end

  # 409/422
  class ValidationFailed < ApiError; end

  # Internal errors
  class SyncError < StandardError; end

  # Configuration errors
  class MissingColumnError < StandardError; end

  class MissingTableError < StandardError; end
end
