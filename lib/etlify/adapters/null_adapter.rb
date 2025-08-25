# frozen_string_literal: true

module Etlify
  module Adapters
    # Adapter no-op pour dev/test
    class NullAdapter
      def upsert!(payload:, object_type:, id_property:)
        payload.fetch(id_property, SecureRandom.uuid).to_s
      end

      def delete!(crm_id:, object_type:)
        true
      end
    end
  end
end
