module Etlify
  module Adapters
    # Adapter no-op pour dev/test
    class NullAdapter
      def upsert!(payload:, object_type:, match_property:, match_value:, crm_id: nil)
        return crm_id.to_s unless crm_id.to_s.strip.empty?

        value = match_value.to_s.strip
        value.empty? ? SecureRandom.uuid : value
      end

      def delete!(crm_id:, object_type:)
        true
      end

      # Mirrors the real adapters' contract: inputs are {value:, properties:}
      # and the returned mapping is keyed by each input's match value.
      def batch_upsert!(inputs:, object_type:, match_property:)
        inputs.each_with_object({}) do |input, h|
          value = (input[:value] || input["value"]).to_s.strip
          key = value.empty? ? SecureRandom.uuid : value
          h[key] = SecureRandom.uuid
        end
      end

      # Update by crm_id (no-op): echoes back each provided crm_id.
      def batch_update!(records:, object_type:)
        records.each_with_object({}) do |r, h|
          id = (r[:crm_id] || r["crm_id"]).to_s
          h[id] = id unless id.empty?
        end
      end

      def batch_delete!(crm_ids:, object_type:)
        true
      end
    end
  end
end
