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

      def batch_upsert!(records:, object_type:, id_property:)
        prop = id_property.to_s
        records.each_with_object({}) do |r, h|
          key = (r[prop] || r[prop.to_sym] || SecureRandom.uuid).to_s
          h[key] = SecureRandom.uuid
        end
      end

      def batch_delete!(crm_ids:, object_type:)
        true
      end
    end
  end
end
