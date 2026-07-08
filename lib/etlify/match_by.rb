module Etlify
  # Validation and resolution of the `match_by` DSL option.
  #
  # `match_by` decouples CRM record matching from the synced payload:
  #   match_by: {
  #     property: :email,                    # CRM property used to match
  #     value: ->(record) { record.email },  # proc or method symbol
  #   }
  #
  # The serializer payload is synced as-is: the matching property is only
  # written at creation time (never on update), unless the serializer
  # explicitly includes it in the payload.
  module MatchBy
    class << self
      # Validate the `match_by` option at DSL declaration time.
      # @param match_by [Hash] {property:, value:}
      # @raise [ArgumentError] when malformed
      def validate!(match_by)
        unless match_by.is_a?(Hash)
          raise ArgumentError,
                "match_by must be a Hash like " \
                "{property: :email, value: ->(record) { record.email }}"
        end

        property = match_by[:property]
        if !(property.is_a?(Symbol) || property.is_a?(String)) ||
            property.to_s.strip.empty?
          raise ArgumentError,
                "match_by[:property] must be a non-empty Symbol or String"
        end

        value = match_by[:value]
        unless value.is_a?(Proc) || value.is_a?(Symbol)
          raise ArgumentError,
                "match_by[:value] must be a Proc or a method name Symbol"
        end

        match_by
      end

      # CRM property name used for matching.
      # @param conf [Hash] etlify_crms configuration entry
      # @return [String]
      def property(conf)
        conf.fetch(:match_by).fetch(:property).to_s
      end

      # Resolve the matching value for a resource.
      # @param resource [Object]
      # @param conf [Hash] etlify_crms configuration entry
      # @return [String] stripped value ("" when the source is nil/blank)
      def resolve(resource, conf)
        value = conf.fetch(:match_by).fetch(:value)
        raw = value.is_a?(Proc) ? value.call(resource) : resource.public_send(value)
        raw.to_s.strip
      end
    end
  end
end
