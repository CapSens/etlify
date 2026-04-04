module Etlify
  module Adapters
    module AirtableV0
      # Helpers for building Airtable filterByFormula
      # expressions.
      module Formula
        SAFE_FIELD_NAME = /\A[\w\s\-]+\z/

        module_function

        # Build a filterByFormula equality expression.
        # @param field_name [String]
        # @param value [String, Numeric]
        # @return [String] e.g. '{Email} = "john@example.com"'
        def eq(field_name, value)
          validate_field_name!(field_name)
          "{#{field_name}} = #{escape(value)}"
        end

        # Escape a value for use inside a formula string.
        # Numerics are left unquoted; strings are
        # double-quoted with internal quotes escaped.
        # Non-scalar types raise ArgumentError.
        def escape(value)
          if value.is_a?(Numeric)
            value.to_s
          elsif value.is_a?(String) || value.is_a?(Symbol)
            escaped_value = value.to_s
                                 .gsub("\\", "\\\\\\\\")
                                 .gsub('"', '\\"')
            "\"#{escaped_value}\""
          else
            raise ArgumentError,
                  "Formula value must be a String, Symbol, or Numeric (got #{value.class})"
          end
        end

        def validate_field_name!(field_name)
          return if field_name.is_a?(String) && field_name.match?(SAFE_FIELD_NAME)

          raise ArgumentError,
                "Invalid Airtable field name: #{field_name.inspect}"
        end
      end
    end
  end
end
