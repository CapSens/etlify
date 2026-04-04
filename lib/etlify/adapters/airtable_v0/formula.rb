# frozen_string_literal: true

module Etlify
  module Adapters
    module AirtableV0
      # Helpers for building Airtable filterByFormula
      # expressions.
      module Formula
        module_function

        # Build a filterByFormula equality expression.
        # @param field_name [String]
        # @param value [Object]
        # @return [String] e.g. '{Email} = "john@example.com"'
        def eq(field_name, value)
          "{#{field_name}} = #{escape(value)}"
        end

        # Escape a value for use inside a formula string.
        # Numerics are left unquoted; everything else is
        # double-quoted with internal quotes escaped.
        def escape(value)
          if value.is_a?(Numeric)
            value.to_s
          else
            escaped_value = value.to_s
                                 .gsub("\\", "\\\\\\\\")
                                 .gsub('"', '\\"')
            "\"#{escaped_value}\""
          end
        end
      end
    end
  end
end
