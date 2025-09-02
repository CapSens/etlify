# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Etlify errors" do
  it "carry rich context attributes" do
    err = Etlify::ApiError.new(
      "bad",
      status: 422,
      code: "unprocessable",
      category: "validation",
      correlation_id: "cid-123",
      details: {field: "email"},
      raw: {http: "body"}
    )

    expect(err.message).to eq("bad")
    expect(err.status).to eq(422)
    expect(err.code).to eq("unprocessable")
    expect(err.category).to eq("validation")
    expect(err.correlation_id).to eq("cid-123")
    expect(err.details).to eq({field: "email"})
    expect(err.raw).to eq({http: "body"})
  end

  it "provides subclasses for transport, HTTP and config" do
    expect(Etlify::TransportError.new("x", status: 0)).to be_a(Etlify::Error)
    expect(Etlify::Unauthorized.new("x", status: 401)).to be_a(Etlify::ApiError)
    expect(Etlify::NotFound.new("x", status: 404)).to be_a(Etlify::ApiError)
    expect(Etlify::RateLimited.new("x", status: 429)).to be_a(Etlify::ApiError)
    expect(
      Etlify::ValidationFailed.new("x", status: 422)
    ).to be_a(Etlify::ApiError)
    expect(Etlify::MissingColumnError).to be < StandardError
  end
end
