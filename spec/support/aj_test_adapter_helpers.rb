# frozen_string_literal: true

module AJTestAdapterHelpers
  def aj_set_test_adapter!
    ActiveJob::Base.queue_adapter =
      ActiveJob::QueueAdapters::TestAdapter.new
  end

  def aj_enqueued_jobs
    ActiveJob::Base.queue_adapter.enqueued_jobs
  end

  def aj_performed_jobs
    ActiveJob::Base.queue_adapter.performed_jobs
  end

  def aj_clear_jobs
    aj_enqueued_jobs.clear
    aj_performed_jobs.clear
  end

  # Perform only immediate jobs; scheduled (with :at) are left in queue.
  def aj_perform_enqueued_jobs
    jobs = aj_enqueued_jobs.dup
    aj_enqueued_jobs.clear
    jobs.each do |j|
      next if j[:at]

      j[:job].perform_now(*j[:args])
    end
  end
end
