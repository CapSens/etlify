# frozen_string_literal: true

module RSpecTimeHelpers
  # Internal stack to support nested freeze/travel
  def __time_stack
    @__rspec_time_stack ||= []
  end

  # Freeze to a specific moment. Works with or without a block.
  def freeze_time(moment = Time.now, &blk)
    travel_to(moment, &blk)
  end

  # Travel to a specific moment. If block given, auto-restore afterwards.
  def travel_to(moment)
    push_time_stub(moment)
    if block_given?
      begin
        yield
      ensure
        travel_back
      end
    end
  end

  # Travel by a duration (Numeric seconds or ActiveSupport::Duration).
  # If block given, auto-restore afterwards; else, stays until travel_back.
  def travel(duration, &blk)
    travel_to(Time.now + duration, &blk)
  end

  # Undo one level of travel/freeze. Restore previous or original behavior.
  def travel_back
    __time_stack.pop
    if (prev = __time_stack.last)
      apply_time_stub(prev)
    else
      remove_time_stub
    end
  end

  private

  def push_time_stub(moment)
    __time_stack << moment
    apply_time_stub(moment)
  end

  def apply_time_stub(moment)
    # Stub Time.now
    allow(Time).to receive(:now).and_return(moment)

    # Stub Time.current
    allow(Time).to receive(:current).and_return(
      (moment.respond_to?(:in_time_zone) ? moment.in_time_zone : moment)
    )
  end

  def remove_time_stub
    allow(Time).to receive(:now).and_call_original
    allow(Time).to receive(:current).and_call_original
  end
end
