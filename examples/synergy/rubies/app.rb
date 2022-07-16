require "sidekiq"

module V2
  class StatisticsWorker
    include Sidekiq::Worker
    sidekiq_options queue: :v2_statistics

    def perform(payload)
      puts "Received a payload (v2): #{payload}"
    end
  end
end

class V1StatisticsWorker
  include Sidekiq::Worker
  sidekiq_options queue: :v1_statistics

  def perform(payload)
    puts "Received a payload (v1): #{payload}"
  end
end
