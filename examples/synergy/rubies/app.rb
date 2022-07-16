require "redis-namespace"
require "sidekiq"

Sidekiq.configure_server do |config|
  config.redis = {
    url: "redis://127.0.0.1:6379",
    namespace: "yolo_app",
  }
end

Sidekiq.configure_client do |config|
  config.redis = {
    url: "redis://127.0.0.1:6379",
    namespace: "yolo_app",
  }
end

module V2
  class StatisticsWorker
    include Sidekiq::Worker
    sidekiq_options queue: "ruby:v2_statistics"

    def perform(payload)
      puts "Received a payload (v2): #{payload}"
    end
  end
end

class V1StatisticsWorker
  include Sidekiq::Worker
  sidekiq_options queue: "ruby:v1_statistics"

  def perform(payload)
    puts "Received a payload (v1): #{payload}"
  end
end

Thread.new do
  loop do
    V1StatisticsWorker.
      set(:queue => "rust:v1_statistics").
      perform_async({"metric" => "temp.outside", "value" => 10.01})

    V2::StatisticsWorker.
      set(:queue => "rust:v2_statistics").
      perform_async({"metric" => "temp.inside", "value" => 20.02})

    sleep 5
  end
end
