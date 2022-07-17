require 'sidekiq'

Sidekiq.configure_client do |config|
  config.redis = {
    url: "redis://127.0.0.1:6379",
    namespace: "yolo_app",
  }
end

require 'sidekiq/web'

map "/" do
  use Rack::Session::Cookie, secret: SecureRandom.hex(32), same_site: true, max_age: 86400
  run Sidekiq::Web
end
