from walrus import Database
import yaml

# Connecting Redis Database
class DatabaseSetup:

  # Database configs
  with open('config/database.yaml', 'r') as f:
    config = yaml.load(f)

  db = Database(host = config['redisdb']['host'], \
                port = config['redisdb']['port'], \
                db = config['redisdb']['db'])