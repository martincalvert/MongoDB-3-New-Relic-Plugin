"""
Plugins are responsible for fetching and parsing the stats from the service
being profiled.

"""
available = {
    'mongodb': 'newrelic_plugin_agent.plugins.mongodb.MongoDB',
    }
