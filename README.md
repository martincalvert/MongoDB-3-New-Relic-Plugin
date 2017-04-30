# MongoDB 3.2+ New Relic Plugin

This is was forked from the Meet Me repo. It has been stripped down to support just mongo currently, but can be easily extended for other applications.

### Mongo Plugin

__Requirements__
- Python 2.7
- Python pip
- Pymongo 3.3

__Installation__
1. Clone repo
2. `pip install .`
3. Edit the newrelic-plugin-agent.cfg

__Running__

`newrelic-plugin-agent -c /path/to/config.cfg`

_add `-f` for foreground monitoring_


### Setup
1. Add your newrelic license key into the config file
2. Alter the GUID in mongodb.py so it shows up in your new relic account
3. Run the plugin
4. Build your dashboards

### Deploying
Currently I don't include any packaging with the plugin, I would suggest dropping it off with chef (git resource) then adding a service file to start it.
