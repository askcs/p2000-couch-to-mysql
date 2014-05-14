p2000-couch-to-mysql
====================

Synchronization scripts for P2000 messages from a CouchDB to a MySQL db.

Scripts:

- app.js
	- Realtime sync between CouchDB and MySQL; *CURRENTLY NOT USED; NOT AS GOOD AS SYNC-BACK.JS*
- clean-couchdb.js
	- Just a copy of another file, not used yet; should contain code to remove old CouchDB items
- continues-sync.js
	- Script used as NodeJS cronjob implementation. Uses the node-schedule plugin to run the sync-back.js script every minute.
- sync-back.js
	- Currently the *most important file of all*. This script handles the complete sync process between CouchDB and MySQL: from getting the messages from CouchDB in batches to doing all the checks and keeping track of the state of the sync process.
	
All files contain a detailed description of what they (are supposed to) do in the header.


Config
====================

Create one or more config files to use in the scripts. By default the script assumes there is a config-development.ini and a config-production.ini file.

A config-example.ini is included in the repository.

Install
====================

Node application, so run _npm install_ from the command line.

Run - Testing/dev (local/remote)
====================

By default the current implementation will use the production ini file as default, so you can just run the node app without arguments:
_node sync-back.js_

If you're also running the script locally you can add a environment parameter and run it like this:
_node sync-back.js dev_

By adding this dev parameter the script will now use the config-development.ini file to get the CouchDB and MySQL DB details.

Run - Permanent setup
====================

For running everything forever we actually use the 'forever' node plugin (first install forever by running: _npm install forever_). Run the following command to let everything happen automatically from that moment:

_forever start continues-sync.js_

This will make sure the script will run *forever* and within the code it will execute the batch sync script (sync-back.js) every minute.
