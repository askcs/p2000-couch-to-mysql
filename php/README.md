Webpaige API and helper PHP scripts
====

Rename _config-example.php_ to _config.php_ and provide your own database details.

_Cleanup.php_ runs daily via a cronjob with parameter: delete=1. This will cleanup the testmessages that got into the database through the sync script filters. If you don't use delete=1 as param you can view which testmessages are currently in the database and ready to be removed; including their _link_ with capcodes. This script won't delete _capcodes itself_ though!

Examples for _p2000.php_:

- http://couchdb.ask-cs.com/~jordi/p2000/p2000.php?code=1400999&callback=jQuery123&capcodelimit=2
- http://couchdb.ask-cs.com/~jordi/p2000/p2000.php?code=1420999,1400999&callback=jQuery123&limit=3

Params:

- _code_: Can be a comma seperated string or array:
  - code=1420999,1400999
  - code[]=1420999&code[]=1400999
- _callback_ (string): Any JSONP (jQuery) callback function name (string)
- _capcodelimit_ (int): Restricts the number of capcodes that is returned for each message. If there are more capcodes than the limit, a +X will be added to indicate how much more capcodes there are for that P2000 message
- _limit_ (int): Defines how much P2000 messages are returned in the response

NOTE: Ask P2000 database goes back to december 29 2013.