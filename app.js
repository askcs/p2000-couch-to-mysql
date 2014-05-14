/*
 *
 * The app.js script will constantly listen on the couchdb changes stream.
 *
 * It will insert the P2000 message in near-realtime into the MySQL database
 * Some messages and/or capcodes will be skipped; e.g. testmessages and one
 * specific groupcapcode that it part of a lot of messages, but it not used.
 *
 * If the script is stopped (manually or because of an error); it can be
 * restarted and will continue will the last message that was successfully
 * processed into the MySQL database.
 *
 */

// Determine the environment based on the parameter
var env = process.argv[2];
var configFilePath;
switch (env) {
    case 'dev':
        configFilePath = './config-development.ini';
        break;
    case 'prod':
	default:
        // Default: Setup production config
		configFilePath = './config-production.ini';
        break;
}

// Read settings from ini file
var fs = require('fs')
  , ini = require('ini');

var config = ini.parse(fs.readFileSync(configFilePath, 'utf-8'))
console.log(config);

// Setup Couch DB connection
var cradle = require('cradle');
var db  = new(cradle.Connection)(config.couch.host, config.couch.port, {
	  cache: false,
	  raw: false,
	  forceSave: true
	}).database( config.couch.database );
 
// Setup MySQL DB connection (pool; so we can perform
var mysql      = require('mysql');
var pool = mysql.createPool({
	host     : config.mysql.host,
	user     : config.mysql.user,
	password : config.mysql.password,
	database : config.mysql.database,
	waitForConnections	: true,
	connectionLimit		: 120
});

// Keep track of the application state
var stateFile = 'state/last-msg-seq';
var lastSavedMessage = fs.readFileSync( stateFile, 'UTF-8' ); // Blocking call to make sure we have it before starting to listen for changes

console.log('Started listening for CouchDB changes from sequence ID: ' + lastSavedMessage);

// Listen to changes in the CouchDB
var feed = db.changes({ since: lastSavedMessage, include_docs: true });
feed.on('change', function (change) {

	//console.log('[C] ' + change.seq + " - " + change.id);
	//console.log(">>" + change.changes[0].rev);
	//console.log(change);

	// Shortcut
	var doc = change.doc;

	console.log('[P] ' + doc.message + ''+"\n"+'<' + change.seq + ':' + change.id + ':' + change.changes[0].rev + '>');

	// Get a MySQL connection (for parallel queries)
	pool.getConnection(function(err, connection){

		if (err){
			console.log( 'Error getting a connection' ); 
			console.log( err ); 
			return; //throw err;
		}
		
		console.log('We got a MySQL connection');
		
		// Check if the p2000 message already exists
		var query = connection.query('SELECT id FROM messages WHERE timestamp="'+doc.timestamp+'" AND message="'+doc.message+'"', function(err, rows, fields) {
				
			if (err){
				console.log( 'Error during inserting the p2000 message' ); 
				console.log( err ); 
				return; // throw err;
			}
		
			// The message doesnt exist yet; add it to the database
			if(typeof rows[0] == "undefined" || rows[0] == null){
		
				// Insert this document into MySQL
				var query = connection.query('INSERT INTO messages (timestamp, message) VALUES ("'+doc.timestamp+'", "'+doc.message+'")', function(err, result) {
						
					if (err){
						console.log( 'Error during inserting the p2000 message' ); 
						console.log( err ); 
						return; // throw err;
					}
					
					var p2000MessageId = result.insertId;
					
					console.log('[I] ' + doc.message + ' <' + p2000MessageId + '>');
					
					// Insert metadata for this message
					insertMessageMetaData(doc, p2000MessageId, change.seq, connection);
					
				});
				
			} else {
				
				// Insert metadata for this message
				insertMessageMetaData(doc, rows[0].id, change.seq, connection);
				
			}

		});
		
	});
  
});

feed.on('error', function (err) {
	console.log('Error event: ' + err);
});

feed.on('stop', function () {
	console.log('Changefeed stopped');
});

// Insert if not exists; message-capcode links
function insertMessageCapcodeLink(p2000MessageId, capcodeId, connection){

	// Check if the link between the message and the capcode already exists
	var query = connection.query('SELECT * FROM message_has_capcode WHERE message_id="'+p2000MessageId+'" AND capcode_id="'+capcodeId+'" LIMIT 1', function(err, rows, fields) {
	
		if (err){
			console.log( 'Error during the message-capcode link check' ); 
			console.log( err ); 
			return; // throw err;
		}
		
		// The link doesnt exist yet; add it to the database
		if(typeof rows[0] == "undefined" || rows[0] == null){
	
			// Now insert the link between the message and the capcode in the link-table
			var query = connection.query('INSERT INTO message_has_capcode (message_id, capcode_id) VALUES ("'+p2000MessageId+'", "'+capcodeId+'")', function(err, result) {

				if (err){
					console.log( 'Error inserting the message-capcode link' ); 
					console.log( err ); 
					return; // throw err;
				}
			
				console.log('Inserted a link between the message ('+p2000MessageId+') and the capcode ('+capcodeId+') to the database');
				
				// Release the connection
				connection.release();
				
			});
			
		} else {
			// Release the connection
			connection.release();
		}
		
	});
}

// Insert if not exists; capcodes
var lastKnownDocSequence;
var alreadyWritingToFile = false;
function insertMessageMetaData(doc, p2000MessageId, docSequence, connection){

	// Create capcode and the link between the message and the capcode
	var numCapcodes = doc.capcodes.length;
	console.log('Num of capcodes: ' + numCapcodes );
	for (var i=0; i < numCapcodes; i++){
	
		var processingCapcode = doc.capcodes[i];
		console.log('Checking capcode: ' + processingCapcode);
		
		// Check if the capcode already exists in the capcode table
		var query = connection.query('SELECT id FROM capcodes WHERE capcode="'+processingCapcode+'" LIMIT 1', function(err, rows, fields) {
		
			if (err){
				console.log( 'Error during the capcode check' ); 
				console.log( err ); 
				return; // throw err;
			}
			
			//console.log('RESULT: ');
			//console.log(rows);
			//console.log(fields);
			
			// Capcode doesn't exist yet in the database; add it
			if(typeof rows[0] == "undefined" || rows[0] == null){
			
				console.log('Inserting capcode ' + processingCapcode + ' into the capcode database');
				
				// Insert the capcode to the capcode table - if they don't already exist
				var query = connection.query('INSERT INTO capcodes (capcode) VALUES ("'+processingCapcode+'")', function(err, result) {
				
					if (err){
						console.log( 'Error inserting a capcode to the capcode database' ); 
						console.log( err ); 
						return; // throw err;
					}
					
					var capcodeId = result.insertId;
					
					console.log("Inserted the capcode in the database with ID: " + capcodeId);
					
					// Insert the link
					insertMessageCapcodeLink(p2000MessageId, capcodeId, connection);
					
				});
			
			} else {
				
				// Insert the link
				insertMessageCapcodeLink(p2000MessageId, rows[0].id, connection);
				
			}
			
		});
		
	}
	
	lastKnownDocSequence = docSequence;
	
	// Write the sequence ID of this message as 'last inserted message'
	if(!alreadyWritingToFile && typeof docSequence != "undefied" && docSequence != ""){
		alreadyWritingToFile = true;
		fs.writeFileSync( stateFile , docSequence );
		alreadyWritingToFile = false;
	}

}

/*
process.on('exit', function(code) {
	// Write the sequence ID of the last known message as 'last inserted message'
	if(typeof lastKnownDocSequence != "undefined" && lastKnownDocSequence != null && lastKnownDocSequence != ""){
		console.log('Writing to last message state file');
		fs.writeFile( stateFile , lastKnownDocSequence );
		console.log('Written document ID ' + lastKnownDocSequence + ' to the state file');
	}
	
  console.log('About to exit with code:', code);
});
//*/