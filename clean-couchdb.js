/*
 * 
 * The clean-couchdb.js script will run every hour and delete all documents
 * which have lower IDs then the current latest document ID minus X.
 *
 * The script will request the 'oldest' messages in the couchdb and will 
 * only keep the latest X messages in there. Requesting the documents will
 * be done in batches.
 *
 * When a message is considered old; its instantly deleted from the couchdb.
 * It should have been synced to MySQL before this happenend, otherwise the
 * message is gone forever (sort of).
 *
*/

// Debug
var debug = false;

// CLI utility
var cli = require('cli');

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
console.log('Starting sync engine: ' + timeString() );
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
	supportBigNumbers	: true,
	waitForConnections	: true,
	connectionLimit		: 120
});

var stateFile = 'state/last-mass-synced-msg-seq';
var listLength = 0;
var listCounter = 0;
var numOfTestMsgs = 0;
var lastKnownHighestId = 0;

var insertedMessages = 0;
var insertedCapcodes = 0;
var insertedCapcodeLinks = 0;

function runSync(){

	// Keep track of the application state
	var lastSavedMessage = fs.readFileSync( stateFile, 'UTF-8' ); // Blocking call to make sure we have it before starting to listen for changes
	var maxMessagesPerBatch = 567; // To get some changing numbers which are easier to recognize

	var toId = ( parseInt(lastSavedMessage) + parseInt(maxMessagesPerBatch) ); //TODO: toId
	console.log('['+timeString()+'] Getting a batch of changes as a list, getting items: ' + lastSavedMessage + ' - ' + toId + '. (' + maxMessagesPerBatch + ')');

	// Listen to changes in the CouchDB
	var feed = db.changes({ since: lastSavedMessage, limit: maxMessagesPerBatch, include_docs: true }, function (err, list) {
	
		//console.log(list);
		
		if (err){
		
			// Release the connection
			connection.release();
			
			if(!debug) return;
			
			console.log( 'Error getting a batch of changes: ' ); 
			console.log( err ); 
			return; //throw err;
		}
		
		listLength = list.length;
		listCounter = 0;
		
		insertedMessages = 0;
		insertedCapcodes = 0;
		insertedCapcodeLinks = 0;
		
		console.log('Actual size of the retrieved batch: ' + listLength );
		
		list.forEach(function (change) {
			
			//console.log('[C] ' + change.seq + " - " + change.id);
			//console.log(">>" + change.changes[0].rev);
			//console.log(change);

			// Shortcut
			var doc = change.doc;
			var docSequence = change.seq;
			
			// Skip testmessages
			if(typeof doc.message == "string"){
				var loweredMsg = doc.message.toLowerCase();
				if(loweredMsg == 'test' || 
					loweredMsg.indexOf('testpage') >= 0 || 
					loweredMsg.indexOf('test ') >= 0 || 
					loweredMsg.indexOf('test voor p2000') >= 0 || 
					loweredMsg.indexOf('test voor c2000') >= 0 || 
					loweredMsg.indexOf('pagertest') >= 0 || 
					loweredMsg.indexOf('testbericht') >= 0 || 
					loweredMsg.indexOf('testoproep') >= 0 || 
					loweredMsg.indexOf('testmelding') >= 0 || 
					loweredMsg.indexOf('-test') >= 0 || 
					loweredMsg.indexOf('test,') >= 0 ||
					loweredMsg.indexOf('test.') >= 0 ||
					loweredMsg.indexOf('test!') >= 0 ||
					loweredMsg.indexOf('testje') >= 0 ||
						(loweredMsg.indexOf('test') && loweredMsg.length < 15) // Also exclude short messages that contain 'test' somewhere
					){
					
					numOfTestMsgs++;
					finishProcessingMessage(docSequence, null);
					return;
				}
			} else {
				// For some reason this happenend wherearound sequence 870.584: "Object true has no method 'toLowerCase'"
				finishProcessingMessage(docSequence, null);
				return;
			}

			//console.log('[P] ' + doc.message + ''+"\n"+'<' + change.seq + ':' + change.id + ':' + change.changes[0].rev + '>');

			// Get a MySQL connection (for parallel queries)
			pool.getConnection(function(err, connection){

				if (err){
				
					finishProcessingMessage(docSequence, connection);
						
					if(!debug) return;
					
					console.log( 'Error getting a connection' ); 
					console.log( err ); 
					return; //throw err;
				}
				
				//console.log('We got a MySQL connection');
				
				// Check if the p2000 message already exists
				var query = connection.query('SELECT id FROM messages WHERE timestamp="'+doc.timestamp+'" AND message="'+doc.message+'"', function(err, rows, fields) {
						
					if (err){
					
						finishProcessingMessage(docSequence, connection);
						
						if(!debug) return;
						
						console.log( 'Error during inserting the p2000 message' ); 
						console.log( err ); 
						return; // throw err;
					}
				
					// The message doesnt exist yet; add it to the database
					if(typeof rows[0] == "undefined" || rows[0] == null){
				
						// Insert this document into MySQL
						var query = connection.query('INSERT INTO messages (timestamp, message) VALUES ("'+doc.timestamp+'", "'+doc.message+'")', function(err, result) {
								
							if (err){
							
								finishProcessingMessage(docSequence, connection);
				
								if(!debug) return;
								
								console.log( 'Error during inserting the p2000 message' ); 
								console.log( err ); 
								return; // throw err;
							}
							
							var p2000MessageId = result.insertId;
							
							insertedMessages++;
							//console.log('[I] ' + doc.message + ' <' + p2000MessageId + '>');
							
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
	  
	});
	
	feed.on('error', function (err) {
		if(debug){
			console.log('Error event: ' + err);
		}
	});

	feed.on('stop', function () {
		console.log('Changefeed stopped');
	});
}

// Start up the sync engines
runSync();

// Insert if not exists; message-capcode links
function insertMessageCapcodeLink(p2000MessageId, capcodeId, connection, docSequence){

	// Check if the link between the message and the capcode already exists
	var query = connection.query('SELECT * FROM message_has_capcode WHERE message_id="'+p2000MessageId+'" AND capcode_id="'+capcodeId+'" LIMIT 1', function(err, rows, fields) {
	
		if (err){
		
			finishProcessingMessage(docSequence, connection);
				
			if(!debug) return;
			
			if(err.code == "ER_DUP_ENTRY") return; // Skip this common error
			console.log( 'Error during the message-capcode link check' ); 
			console.log( err ); 
			return; // throw err;
		}
		
		// The link doesnt exist yet; add it to the database
		if(typeof rows[0] == "undefined" || rows[0] == null){
	
			// Now insert the link between the message and the capcode in the link-table
			var query = connection.query('INSERT INTO message_has_capcode (message_id, capcode_id) VALUES ("'+p2000MessageId+'", "'+capcodeId+'")', function(err, result) {

				if (err){
					
					finishProcessingMessage(docSequence, connection);
						
					if(!debug) return;
					
					console.log( 'Error inserting the message-capcode link' ); 
					console.log( err ); 
					return; // throw err;
				}
			
				insertedCapcodeLinks++;
				//console.log('Inserted a link between the message ('+p2000MessageId+') and the capcode ('+capcodeId+') to the database');
				
				finishProcessingMessage(docSequence, connection);
				
			});
			
		} else {
			finishProcessingMessage(docSequence, connection);
		}
		
	});
}

// Insert if not exists; capcodes
var lastKnownDocSequence;
var alreadyWritingToFile = false;
function insertMessageMetaData(doc, p2000MessageId, docSequence, connection){

	// Create capcode and the link between the message and the capcode
	var numCapcodes = doc.capcodes.length;
	//console.log('Num of capcodes: ' + numCapcodes );
	for (var i=0; i < numCapcodes; i++){
	
		var processingCapcode = doc.capcodes[i];
		//console.log('Checking capcode: ' + processingCapcode);
		
		// Skip the 'Brandweer landelijk' capcode which is almost set for every P2000 message
		if(processingCapcode == '2029568'){
			//console.log( 'Skipped adding capcode: ' + processingCapcode );
			continue;
		}
		
		// Check if the capcode already exists in the capcode table
		var query = connection.query('SELECT id FROM capcodes WHERE capcode="'+processingCapcode+'" LIMIT 1', function(err, rows, fields) {
		
			if (err){
			
				finishProcessingMessage(docSequence, connection);
					
				if(!debug) return;
				
				console.log( 'Error during the capcode check' ); 
				console.log( err ); 
				
				return; // throw err;
			}
			
			//console.log('RESULT: ');
			//console.log(rows);
			//console.log(fields);
			
			// Capcode doesn't exist yet in the database; add it
			if(typeof rows[0] == "undefined" || rows[0] == null){
			
				//console.log('Inserting capcode ' + processingCapcode + ' into the capcode database');
				
				// Insert the capcode to the capcode table - if they don't already exist
				var query = connection.query('INSERT INTO capcodes (capcode) VALUES ("'+processingCapcode+'")', function(err, result) {
				
					if (err){
						
						finishProcessingMessage(docSequence, connection);
				
						if(!debug) return;
						
						console.log( 'Error inserting capcode '+processingCapcode+' to the capcode database' ); 
						console.log( err );
						
						return; // throw err;
					}
					
					var capcodeId = result.insertId;
					
					insertedCapcodes++;
					//console.log("Inserted the capcode in the database with ID: " + capcodeId);
					
					// Insert the link
					insertMessageCapcodeLink(p2000MessageId, capcodeId, connection, docSequence);
					
				});
			
			} else {
				
				// Insert the link
				insertMessageCapcodeLink(p2000MessageId, rows[0].id, connection, docSequence);
				
			}
			
		});
		
	}

}

var prevProgress = 0;
var progress = 0;
function finishProcessingMessage(docId, connection){

	// Release the connection
	if(typeof connection != "undefined" && connection != null){
		//console.log('RELEASED CONNECTION');
		connection.release();
	}
	
	// Did all messages reach the end of the full processing line?
	listCounter++;
	
	// Bump the last known highest ID
	if(docId > lastKnownHighestId){
		lastKnownHighestId = docId;
	}
	
	// Calculate the progress
	progress = (listCounter/listLength).toFixed(2);
	
	// Updat the progressbar if the progress changed at least 1 percent
	if(progress != prevProgress){
		cli.progress( progress );
	}
	prevProgress = progress;
	
	// If this is true; we're done processing and can start to sync a next batch of messages
	if(listCounter == listLength){
		
		cli.ok('['+timeString()+'] Finished processing '+listLength+' items!');
		console.log('Messages added: ' + insertedMessages + ', inserted capcodes ' + insertedCapcodes + ', inserted capcode links '+ insertedCapcodeLinks);
		console.log('Skipped test messages in this batch: ' + numOfTestMsgs);
		numOfTestMsgs = 0;
		
		console.log('Last ID set to: ' + docId);
		console.log('Last known highest ID: ' + lastKnownHighestId);
		if(lastKnownHighestId > docId){
			console.log('Overwriting the last ID with the last known highest ID');
			docId = lastKnownHighestId;
		}
		fs.writeFileSync( stateFile , docId );
		
		// Give the script some time (1250-2500 ms) to finish the async tasks (and write the last ID back to the state file)
		//var randomDelay = 100; // 1000 + Math.floor((Math.random()*600)+1);
		
		var stopSeqId = 1654000;
		if(docId < stopSeqId){
		console.log('Restarting the sync process'); // in '+randomDelay+'ms');
			//setTimeout(function () {
				//console.log('Restarting now');
				runSync();
			//}, randomDelay)
		} else {
			console.log('['+timeString()+'] We\'re done; stopped the syncprocess at sequence: ' + stopSeqId);
			process.exit();
		}
	}
	
	/*
	// Write the sequence ID of this message as 'last inserted message'
	if(!alreadyWritingToFile && typeof docSequence != "undefied" && docSequence != ""){
	//if(typeof docSequence != "undefied" && docSequence != ""){
		//alreadyWritingToFile = true;
		
		var currentLastId = parseInt( fs.readFileSync( stateFile, 'UTF-8' ) ); 
		if(docSequence > currentLastId || currentLastId == 0 || currentLastId == null || currentLastId == ""){
			fs.writeFileSync( stateFile , docSequence );
			console.log('Last ID set to: ' + docSequence);
		} else {
			//console.log('Last ID was NOT set to: ' + docSequence);
		}
		
		//alreadyWritingToFile = false;
	} else {
		console.log('Skip writing to statefile, already in use');
	}
	//*/

}

function timeString(){
	var dateObj = new Date();
	return dateObj.getHours() + ":" + dateObj.getMinutes() + ":" + dateObj.getSeconds();
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