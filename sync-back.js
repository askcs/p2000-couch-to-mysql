/*
 *
 * The sync-back.js script is used for syncing batches of data from the couchdb to the mysql database
 * 
 * It will request a couple of hundred documents from the couchdb at a time. 
 * The script will process these messages and keep track of where it started and finished.
 * On the next run it will continue with where it was finished. To offload the couchdb server, the
 * script should be executed on a different client. This client can then transfer the (finished)
 * MySQL dump to the couchdb server; where MySQL is also installed and running
 *
 * What does this script do?
 * 
 *	- Get the messages
 *	- Loop over these messages to process them: Check if they (message/capcode/message-capcode-link) 
 *		exists and insert if necessary
 *	- Collect stats during processing: excluded testmessages, inserted messages, capcodes and 
 *		message-capcode-links
 * 	- Exclude testmessages and one specific capcode which is added to each groupmessage (and useless)
 *	- On the CLI it will show the stats after each batch has finished and during the executing it 
 *		will show progressbars
 *	- The script keeps track of it's state (last processed ID) in the state/last-mass-synced-msg-seq 
 *		file
 *
 * Development / Production
 *
 * Different ini files can be created in the main directory to run the code on different machines 
 * when they have different credentials for the couchdb and/or mysql. Start the nodejs script with 
 * a parameter to select the correct credentials file. E.g: node sync-back.js dev (for using the 
 * dev login credentials). Note: You probably don't want to run this 'bulk sync' on the couchdb
 * server itself. Only run the other realtime sync script on that machine: app.js
 */

//console.log('Manual sync with MySQL databases in progress');
//process.exit();
 
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
	connectionLimit		: 2
});

var stateFile = 'state/last-mass-synced-msg-seq';
var listLength = 0;
var listCounter = 0;
var numOfTestMsgs = 0;
var brokenMessages = 0;
var lastKnownHighestId = 0;

var insertedMessages = 0;
var messagesAlreadyExisted = 0;
var insertedCapcodes = 0;
var insertedCapcodeLinks = 0;
var capcodeLinksAlreadyExisted = 0;

var seqIds = [];
var docIds = [];

function runSync(){

	// Keep track of the application state
	var lastSavedMessage = parseInt( fs.readFileSync( stateFile, 'UTF-8') ); // Blocking call to make sure we have it before starting to listen for changes
	var maxMessagesPerBatch = 567; // To get some changing numbers which are easier to recognize

	var toId = ( parseInt(lastSavedMessage) + parseInt(maxMessagesPerBatch) );
	console.log('['+timeString()+'] Getting a batch of changes as a list, getting items: ' + lastSavedMessage + ' - ' + toId + '. (' + maxMessagesPerBatch + ')');

	// Listen to changes in the CouchDB
	var feed = db.changes({ since: lastSavedMessage, limit: maxMessagesPerBatch, include_docs: true }, function (err, list) {
	
		//console.log(list);
		
		if (err){
		
			// Release the connection
			//connection.release();
			
			if(!debug) return;
			
			console.log( 'Error getting a batch of changes: ' ); 
			console.log( err ); 
			return; //throw err;
		}
		
		listLength = list.length;
		listCounter = 0;
	
		if(listLength == 0){
			console.log('Stopped the sync process, nothing to sync');
			exitNicely();
		}
	
		numOfTestMsgs = 0;
		brokenMessages = 0;
		
		insertedMessages = 0;
		processedMessages = 0;
		messagesAlreadyExisted = 0;
		insertedCapcodes = 0;
		insertedCapcodeLinks = 0;
		capcodeLinksAlreadyExisted = 0;
		
		console.log('Actual size of the retrieved batch: ' + listLength );
		
		// Reset tracking arrays
		seqIds = [];
		docIds = [];
		
		var loopNum = 0;
		list.forEach(function (change) {
			
			//console.log('[C] ' + change.seq + " - " + change.id);
			//console.log(">>" + change.changes[0].rev);
			//console.log(change);
			
			//console.log('Loop: ' + ++loopNum);

			// Shortcut
			var doc = change.doc;
			var docSequence = change.seq;
			
			// Keep track of this ID to see if it ever finished
			seqIds.push(docSequence);
			docIds.push(change.id);
			
			// Skip testmessages
			if(typeof doc.message == "string"){
				var loweredMsg = doc.message.toLowerCase();
				if(loweredMsg == 'test' || 
					loweredMsg == 'testen' ||
					loweredMsg.indexOf('testpage') >= 0 || 
					loweredMsg.indexOf('test ') >= 0 || 
					loweredMsg.indexOf('c2000 test ') >= 0 || 
					loweredMsg.indexOf('test voor p2000') >= 0 || 
					loweredMsg.indexOf('test voor c2000') >= 0 || 
					loweredMsg.indexOf('pagertest') >= 0 || 
					loweredMsg.indexOf('testbericht') >= 0 || 
					loweredMsg.indexOf('testoproep') >= 0 || 
					loweredMsg.indexOf('testmelding') >= 0 || 
					loweredMsg.indexOf('testalarmering') >= 0 || 
					loweredMsg.indexOf('testalarm') >= 0 || 
					loweredMsg.indexOf(' een test') >= 0 || 
					loweredMsg.indexOf('testen ') >= 0 || 
					loweredMsg.indexOf('-test') >= 0 || 
					loweredMsg.indexOf('test,') >= 0 ||
					loweredMsg.indexOf('test.') >= 0 ||
					loweredMsg.indexOf('test!') >= 0 ||
					loweredMsg.indexOf('testje') >= 0 ||
						(loweredMsg.indexOf('test') && loweredMsg.length < 17) // Also exclude short messages that contain 'test' somewhere
					){
					
					numOfTestMsgs++;
					
					//console.log('test msg');
					
					finishProcessingMessage(docSequence, null);
					return;
				}
			} else {
				if(debug) console.log('broken msg');
				brokenMessages++;
				// For some reason this happenend wherearound sequence 870.584: "Object true has no method 'toLowerCase'"
				finishProcessingMessage(docSequence, null);
				return;
			}

			//console.log('[P] ' + doc.message + ''+"\n"+'<' + change.seq + ':' + change.id + ':' + change.changes[0].rev + '>');

			// Get a MySQL connection (for parallel queries)
			pool.getConnection(function(err, connection){

				if (err){
				
					console.log('err get connection');
					
					brokenMessages++; // Since processing is stopped from here on anyway; mark it as broken
					finishProcessingMessage(docSequence, connection);
					
					if(!debug) return;
					
					console.log( 'Error getting a connection' ); 
					console.log( err ); 
					
					
					console.log('Stopped the sync script because of a MySQL error: ' + err );
					process.exit();
					
					return; //throw err;
				}
				
				//console.log('We got a MySQL connection');
				
				// Check if the p2000 message already exists
				var query = connection.query('SELECT id FROM messages WHERE timestamp="'+doc.timestamp+'" AND message="'+doc.message+'"', function(err, rows, fields) {
						
					if (err){
					
						console.log('err select msg');
						
						//messagesAlreadyExisted++; // NOTE: Assume it's the duplicate error
						brokenMessages++;
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
							
								console.log('err insert msg');
								//messagesAlreadyExisted++; // NOTE: Assume it's the duplicate error
								brokenMessages++;
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
						
						messagesAlreadyExisted++;
						
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
function insertMessageCapcodeLink(p2000MessageId, capcodeId, connection, docSequence, isNum, endNum, capcodeInsertedCallback){

	//if(debug) console.log('Item: ' + isNum + ', ' + endNum);
	// Check if the link between the message and the capcode already exists
	var query = connection.query('SELECT * FROM message_has_capcode WHERE message_id="'+p2000MessageId+'" AND capcode_id="'+capcodeId+'" LIMIT 1', function(err, rows, fields) {
	
		if (err){
			
			capcodeInsertedCallback('broken', docSequence, connection);
				
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
					
					if(debug) console.log('err insert mes cap link');
					brokenMessages++; // Since processing is stopped from here on anyway; mark it as broken
					finishProcessingMessage(docSequence, connection);
						
					if(!debug) return;
					
					console.log( 'Error inserting the message-capcode link' ); 
					console.log( err ); 
					return; // throw err;
				}
			
				insertedCapcodeLinks++;
				//console.log('Inserted a link between the message ('+p2000MessageId+') and the capcode ('+capcodeId+') to the database');
				
				// Is this the last capcode in a row of multiple capcodes?
				// Use actual callbacks since previous capcodes may not have finished yet (and thus lose their connection because of this finish)
				capcodeInsertedCallback('inserted', docSequence, connection);
				
			});
			
		} else {
		
			capcodeLinksAlreadyExisted++;
			
			// Is this the last capcode in a row of multiple capcodes?
			// Use actual callbacks since previous capcodes may not have finished yet (and thus lose their connection because of this finish)
			capcodeInsertedCallback('inserted', docSequence, connection);
			
		}
		
	});
}

// Insert if not exists; capcodes
var lastKnownDocSequence;
var alreadyWritingToFile = false;
function insertMessageMetaData(doc, p2000MessageId, docSequence, connection){

	// Check for the capcodes
	if(typeof doc.capcodes.length != "number"){
		console.log('The length of the capcodes list is not set since the value is not an error, but instead: ' + typeof doc.capcodes.length);
		brokenMessages++; // Since processing is stopped from here on anyway; mark it as broken
		finishProcessingMessage(docSequence, connection);
		return;
	}
	
	// Create capcode and the link between the message and the capcode
	var numCapcodes = doc.capcodes.length;
	var loopCounter = 0;
	//console.log('Num of capcodes: ' + numCapcodes );
	
	// Capcodes inserted callback
	var capcodesProcessedCounter = 0;
	function capcodeInsertedCallback(type, docSequence, connection){
		capcodesProcessedCounter++;
		
		//console.log('['+capcodesProcessedCounter+'/'+numCapcodes+'] Inserted callback called for p2000 msg ' + p2000MessageId);
		
		if(capcodesProcessedCounter == numCapcodes){
			if(type == 'broken'){
				//console.log('Broken msg');
				brokenMessages++;
			} else {
				processedMessages++;
			}
			
			//console.log('finished processing doc seq: ' + docSequence);
			finishProcessingMessage(docSequence, connection);
		}
		
	}
	
	// Do we have capcodes for this message at all?
	if(numCapcodes == 0){
		brokenMessages++; // Since processing is stopped from here on anyway; mark it as broken
		finishProcessingMessage(docSequence, connection);
		return;
	}
	
	for (var i=0; i < numCapcodes; i++){
	
		var processingCapcode = doc.capcodes[i];
		//var item = i;
		//console.log('Checking capcode: ' + processingCapcode);
		
		// Skip the 'Brandweer landelijk' capcode which is almost set for every P2000 message
		if(processingCapcode == '2029568'){
			//console.log( 'Skipped adding capcode: ' + processingCapcode );
			loopCounter++;
			
			// NOTE: On 06-05-2014 it happened that a message was only send to this capcode; and thus the message processing was never 'finished':
			if(debug) console.log('Only capcode for this msg is a capcode we ignored; finishing message');
			capcodeInsertedCallback('broken', docSequence, connection);
			
			continue;
		}
		
		// Check if the capcode already exists in the capcode table
		var query = connection.query('SELECT id FROM capcodes WHERE capcode="'+processingCapcode+'" LIMIT 1', function(err, rows, fields) {
		
			if (err){
			
				brokenMessages++; // Since processing is stopped from here on anyway; mark it as broken
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
						
						brokenMessages++; // Since processing is stopped from here on anyway; mark it as broken
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
					loopCounter++;
					insertMessageCapcodeLink(p2000MessageId, capcodeId, connection, docSequence, loopCounter, numCapcodes, capcodeInsertedCallback);
					
				});
			
			} else {
				
				// Insert the link
				loopCounter++;
				insertMessageCapcodeLink(p2000MessageId, rows[0].id, connection, docSequence, loopCounter, numCapcodes, capcodeInsertedCallback);
				
			}
			
		});
		
	}

}

var prevProgress = 0;
var progress = 0;
var lastDoc = '';
function finishProcessingMessage(docId, connection){
	
	// Did all messages reach the end of the full processing line?
	listCounter++;
	
	// Check if none of the messages (in the loop) called this method multiple times in a row
	if(lastDoc == docId){
		console.log('oops');
	}
	lastDoc = docId;
	
	// Release the connection
	if(typeof connection != "undefined" && connection != null){
		//console.log('RELEASED CONNECTION');
		connection.release();
	}
	
	// Remove the docId from the tracking array
	var index = seqIds.indexOf(docId);
	if (index > -1) {
		seqIds.splice(index, 1);
		docIds.splice(index, 1);
	} else {
		if(debug) console.log('Specified docId not in the tracking list: ' + docId);
	}
	
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
		
		var totalProcessed = processedMessages + numOfTestMsgs + brokenMessages;
		
		cli.ok('['+timeString()+'] Finished processing '+listLength+' items!');
		console.log('- - - - - - - - - - - - - - - -');
		console.log('Messages added: ' + insertedMessages + ' ['+processedMessages+' inserted processed, '+totalProcessed+' processed in total]');
		console.log('> ' + messagesAlreadyExisted+' already existed, '+brokenMessages+' broken, '+numOfTestMsgs+' tests. Inserted capcodes: ' + insertedCapcodes + ', inserted capcode links: '+ insertedCapcodeLinks + ' ('+capcodeLinksAlreadyExisted+' already existed)');
		console.log('- - - - - - - - - - - - - - - -');
		//console.log('Skipped test messages in this batch: ' + numOfTestMsgs);
		
		
		if(totalProcessed != listLength){
			console.log('['+timeString()+'] The total amount of messages ('+listLength+') did not match the amount of proccessed messages ('+totalProcessed+'). Stopping the script to prevent that the highest ID is written to the state.');
			exitNicely();
		}
		
		console.log('Last ID set to: ' + docId);
		console.log('Last known highest ID: ' + lastKnownHighestId);
		if(lastKnownHighestId > docId){
			console.log('Overwriting the last ID with the last known highest ID');
			docId = lastKnownHighestId;
		}
		fs.writeFileSync( stateFile , docId );
		
		// Give the script some time (1250-2500 ms) to finish the async tasks (and write the last ID back to the state file)
		//var randomDelay = 100; // 1000 + Math.floor((Math.random()*600)+1);
		
		var stopSeqId = 1659400;
		//if(docId < stopSeqId){
		console.log('Restarting the sync process'); // in '+randomDelay+'ms');
			//setTimeout(function () {
				//console.log('Restarting now');
				runSync();
			//}, randomDelay)
		//} else {
		//	console.log('['+timeString()+'] We\'re done; stopped the syncprocess at sequence: ' + stopSeqId);
		//	process.exit();
		//}
	} else {
		//if(debug) console.log(listCounter + '/' + listLength);
		
		
		/* // DEBUG PURPOSES (IF THE SYNC SCRIPT IS STUCK ON A MESSAGE WITH NO CLEAR REASON
		// Show 'leftovers' during the 5 last finish-es
		if(listCounter > (listLength - 2)){
			console.log(seqIds);
			console.log(docIds);
			db.get(docIds, function (err, res) {
				//console.log(err);
				console.log(res);
				console.log(res[0].doc.capcodes);
			});
		}
		*/
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

function exitNicely(){
	// Nothing nice about this exit, but...
	// if necessary a delay could be build in here so that the script (async tasks) get some time to finish before the process is killed
	process.exit();
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
