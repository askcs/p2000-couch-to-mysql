/*
 * Runs the sync-back.js script every minute
 * 
 */
 
var scriptExecStart = 'node'; // 'forever start';
var scriptExecStop = ''; // 'forever stop';
var scriptCmd = 'sync-back.js';
var scriptParams = 'prod';

// TODO: Implement something that makes sure that the previously started sync-back is already finished before starting a new one:
// If that happens it could be stopped and started again, or maybe better; keep it going and don't start a new sync-back process

//var startHour = 3; // 1:00 (2 hour offset)
//var stopHour = 6; // 4:00 (2 hour offset)

var schedule = require('node-schedule');
var exec = require('child_process').exec;
var child, childchild, child2;

var rule = new schedule.RecurrenceRule();
//rule.hour = startHour;
//rule.minute = 0; // parseInt(process.argv[2])
rule.second = 5;

var j = schedule.scheduleJob(rule, function(){
    console.log('Start the sync script:');
	
	// Start the script
	child = exec(scriptExecStart + " " + scriptCmd + " " + scriptParams, function (error, stdout, stderr) {
		console.log(stdout);
		if (error !== null) {
			console.log('Error: ' + error);
		}
		
		// Show running scripts
		console.log('Running forever scripts:');
		childchild = exec("forever list", function (error, stdout, stderr) {
			console.log(stdout);
		});
	});

});

// Stop script
if(scriptExecStop != ''){

	var rule2 = new schedule.RecurrenceRule();
	//rule2.hour = stopHour;
	//rule2.minute = 0; // parseInt(process.argv[3])
	rule2.second = 55;

	var j2 = schedule.scheduleJob(rule2, function(){
		console.log('Stop the sync script:');
		
		// Stop the script
		child2 = exec(scriptExecStop + " " + scriptCmd, function (error, stdout, stderr) {
			console.log(stdout);
			if (error !== null) {
				console.log('Error: ' + error);
			}
		});

	});

}