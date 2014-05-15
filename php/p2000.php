<?php

// p2000.php

// JSON node
$msgs = array();

// Capcode(s)
if(!isset($_GET['code'])){
	echo json_encode( array('error' => 'You did not specific the \'code\' parameter.') );
	exit;
}


// JSONP callback
if(!isset($_GET['callback'])){
	echo json_encode( array('error' => 'You did not specific the \'callback\' parameter.') );
	exit;
}

// Limit the amount of returned messages. Default to 6
$limit = 6;
if(isset($_GET['limit'])){
	$limit = htmlentities($_GET['limit'], ENT_QUOTES);
}

// Limit the limit
$maxLimit = 100;
if($limit > $maxLimit){
	$limit = $maxLimit;
	$msgs['note'] = 'Request automatically limited to '.$maxLimit.' P2000 messages';
}

// Limit the amount of returned capcodes per message. Default off
$capcodeLimit = 0;
if(isset($_GET['capcodelimit'])){
	$capcodeLimit = htmlentities($_GET['capcodelimit'], ENT_QUOTES);
}

// DB Connectiohn
require_once('config.php');


// Capcodes GET style (csv / array)
if( is_array( $_REQUEST['code'] ) ){
	$capcodesArray = $_REQUEST['code'];
} else {
	$capcodes = htmlentities($_GET['code'], ENT_QUOTES);
	$capcodesArray = explode(',', $capcodes);
}

// Get the DB id for the capcode that's requested
$capcodeIdsArray = array();
$dbid = $link->query('SELECT * FROM capcodes WHERE capcode IN ("'.implode('","', $capcodesArray).'")');
while($capcodeIdRow = mysqli_fetch_assoc($dbid)) {
	$capcodeIdsArray[] = $capcodeIdRow['id'];
}

// Query the latest P2000 messages for a specific capcode
$result = $link->query('SELECT * FROM messages WHERE id IN (SELECT message_id FROM message_has_capcode WHERE capcode_id IN ("'.implode('","', $capcodeIdsArray).'") ORDER BY message_id DESC) ORDER BY id DESC LIMIT '.$limit);


// Display the data
while($row = mysqli_fetch_assoc($result)) {
	$capcodes = $link->query('SELECT * FROM capcodes WHERE id IN (
			SELECT capcode_id
			FROM  `message_has_capcode` 
			WHERE  `message_id` = '.$row['id'].'
		)
	');
	
	//echo '<pre>'.print_r($row,1).'</pre>';
	
	$msg = array();
	$msg['body'] = $row['message'];
	$timstamp = ($row['timestamp']/1000);
	$msg['day'] = date('d-m-Y', $timstamp);
	$msg['time'] = date('H:i:s', $timstamp);
	
	$msg['msgCode'] = '';
	$capcodesArray = array();
	while($capcode = mysqli_fetch_assoc($capcodes)) {
		
		// Capcodes list limiter
		if($capcodeLimit != 0 && count($capcodesArray) >= $capcodeLimit){
			$capcodesArray[] = '... (+'.(mysqli_num_rows($capcodes)-$capcodeLimit).')';
			break;
		}
		
		$capcodesArray[] = $capcode['capcode'];
	}
	
	$msg['msgCode'] .= implode(', ', $capcodesArray);
	
	$msgs[] = $msg;
}

//print_r($msgs);

$msgs['name'] = 'response';

if(isset($_GET['callback'])){
	echo  $_GET['callback']."(".json_encode($msgs, JSON_FORCE_OBJECT).");";
} else {
	echo  "foobar(".json_encode($msgs, JSON_FORCE_OBJECT).");";
}
?>