<?php
require_once('config.php');

$capcodes = htmlentities($_GET['c'], ENT_QUOTES);
$capcodesArray = explode(',', $capcodes);

// Get the DB id for the capcode that's requested
$capcodeIdsArray = array();
$dbid = $link->query('SELECT * FROM capcodes WHERE capcode IN ("'.implode('","', $capcodesArray).'")');
while($capcodeIdRow = mysqli_fetch_assoc($dbid)) {
	$capcodeIdsArray[] = $capcodeIdRow['id'];
}
echo 'Latest messages for capcode '.$capcodes.' with DB id\'s '.implode(', ', $capcodeIdsArray);
echo "\n\n";
echo '<hr />';

// Query the latest P2000 messages for a specific capcode
$result = $link->query('SELECT * FROM messages WHERE id IN (SELECT message_id FROM message_has_capcode WHERE capcode_id IN ("'.implode('","', $capcodeIdsArray).'") ORDER BY message_id DESC) ORDER BY id DESC LIMIT 50');

// Display the data
while($row = mysqli_fetch_assoc($result)) {
	$capcodes = $link->query('SELECT * FROM capcodes WHERE id IN (
			SELECT capcode_id
			FROM  `message_has_capcode` 
			WHERE  `message_id` = '.$row['id'].'
		)
	');
	
	//echo '<pre>'.print_r($row,1).'</pre>';
	
	echo $row['timestamp'].' - '.$row['message'];
	echo '<ul>';
	while($capcode = mysqli_fetch_assoc($capcodes)) {
		echo '<li>'.$capcode['capcode'].': '.$capcode['name'].' ('.$capcode['service'].') / '.$capcode['region'].', '.$capcode['city'].'</li>';
	}
	echo '</ul>';
	echo '<hr />';
} 
?>