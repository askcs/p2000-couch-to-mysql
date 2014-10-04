<?php
require_once('config.php');

// Inject sql WHERE part for the ID filtering
$sqlWhere = '';
$fromId = (isset($_GET['id'])) ? $_GET['id'] : null;
if(isset($fromId) && $fromId != null){
	$sqlWhere = 'WHERE id <= '.$fromId.' ';
}

// Inject search query filtering
$q = (isset($_GET['q'])) ? $_GET['q'] : null;
if(isset($q) && $q != null){
	$sqlWhere = 'WHERE message LIKE \'%'.$q.'%\' ';
}

// Query the latest P2000 messages
$result = $link->query('SELECT * FROM messages '.$sqlWhere.'ORDER BY id DESC LIMIT 15');

// Display the data

while($row = mysqli_fetch_assoc($result)) {
	$capcodes = $link->query('SELECT * FROM capcodes WHERE id IN (
			SELECT capcode_id
			FROM  `message_has_capcode` 
			WHERE  `message_id` = '.$row['id'].'
		)
	');
	
	//echo '<pre>'.print_r($row,1).'</pre>';
	
	echo date('H:i:s d-m-Y', ($row['timestamp']/1000)).' ['.$row['id'].'] - '.$row['message'];
	echo '<ul>';
	while($capcode = mysqli_fetch_assoc($capcodes)) {
		echo '<li>'.$capcode['capcode'].': '.$capcode['name'].' ('.$capcode['service'].') / '.$capcode['region'].', '.$capcode['city'].'</li>';
	}
	echo '</ul>';
	echo '<hr />';
} 
?>
