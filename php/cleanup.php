<?php
echo 'The script will delete a max of 500 test messages at a time. Keep refreshing this page untill you see nothing below this line.';
echo '<br />';
echo '[Currently only shows what can be deleted; doesnt delete messages. Uncomment the continue statement in the loop to do that]';
echo '<hr />';

require_once('config.php');

// Query all 'real' testmessages
//$result = $link->query('SELECT COUNT(*) AS num, message FROM messages WHERE message LIKE \'%test%\' AND message NOT LIKE \'%testraat%\' AND LENGTH(message) < 30 GROUP BY message ORDER BY num DESC');
//$result = $link->query('SELECT COUNT(*) AS num FROM messages WHERE message LIKE \'%test%\' AND message NOT LIKE \'%testraat%\' AND LENGTH(message) < 30 ');
$result = $link->query('SELECT * FROM messages WHERE message LIKE \'%test%\' AND message NOT LIKE \'%testraat%\' AND LENGTH(message) < 30 LIMIT 500');

// Display the data

while($row = mysqli_fetch_assoc($result)) {

	echo 'Delete message ('.$row['message'].') and message-capcode relation(s) for message ID: ' . $row['id'];
	echo '<br />';
	
	// Only show what we want to delete; don't delete it just yet [add ?delete=1 to actually delete the test messages]
	if(empty($_GET['delete']) || !isset($_GET['delete'])){
		continue; // Comment this line to actually delete the messages and message-capcode-links\
	}
	
	$capcodesDelete = $link->query('DELETE FROM messages WHERE  `id` = '.$row['id'].' LIMIT 1'); // Only delete 1 message at a time

	// Deleted rows
	echo 'Deleted messages: ' . mysqli_affected_rows($link);
	
	// Delete the relation with the message (keep the capcode in the capcode table though; it may be used elsewhere or in the future)
	$capcodesDelete = $link->query('DELETE FROM message_has_capcode WHERE  `message_id` = '.$row['id'].' LIMIT 50'); // Limit 50; just to be sure in case something goes wrong
	
	// Deleted rows
	echo '<br />';
	echo 'Deleted relations: ' . mysqli_affected_rows($link);
	
	//echo '<pre>'.print_r($capcodesDelete,1).'</pre>';
	
	//echo $row['message'].' - '.$row['num'];
	//echo $row['num'];
	echo '<br />';
} 
?>