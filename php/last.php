<?php
require_once('config.php');

// Query
$result = $link->query('SELECT * FROM messages ORDER BY id DESC LIMIT 1');

$last = $result->fetch_assoc();

if(isset($_GET['x'])){
	print_r($last);
}

echo (($last['timestamp'] - time()) / 1000);

?>