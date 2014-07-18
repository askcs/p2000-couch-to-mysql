<?php
require_once('config.php');

// Query
$result = $link->query('SELECT * FROM messages ORDER BY id DESC LIMIT 1');

$x = $result->mysqli_fetch_row();

print_r( $x );

?>