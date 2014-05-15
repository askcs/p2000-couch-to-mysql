<?php
// Default timezone
date_default_timezone_set('Europe/Amsterdam');

// Connection
$link = mysqli_connect('localhost', 'username', 'password', 'database') or die('Error ' . mysqli_error($link));
?>