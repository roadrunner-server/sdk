<?php

declare(strict_types=1);

use Spiral\Goridge\StreamRelay;
use Spiral\RoadRunner\Worker as RoadRunner;

require __DIR__ . "/vendor/autoload.php";

$rr = new RoadRunner(new StreamRelay(\STDIN, \STDOUT));
$mem = '';
while($rr->waitPayload()){
    $rr->respond(new \Spiral\RoadRunner\Payload(""));
}
