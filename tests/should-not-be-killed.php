<?php
declare(strict_types=1);

use Spiral\Goridge\StreamRelay;
use Spiral\RoadRunner\Worker as RoadRunner;

require __DIR__ . "/vendor/autoload.php";

$rr = new RoadRunner(new StreamRelay(\STDIN, \STDOUT));
$mem = '';
while($rr->waitPayload()){
        // Allocate some memory, this should be enough to exceed the `max_worker_memory` limit defined in
        // .rr.yaml
        $array = range(1, 10000000);
        fprintf(STDERR, 'memory allocated: ' . memory_get_usage());
        sleep(30);

        $rr->respond(new \Spiral\RoadRunner\Payload("alive"));
}
