<?php

declare(strict_types=1);

use Spiral\Goridge\StreamRelay;
use Spiral\RoadRunner\Worker;
use Spiral\RoadRunner;
use Nyholm\Psr7\Factory\Psr17Factory;
use Nyholm\Psr7\Response;
use Nyholm\Psr7\Stream;

require __DIR__ . "/vendor/autoload.php";

$worker = new Spiral\RoadRunner\Worker(new StreamRelay(\STDIN, \STDOUT));
$psr7 = new RoadRunner\Http\PSR7Worker(
    $worker,
    new Psr17Factory(),
    new Psr17Factory(),
    new Psr17Factory()
);

$psr7->chunk_size = 10 * 10 * 1024;
$fp = "fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

while ($worker->waitPayload()) {
    try {
        $resp = (new Response())->withBody(Stream::create($fp));
        $psr7->respond($resp);
    } catch (\Throwable $e) {
        $psr7->getWorker()->error((string)$e);
    }
}

?>