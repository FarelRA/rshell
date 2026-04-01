<?php

const VERSION = 1;
const MAX_PACKET_SIZE = 65535;
const KEEPALIVE_EVERY = 5.0;
const SESSION_TIMEOUT = 20.0;
const PUNCH_EVERY = 0.5;
const PUNCH_TIMEOUT = 15.0;
const REGISTER_EVERY = 10.0;

function parse_host_port(string $value): array {
    $parts = explode(':', $value);
    $port = (int) array_pop($parts);
    $host = implode(':', $parts);
    return [$host, $port];
}

function send_json($socket, array $addr, array $msg): void {
    $msg['v'] = VERSION;
    $msg['ts'] = (int) round(microtime(true) * 1000);
    $payload = json_encode($msg, JSON_UNESCAPED_SLASHES);
    socket_sendto($socket, $payload, strlen($payload), 0, $addr[0], $addr[1]);
}

function recv_json(string $data): array {
    return json_decode($data, true, 512, JSON_THROW_ON_ERROR);
}

function encode_data(string $data): string {
    return base64_encode($data);
}

function decode_data(string $data): string {
    return $data === '' ? '' : base64_decode($data, true);
}
