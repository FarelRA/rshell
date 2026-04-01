<?php

require_once __DIR__ . '/native.php';

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

function create_udp_socket(string $listen): mixed {
    [$host, $port] = parse_host_port($listen);
    $socket = @stream_socket_server("udp://{$host}:{$port}", $errno, $errstr, STREAM_SERVER_BIND);
    if ($socket === false) {
        fwrite(STDERR, "failed to bind UDP socket: {$errstr} ({$errno})" . PHP_EOL);
        exit(1);
    }
    stream_set_blocking($socket, false);
    return $socket;
}

function send_json($socket, array $addr, array $msg): void {
    $msg['v'] = VERSION;
    $msg['ts'] = (int) round(microtime(true) * 1000);
    $payload = json_encode($msg, JSON_UNESCAPED_SLASHES);
    @stream_socket_sendto($socket, $payload, 0, "{$addr[0]}:{$addr[1]}");
}

function recv_packet($socket): ?array {
    $peer = null;
    $data = @stream_socket_recvfrom($socket, MAX_PACKET_SIZE, 0, $peer);
    if ($data === false || $data === '') {
        return null;
    }
    return ['data' => $data, 'peer' => $peer];
}

function recv_json(string $data): array {
    return json_decode($data, true, 512, JSON_THROW_ON_ERROR);
}

function encode_data(string $data): string {
    return base64_encode($data);
}

function decode_data(string $data): string {
    return $data === '' ? '' : (base64_decode($data, true) ?: '');
}

function terminal_info(string $fallback, ?int $fd = null): array {
    [$cols, $rows] = terminal_size($fd);
    return [
        'term' => getenv('TERM') ?: 'xterm-256color',
        'term_program' => getenv('TERM_PROGRAM') ?: $fallback,
        'terminal' => getenv('TERMINAL') ?: $fallback,
        'cols' => $cols,
        'rows' => $rows,
    ];
}

function terminal_size(?int $fd = null): array {
    if ($fd !== null) {
        $native = native_get_winsize($fd);
        if ($native !== null) {
            return $native;
        }
    }
    $size = trim((string) @shell_exec('stty size < /dev/tty 2>/dev/null'));
    if ($size !== '') {
        [$rows, $cols] = array_map('intval', preg_split('/\s+/', $size));
        return [$cols, $rows];
    }
    return [0, 0];
}
