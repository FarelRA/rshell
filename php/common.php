<?php

require_once __DIR__ . '/native.php';

const VERSION = 1;
const MAX_PACKET_SIZE = 65535;
const CHUNK_SIZE = 1024;
const INTRO_EVERY = 1.0;
const RETRANSMIT_EVERY = 0.2;
const KEEPALIVE_EVERY = 5.0;
const SESSION_TIMEOUT = 20.0;
const PUNCH_EVERY = 0.5;
const PUNCH_TIMEOUT = 20.0;
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
        log_event('error', 'php', 'runtime', 'startup_failed', 'failed to bind udp socket', ['listen' => $listen, 'error' => "{$errstr} ({$errno})"]);
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
    $msg = json_decode($data, true, 512, JSON_THROW_ON_ERROR);
    if (!is_array($msg)) {
        throw new RuntimeException('json object required');
    }
    return $msg;
}

function encode_data(string $data): string {
    return base64_encode($data);
}

function decode_data(string $data): string {
    if ($data === '') {
        return '';
    }
    $decoded = base64_decode($data, true);
    if ($decoded === false || base64_encode($decoded) !== $data) {
        throw new RuntimeException('invalid base64');
    }
    return $decoded;
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

function message_has_version(array $msg): bool {
    return ($msg['v'] ?? null) === VERSION;
}

function positive_int(mixed $value, int $fallback = 0): int {
    if (!is_numeric($value)) {
        return $fallback;
    }
    $parsed = (int) $value;
    return $parsed > 0 ? $parsed : $fallback;
}

function log_event(string $level, string $impl, string $role, string $event, string $message, array $fields = []): void {
    $parts = [
        'ts=' . json_encode(gmdate('Y-m-d\TH:i:s') . sprintf('.%06dZ', (int) ((microtime(true) % 1) * 1000000))),
        'level=' . json_encode($level),
        'impl=' . json_encode($impl),
        'role=' . json_encode($role),
        'event=' . json_encode($event),
        'msg=' . json_encode($message),
    ];
    foreach ($fields as $key => $value) {
        if ($value !== null) {
            $parts[] = $key . '=' . json_encode($value, JSON_UNESCAPED_SLASHES);
        }
    }
    fwrite(STDERR, implode(' ', $parts) . PHP_EOL);
}

function reliable_sender_create(string $stream): array {
    return ['stream' => $stream, 'next_seq' => 0, 'pending' => []];
}

function reliable_sender_push(array &$sender, string $sessionId, string $token, string $chunk): array {
    $sender['next_seq']++;
    $encoded = encode_data($chunk);
    $sender['pending'][$sender['next_seq']] = ['data' => $encoded, 'sent_at' => microtime(true)];
    return ['type' => 'data', 'session' => $sessionId, 'token' => $token, 'stream' => $sender['stream'], 'seq' => $sender['next_seq'], 'data' => $encoded];
}

function reliable_sender_ack(array &$sender, int $ack): void {
    foreach (array_keys($sender['pending']) as $seq) {
        if ((int) $seq <= $ack) {
            unset($sender['pending'][$seq]);
        }
    }
}

function reliable_sender_retransmit(array &$sender, string $sessionId, string $token): array {
    $now = microtime(true);
    $messages = [];
    foreach ($sender['pending'] as $seq => &$chunk) {
        if ($now - $chunk['sent_at'] < RETRANSMIT_EVERY) {
            continue;
        }
        $chunk['sent_at'] = $now;
        $messages[] = ['type' => 'data', 'session' => $sessionId, 'token' => $token, 'stream' => $sender['stream'], 'seq' => (int) $seq, 'data' => $chunk['data']];
    }
    unset($chunk);
    return $messages;
}

function reliable_receiver_create(): array {
    return ['expected' => 1, 'buffer' => []];
}

function reliable_receiver_accept(array &$receiver, int $seq, string $chunk): array {
    if ($seq <= 0) {
        return [[], $receiver['expected'] - 1];
    }
    if ($seq < $receiver['expected']) {
        return [[], $receiver['expected'] - 1];
    }
    if (!isset($receiver['buffer'][$seq])) {
        $receiver['buffer'][$seq] = $chunk;
    }
    $chunks = [];
    while (isset($receiver['buffer'][$receiver['expected']])) {
        $chunks[] = $receiver['buffer'][$receiver['expected']];
        unset($receiver['buffer'][$receiver['expected']]);
        $receiver['expected']++;
    }
    return [$chunks, $receiver['expected'] - 1];
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
