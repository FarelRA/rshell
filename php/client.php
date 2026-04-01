<?php
require __DIR__ . '/common.php';

$opts = getopt('', ['rendezvous::', 'service::', 'listen::']);
$rendezvous = $opts['rendezvous'] ?? '127.0.0.1:4000';
$service = $opts['service'] ?? 'demo-shell';
$listen = $opts['listen'] ?? '0.0.0.0:0';

[$listenHost, $listenPort] = parse_host_port($listen);
[$rdvHost, $rdvPort] = parse_host_port($rendezvous);

$socket = socket_create(AF_INET, SOCK_DGRAM, SOL_UDP);
socket_bind($socket, $listenHost, $listenPort);
socket_set_nonblock($socket);

send_json($socket, [$rdvHost, $rdvPort], ['type' => 'connect_request', 'service' => $service, 'meta' => ['impl' => 'php-client']]);

$peer = null;
$sessionId = null;
$token = null;
$active = false;
$closed = false;
$lastSeen = microtime(true);
$lastPunch = 0.0;
$lastKeepalive = 0.0;
$punchUntil = 0.0;

$sttyState = null;
if (function_exists('shell_exec') && posix_isatty(STDIN)) {
    $sttyState = trim((string) shell_exec('stty -g < /dev/tty'));
    shell_exec('stty raw -echo < /dev/tty');
}

function restore_terminal(?string $state): void {
    if ($state !== null) {
        shell_exec('stty ' . escapeshellarg($state) . ' < /dev/tty');
    }
}

function close_client($socket, ?array $peer, ?string $sessionId, ?string $token, string $reason, ?string $sttyState): void {
    if ($peer && $sessionId && $token) {
        @send_json($socket, $peer, ['type' => 'close', 'session' => $sessionId, 'token' => $token, 'reason' => $reason]);
    }
    restore_terminal($sttyState);
    exit(0);
}

pcntl_async_signals(true);
pcntl_signal(SIGINT, function() use ($socket, &$peer, &$sessionId, &$token, $sttyState) {
    close_client($socket, $peer, $sessionId, $token, 'signal', $sttyState);
});
pcntl_signal(SIGTERM, function() use ($socket, &$peer, &$sessionId, &$token, $sttyState) {
    close_client($socket, $peer, $sessionId, $token, 'signal', $sttyState);
});

stream_set_blocking(STDIN, false);

while (true) {
    $now = microtime(true);
    $buf = '';
    $from = '';
    $port = 0;
    $read = @socket_recvfrom($socket, $buf, MAX_PACKET_SIZE, 0, $from, $port);
    if ($read !== false && $read > 0) {
        try {
            $msg = recv_json($buf);
        } catch (Throwable $e) {
            $msg = null;
        }
        if ($msg) {
            $type = $msg['type'] ?? '';
            if ($type === 'error') {
                restore_terminal($sttyState);
                fwrite(STDERR, ($msg['code'] ?? 'error') . ': ' . ($msg['message'] ?? '') . PHP_EOL);
                exit(1);
            }
            if ($type === 'connect_intro') {
                $peer = parse_host_port($msg['peer_addr']);
                $sessionId = $msg['session'];
                $token = $msg['token'];
                $punchUntil = $now + PUNCH_TIMEOUT;
                if (posix_isatty(STDIN)) {
                    $size = trim((string) shell_exec('stty size < /dev/tty'));
                    if ($size !== '') {
                        [$rows, $cols] = array_map('intval', preg_split('/\s+/', $size));
                        @send_json($socket, $peer, ['type' => 'resize', 'session' => $sessionId, 'token' => $token, 'rows' => $rows, 'cols' => $cols]);
                    }
                }
                continue;
            }
            if ($sessionId && $token && ($msg['session'] ?? '') === $sessionId && ($msg['token'] ?? '') === $token) {
                $lastSeen = $now;
                if ($type === 'punch') {
                    @send_json($socket, $peer, ['type' => 'hello', 'session' => $sessionId, 'token' => $token, 'role' => 'client']);
                } elseif ($type === 'hello' || $type === 'hello_ack') {
                    $active = true;
                    @send_json($socket, $peer, ['type' => 'hello_ack', 'session' => $sessionId, 'token' => $token]);
                } elseif ($type === 'stdout') {
                    fwrite(STDOUT, decode_data($msg['data'] ?? ''));
                } elseif ($type === 'keepalive') {
                    @send_json($socket, $peer, ['type' => 'keepalive', 'session' => $sessionId, 'token' => $token]);
                } elseif ($type === 'close') {
                    close_client($socket, $peer, $sessionId, $token, $msg['reason'] ?? 'peer_close', $sttyState);
                }
            }
        }
    }

    if ($peer && $sessionId && $token && !$active && $now <= $punchUntil && $now - $lastPunch >= PUNCH_EVERY) {
        @send_json($socket, $peer, ['type' => 'punch', 'session' => $sessionId, 'token' => $token]);
        @send_json($socket, $peer, ['type' => 'hello', 'session' => $sessionId, 'token' => $token, 'role' => 'client']);
        $lastPunch = $now;
    }
    if ($peer && $sessionId && $token && $now - $lastKeepalive >= KEEPALIVE_EVERY) {
        if ($now - $lastSeen > SESSION_TIMEOUT) {
            fwrite(STDERR, "session timeout" . PHP_EOL);
            close_client($socket, $peer, $sessionId, $token, 'timeout', $sttyState);
        }
        @send_json($socket, $peer, ['type' => 'keepalive', 'session' => $sessionId, 'token' => $token]);
        $lastKeepalive = $now;
    }

    if ($peer && $sessionId && $token) {
        $chunk = fread(STDIN, 4096);
        if ($chunk !== false && $chunk !== '') {
            @send_json($socket, $peer, ['type' => 'stdin', 'session' => $sessionId, 'token' => $token, 'data' => encode_data($chunk)]);
        }
    }

    usleep(50000);
}
