<?php
require __DIR__ . '/common.php';

if (function_exists('cli_set_process_title')) {
    @cli_set_process_title('rshell-php-host');
}

$opts = getopt('', ['rendezvous::', 'service::', 'listen::', 'shell::']);
$rendezvous = $opts['rendezvous'] ?? '127.0.0.1:4000';
$service = $opts['service'] ?? 'demo-shell';
$listen = $opts['listen'] ?? '0.0.0.0:0';
$shell = $opts['shell'] ?? (getenv('SHELL') ?: '/bin/sh');

[$rdvHost, $rdvPort] = parse_host_port($rendezvous);
$socket = create_udp_socket($listen);

$sessions = [];
$lastRegister = 0.0;
$lastKeepalive = 0.0;
$running = true;

function quote_sh(string $value): string {
    return "'" . str_replace("'", "'\\''", $value) . "'";
}

function shell_bootstrap(array $session, string $shell): string {
    $term = $session['term'] ?? 'xterm-256color';
    $cols = (int) ($session['cols'] ?? 80);
    $rows = (int) ($session['rows'] ?? 24);
    return "export TERM=" . quote_sh($term) . " TERM_PROGRAM='rshell-php-host' TERMINAL='rshell-php-host'; stty cols {$cols} rows {$rows} 2>/dev/null || true; exec " . quote_sh($shell) . " -i";
}

function close_session(&$sessions, $socket, string $id, string $reason = 'close'): void {
    if (!isset($sessions[$id]) || $sessions[$id]['closed']) {
        return;
    }
    $sessions[$id]['closed'] = true;
    @send_json($socket, $sessions[$id]['peer'], ['type' => 'close', 'session' => $id, 'token' => $sessions[$id]['token'], 'reason' => $reason]);
    foreach (($sessions[$id]['pipes'] ?? []) as $pipe) {
        @fclose($pipe);
    }
    if (isset($sessions[$id]['proc']) && is_resource($sessions[$id]['proc'])) {
        @proc_terminate($sessions[$id]['proc'], 9);
    }
}

function start_shell(&$sessions, $socket, string $id, string $shell): void {
    if (!isset($sessions[$id]) || $sessions[$id]['active'] || $sessions[$id]['closed']) {
        return;
    }
    $desc = [
        0 => ['pipe', 'r'],
        1 => ['pipe', 'w'],
        2 => ['pipe', 'w'],
    ];
    $proc = proc_open(['script', '-qefc', shell_bootstrap($sessions[$id], $shell), '/dev/null'], $desc, $pipes);
    if (!is_resource($proc)) {
        return;
    }
    foreach ($pipes as $pipe) {
        stream_set_blocking($pipe, false);
    }
    $sessions[$id]['proc'] = $proc;
    $sessions[$id]['pipes'] = $pipes;
    $sessions[$id]['active'] = true;
}

function apply_resize(&$sessions, string $id): void {
    if (empty($sessions[$id]['pipes'][0])) {
        return;
    }
    $cols = (int) ($sessions[$id]['cols'] ?? 80);
    $rows = (int) ($sessions[$id]['rows'] ?? 24);
    @fwrite($sessions[$id]['pipes'][0], "stty cols {$cols} rows {$rows} 2>/dev/null\n");
    @fflush($sessions[$id]['pipes'][0]);
}

function capture_terminal(&$session, array $msg): void {
    if (!empty($msg['term'])) $session['term'] = $msg['term'];
    if (!empty($msg['cols'])) $session['cols'] = (int) $msg['cols'];
    if (!empty($msg['rows'])) $session['rows'] = (int) $msg['rows'];
}

function send_register($socket, string $service, array $rdv): void {
    send_json($socket, $rdv, ['type' => 'register', 'service' => $service, 'meta' => ['impl' => 'php-host']]);
}

pcntl_async_signals(true);
pcntl_signal(SIGINT, function() use (&$running) { $running = false; });
pcntl_signal(SIGTERM, function() use (&$running) { $running = false; });

while ($running) {
    $now = microtime(true);
    if ($now - $lastRegister >= REGISTER_EVERY) {
        send_register($socket, $service, [$rdvHost, $rdvPort]);
        $lastRegister = $now;
    }
    if ($now - $lastKeepalive >= KEEPALIVE_EVERY) {
        @send_json($socket, [$rdvHost, $rdvPort], ['type' => 'keepalive', 'service' => $service]);
        foreach (array_keys($sessions) as $id) {
            if ($sessions[$id]['closed'] || $now - $sessions[$id]['last_seen'] > SESSION_TIMEOUT) {
                close_session($sessions, $socket, $id, 'timeout');
                unset($sessions[$id]);
                continue;
            }
            if ($sessions[$id]['active']) {
                @send_json($socket, $sessions[$id]['peer'], ['type' => 'keepalive', 'session' => $id, 'token' => $sessions[$id]['token']]);
            }
        }
        $lastKeepalive = $now;
    }

    $packet = recv_packet($socket);
    if ($packet !== null) {
        try {
            $msg = recv_json($packet['data']);
        } catch (Throwable $e) {
            $msg = null;
        }
        if ($msg) {
            $type = $msg['type'] ?? '';
            if ($type === 'register_ok') {
                fwrite(STDERR, "registered {$service} public=" . ($msg['public_addr'] ?? '') . PHP_EOL);
            } elseif ($type === 'connect_intro') {
                $sessions[$msg['session']] = [
                    'token' => $msg['token'],
                    'peer' => parse_host_port($msg['peer_addr']),
                    'active' => false,
                    'closed' => false,
                    'last_seen' => $now,
                    'punch_until' => $now + PUNCH_TIMEOUT,
                    'term' => 'xterm-256color',
                    'cols' => 80,
                    'rows' => 24,
                ];
            } elseif (!empty($msg['session']) && isset($sessions[$msg['session']]) && $sessions[$msg['session']]['token'] === ($msg['token'] ?? '')) {
                $id = $msg['session'];
                $sessions[$id]['last_seen'] = $now;
                if ($type === 'punch') {
                    @send_json($socket, $sessions[$id]['peer'], ['type' => 'hello', 'session' => $id, 'token' => $sessions[$id]['token'], 'role' => 'host']);
                } elseif ($type === 'hello') {
                    capture_terminal($sessions[$id], $msg);
                    start_shell($sessions, $socket, $id, $shell);
                    @send_json($socket, $sessions[$id]['peer'], ['type' => 'hello_ack', 'session' => $id, 'token' => $sessions[$id]['token']]);
                } elseif ($type === 'hello_ack') {
                    start_shell($sessions, $socket, $id, $shell);
                } elseif ($type === 'stdin') {
                    start_shell($sessions, $socket, $id, $shell);
                    if (isset($sessions[$id]['pipes'][0])) {
                        @fwrite($sessions[$id]['pipes'][0], decode_data($msg['data'] ?? ''));
                        @fflush($sessions[$id]['pipes'][0]);
                    }
                } elseif ($type === 'resize') {
                    capture_terminal($sessions[$id], $msg);
                    apply_resize($sessions, $id);
                } elseif ($type === 'keepalive') {
                    @send_json($socket, $sessions[$id]['peer'], ['type' => 'keepalive', 'session' => $id, 'token' => $sessions[$id]['token']]);
                } elseif ($type === 'close') {
                    close_session($sessions, $socket, $id, $msg['reason'] ?? 'peer_close');
                    unset($sessions[$id]);
                }
            }
        }
    }

    foreach (array_keys($sessions) as $id) {
        if (!$sessions[$id]['closed'] && !$sessions[$id]['active'] && $now < $sessions[$id]['punch_until']) {
            @send_json($socket, $sessions[$id]['peer'], ['type' => 'punch', 'session' => $id, 'token' => $sessions[$id]['token']]);
            @send_json($socket, $sessions[$id]['peer'], ['type' => 'hello', 'session' => $id, 'token' => $sessions[$id]['token'], 'role' => 'host']);
        }
        foreach ([1, 2] as $pipeIndex) {
            if (!empty($sessions[$id]['pipes'][$pipeIndex])) {
                $chunk = @fread($sessions[$id]['pipes'][$pipeIndex], 4096);
                if ($chunk !== false && $chunk !== '') {
                    @send_json($socket, $sessions[$id]['peer'], ['type' => 'stdout', 'session' => $id, 'token' => $sessions[$id]['token'], 'data' => encode_data($chunk)]);
                }
            }
        }
        if (!empty($sessions[$id]['proc']) && is_resource($sessions[$id]['proc'])) {
            $status = proc_get_status($sessions[$id]['proc']);
            if (!$status['running']) {
                close_session($sessions, $socket, $id, 'shell_exit');
                unset($sessions[$id]);
            }
        }
    }

    usleep(20000);
}

foreach (array_keys($sessions) as $id) {
    close_session($sessions, $socket, $id, 'shutdown');
}
