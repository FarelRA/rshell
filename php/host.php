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

function close_session(&$sessions, $socket, string $id, string $reason = 'close'): void {
    if (!isset($sessions[$id]) || $sessions[$id]['closed']) {
        return;
    }
    $sessions[$id]['closed'] = true;
    @send_json($socket, $sessions[$id]['peer'], ['type' => 'close', 'session' => $id, 'token' => $sessions[$id]['token'], 'reason' => $reason]);
    if (!empty($sessions[$id]['master_stream'])) {
        @fclose($sessions[$id]['master_stream']);
    }
    if (isset($sessions[$id]['master_fd'])) {
        native_close_fd((int) $sessions[$id]['master_fd']);
    }
    if (!empty($sessions[$id]['child_pid'])) {
        native_kill_pid((int) $sessions[$id]['child_pid']);
    }
}

function capture_terminal(&$session, array $msg): void {
    if (!empty($msg['term'])) $session['term'] = $msg['term'];
    if (!empty($msg['cols'])) $session['cols'] = (int) $msg['cols'];
    if (!empty($msg['rows'])) $session['rows'] = (int) $msg['rows'];
}

function apply_resize(&$sessions, string $id): void {
    if (empty($sessions[$id]['master_fd'])) {
        return;
    }
    native_set_winsize((int) $sessions[$id]['master_fd'], (int) ($sessions[$id]['cols'] ?? 80), (int) ($sessions[$id]['rows'] ?? 24));
}

function start_shell(&$sessions, $socket, string $id, string $shell): void {
    if (!isset($sessions[$id]) || $sessions[$id]['active'] || $sessions[$id]['closed']) {
        return;
    }
    $pty = native_openpty((int) ($sessions[$id]['cols'] ?? 80), (int) ($sessions[$id]['rows'] ?? 24));
    if ($pty === null) {
        return;
    }
    $pid = pcntl_fork();
    if ($pid === -1) {
        native_close_fd($pty['master_fd']);
        native_close_fd($pty['slave_fd']);
        return;
    }
    if ($pid === 0) {
        native_close_fd($pty['master_fd']);
        if (!native_login_tty($pty['slave_fd'])) {
            exit(1);
        }
        putenv('TERM=' . ($sessions[$id]['term'] ?? 'xterm-256color'));
        putenv('TERM_PROGRAM=rshell-php-host');
        putenv('TERMINAL=rshell-php-host');
        pcntl_exec($shell, ['-i']);
        exit(1);
    }

    native_close_fd($pty['slave_fd']);
    $masterStream = @fopen('php://fd/' . $pty['master_fd'], 'r+');
    if ($masterStream === false) {
        native_kill_pid($pid);
        native_close_fd($pty['master_fd']);
        return;
    }
    stream_set_blocking($masterStream, false);

    $sessions[$id]['child_pid'] = $pid;
    $sessions[$id]['master_fd'] = $pty['master_fd'];
    $sessions[$id]['master_stream'] = $masterStream;
    $sessions[$id]['active'] = true;
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
                    if (!empty($sessions[$id]['master_stream'])) {
                        @fwrite($sessions[$id]['master_stream'], decode_data($msg['data'] ?? ''));
                        @fflush($sessions[$id]['master_stream']);
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
        if (!empty($sessions[$id]['master_stream'])) {
            $chunk = @fread($sessions[$id]['master_stream'], 4096);
            if ($chunk !== false && $chunk !== '') {
                @send_json($socket, $sessions[$id]['peer'], ['type' => 'stdout', 'session' => $id, 'token' => $sessions[$id]['token'], 'data' => encode_data($chunk)]);
            }
        }
        if (!empty($sessions[$id]['child_pid']) && native_waitpid((int) $sessions[$id]['child_pid'])) {
            close_session($sessions, $socket, $id, 'shell_exit');
            unset($sessions[$id]);
        }
    }

    usleep(20000);
}

foreach (array_keys($sessions) as $id) {
    close_session($sessions, $socket, $id, 'shutdown');
}
