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
        $sessions[$id]['master_stream'] = null;
        $sessions[$id]['master_fd'] = null;
    } elseif (isset($sessions[$id]['master_fd'])) {
        native_close_fd((int) $sessions[$id]['master_fd']);
        $sessions[$id]['master_fd'] = null;
    }
    if (!empty($sessions[$id]['child_pid'])) {
        native_kill_pid((int) $sessions[$id]['child_pid']);
    }
}

function capture_terminal(&$session, array $msg): void {
    if (!empty($msg['term'])) $session['term'] = $msg['term'];
    $session['cols'] = positive_int($msg['cols'] ?? null, (int) ($session['cols'] ?? 80));
    $session['rows'] = positive_int($msg['rows'] ?? null, (int) ($session['rows'] ?? 24));
}

function apply_resize(&$sessions, string $id): void {
    if (empty($sessions[$id]['master_fd'])) {
        return;
    }
    native_set_winsize((int) $sessions[$id]['master_fd'], (int) ($sessions[$id]['cols'] ?? 80), (int) ($sessions[$id]['rows'] ?? 24));
}

function start_shell(&$sessions, $socket, string $id, string $shell): bool {
    if (!isset($sessions[$id]) || $sessions[$id]['active'] || $sessions[$id]['closed']) {
        return true;
    }
    $pty = native_openpty((int) ($sessions[$id]['cols'] ?? 80), (int) ($sessions[$id]['rows'] ?? 24));
    if ($pty === null) {
        close_session($sessions, $socket, $id, 'shell_start_failed');
        return false;
    }
    $pid = pcntl_fork();
    if ($pid === -1) {
        native_close_fd($pty['master_fd']);
        native_close_fd($pty['slave_fd']);
        close_session($sessions, $socket, $id, 'shell_start_failed');
        return false;
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
        close_session($sessions, $socket, $id, 'shell_start_failed');
        return false;
    }
    stream_set_blocking($masterStream, false);

    $sessions[$id]['child_pid'] = $pid;
    $sessions[$id]['master_fd'] = $pty['master_fd'];
    $sessions[$id]['master_stream'] = $masterStream;
    $sessions[$id]['active'] = true;
    return true;
}

function send_register($socket, string $service, array $rdv): void {
    send_json($socket, $rdv, ['type' => 'register', 'service' => $service, 'meta' => ['impl' => 'php-host']]);
}

pcntl_async_signals(true);
pcntl_signal(SIGINT, function() use (&$running) { $running = false; });
pcntl_signal(SIGTERM, function() use (&$running) { $running = false; });

while ($running) {
    $read = [$socket];
    foreach ($sessions as $session) {
        if (!empty($session['master_stream'])) {
            $read[] = $session['master_stream'];
        }
    }
    $write = null;
    $except = null;
    @stream_select($read, $write, $except, 0, 500000);

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
        if ($msg && message_has_version($msg)) {
            $type = $msg['type'] ?? '';
            if ($type === 'register_ok') {
                log_event('info', 'php', 'host', 'registered', 'registration confirmed', ['service' => $service, 'public_addr' => $msg['public_addr'] ?? '']);
            } elseif ($type === 'connect_intro') {
                $sessions[$msg['session']] = [
                    'token' => $msg['token'],
                    'peer' => parse_host_port($msg['peer_addr']),
                    'active' => false,
                    'closed' => false,
                    'last_seen' => $now,
                    'punch_until' => $now + PUNCH_TIMEOUT,
                    'last_punch' => 0.0,
                    'term' => 'xterm-256color',
                    'cols' => 80,
                    'rows' => 24,
                    'stdin_rx' => reliable_receiver_create(),
                    'pty_tx' => reliable_sender_create('pty'),
                ];
                log_event('info', 'php', 'host', 'session_intro', 'received session intro', ['service' => $service, 'session' => $msg['session'], 'peer' => $msg['peer_addr']]);
            } elseif (!empty($msg['session']) && isset($sessions[$msg['session']]) && $sessions[$msg['session']]['token'] === ($msg['token'] ?? '')) {
                $id = $msg['session'];
                $sessions[$id]['last_seen'] = $now;
                if ($type === 'punch') {
                    @send_json($socket, $sessions[$id]['peer'], ['type' => 'hello', 'session' => $id, 'token' => $sessions[$id]['token'], 'role' => 'host']);
                } elseif ($type === 'hello') {
                    capture_terminal($sessions[$id], $msg);
                    if (!start_shell($sessions, $socket, $id, $shell)) {
                        unset($sessions[$id]);
                        continue;
                    }
                    @send_json($socket, $sessions[$id]['peer'], ['type' => 'hello_ack', 'session' => $id, 'token' => $sessions[$id]['token']]);
                } elseif ($type === 'hello_ack') {
                    if (!start_shell($sessions, $socket, $id, $shell)) {
                        unset($sessions[$id]);
                    }
                } elseif ($type === 'data' && ($msg['stream'] ?? '') === 'stdin') {
                    if (!start_shell($sessions, $socket, $id, $shell)) {
                        unset($sessions[$id]);
                        continue;
                    }
                    if (!empty($sessions[$id]['master_stream'])) {
                        try {
                            [$chunks, $ack] = reliable_receiver_accept($sessions[$id]['stdin_rx'], positive_int($msg['seq'] ?? null, 0), decode_data($msg['data'] ?? ''));
                            foreach ($chunks as $chunk) {
                                @fwrite($sessions[$id]['master_stream'], $chunk);
                                @fflush($sessions[$id]['master_stream']);
                            }
                            @send_json($socket, $sessions[$id]['peer'], ['type' => 'ack', 'session' => $id, 'token' => $sessions[$id]['token'], 'stream' => 'stdin', 'ack' => $ack]);
                        } catch (Throwable $e) {
                            log_event('error', 'php', 'host', 'invalid_payload', 'dropped invalid stdin payload', ['session' => $id]);
                        }
                    }
                } elseif ($type === 'ack' && ($msg['stream'] ?? '') === 'pty') {
                    reliable_sender_ack($sessions[$id]['pty_tx'], positive_int($msg['ack'] ?? null, 0));
                } elseif ($type === 'resize') {
                    capture_terminal($sessions[$id], $msg);
                    apply_resize($sessions, $id);
                } elseif ($type === 'keepalive') {
                    // Receiving a keepalive is sufficient to refresh last_seen.
                } elseif ($type === 'close') {
                    close_session($sessions, $socket, $id, $msg['reason'] ?? 'peer_close');
                    unset($sessions[$id]);
                }
            }
        }
    }

    foreach (array_keys($sessions) as $id) {
        if (!$sessions[$id]['closed'] && !$sessions[$id]['active'] && $now < $sessions[$id]['punch_until'] && $now - ($sessions[$id]['last_punch'] ?? 0.0) >= PUNCH_EVERY) {
            @send_json($socket, $sessions[$id]['peer'], ['type' => 'punch', 'session' => $id, 'token' => $sessions[$id]['token']]);
            @send_json($socket, $sessions[$id]['peer'], ['type' => 'hello', 'session' => $id, 'token' => $sessions[$id]['token'], 'role' => 'host']);
            $sessions[$id]['last_punch'] = $now;
        }
        foreach (reliable_sender_retransmit($sessions[$id]['pty_tx'], $id, $sessions[$id]['token']) as $msg) {
            @send_json($socket, $sessions[$id]['peer'], $msg);
        }
        if (!empty($sessions[$id]['master_stream']) && in_array($sessions[$id]['master_stream'], $read, true)) {
            $chunk = @fread($sessions[$id]['master_stream'], CHUNK_SIZE);
            if ($chunk !== false && $chunk !== '') {
                @send_json($socket, $sessions[$id]['peer'], reliable_sender_push($sessions[$id]['pty_tx'], $id, $sessions[$id]['token'], $chunk));
            }
        }
        if (!empty($sessions[$id]['child_pid']) && native_waitpid((int) $sessions[$id]['child_pid'])) {
            close_session($sessions, $socket, $id, 'shell_exit');
            unset($sessions[$id]);
        }
    }
}

foreach (array_keys($sessions) as $id) {
    close_session($sessions, $socket, $id, 'shutdown');
}
