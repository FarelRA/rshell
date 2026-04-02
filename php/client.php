<?php
require __DIR__ . '/common.php';

$opts = getopt('', ['rendezvous::', 'service::', 'listen::']);
$rendezvous = $opts['rendezvous'] ?? '127.0.0.1:4000';
$service = $opts['service'] ?? 'demo-shell';
$listen = $opts['listen'] ?? '0.0.0.0:0';

[$rdvHost, $rdvPort] = parse_host_port($rendezvous);
$socket = create_udp_socket($listen);
function send_connect_request($socket, string $service, array $rdv): void {
    send_json($socket, $rdv, ['type' => 'connect_request', 'service' => $service, 'meta' => ['impl' => 'php-client']]);
}

send_connect_request($socket, $service, [$rdvHost, $rdvPort]);

$peer = null;
$sessionId = null;
$token = null;
$active = false;
$closed = false;
$lastSeen = microtime(true);
$lastPunch = 0.0;
$lastKeepalive = 0.0;
$punchUntil = 0.0;
$introUntil = microtime(true) + PUNCH_TIMEOUT;
$lastIntro = 0.0;
$pendingResize = false;
$stdinSender = reliable_sender_create('stdin');
$ptyReceiver = reliable_receiver_create();

$tty = native_open_tty();
$inputStream = $tty['stream'] ?? STDIN;
$ttyFd = $tty['fd'] ?? null;
$rawState = $ttyFd !== null ? native_terminal_make_raw($ttyFd) : null;
stream_set_blocking($inputStream, false);

function restore_terminal(?int $ttyFd, ?string $state): void {
    if ($ttyFd !== null) {
        native_terminal_restore($ttyFd, $state);
    }
}

function close_client($socket, ?array $peer, ?string $sessionId, ?string $token, string $reason, ?int $ttyFd, ?string $rawState, int $exitCode = 0, bool $notify = true): void {
    if ($notify && $peer && $sessionId && $token) {
        @send_json($socket, $peer, ['type' => 'close', 'session' => $sessionId, 'token' => $token, 'reason' => $reason]);
    }
    restore_terminal($ttyFd, $rawState);
    exit($exitCode);
}

function hello_message(?string $sessionId, ?string $token, ?int $ttyFd): array {
    return ['type' => 'hello', 'session' => $sessionId, 'token' => $token, 'role' => 'client'] + terminal_info('rshell-php-client', $ttyFd);
}

function resize_message(?string $sessionId, ?string $token, ?int $ttyFd): array {
    return ['type' => 'resize', 'session' => $sessionId, 'token' => $token] + terminal_info('rshell-php-client', $ttyFd);
}

$GLOBALS['rshell_sigstp'] = function() use ($ttyFd, &$rawState) {
    restore_terminal($ttyFd, $rawState);
    pcntl_signal(SIGTSTP, SIG_DFL);
    posix_kill(posix_getpid(), SIGTSTP);
    pcntl_signal(SIGTSTP, $GLOBALS['rshell_sigstp']);
};

pcntl_async_signals(true);
pcntl_signal(SIGINT, function() use ($socket, &$peer, &$sessionId, &$token, $ttyFd, $rawState) {
    if ($ttyFd !== null && $rawState !== null) {
        return;
    }
    close_client($socket, $peer, $sessionId, $token, 'signal', $ttyFd, $rawState, 0, true);
});
pcntl_signal(SIGTERM, function() use ($socket, &$peer, &$sessionId, &$token, $ttyFd, $rawState) {
    close_client($socket, $peer, $sessionId, $token, 'signal', $ttyFd, $rawState, 0, true);
});
pcntl_signal(SIGWINCH, function() {
    $GLOBALS['pendingResize'] = true;
});
pcntl_signal(SIGTSTP, $GLOBALS['rshell_sigstp']);
pcntl_signal(SIGCONT, function() use ($ttyFd, &$rawState) {
    if ($ttyFd !== null) {
        $rawState = native_terminal_make_raw($ttyFd) ?? $rawState;
        $GLOBALS['pendingResize'] = true;
    }
});

while (true) {
    $read = [$socket];
    if ($peer && $sessionId && $token) {
        $read[] = $inputStream;
    }
    $write = null;
    $except = null;
    @stream_select($read, $write, $except, 0, 500000);

    $now = microtime(true);
    if (!$peer && $now > $introUntil) {
        log_event('error', 'php', 'client', 'intro_timeout', 'timed out waiting for rendezvous intro', ['service' => $service]);
        restore_terminal($ttyFd, $rawState);
        exit(1);
    }
    if (!$peer && $now - $lastIntro >= INTRO_EVERY) {
        @send_connect_request($socket, $service, [$rdvHost, $rdvPort]);
        $lastIntro = $now;
    }
    if (in_array($socket, $read, true)) {
        $packet = recv_packet($socket);
        if ($packet === null) {
            $packet = null;
        }
    } else {
        $packet = null;
    }
    if ($packet !== null) {
        try {
            $msg = recv_json($packet['data']);
        } catch (Throwable $e) {
            $msg = null;
        }
        if ($msg && message_has_version($msg)) {
            $type = $msg['type'] ?? '';
            if ($type === 'error') {
                log_event('error', 'php', 'client', 'server_error', 'rendezvous returned an error', ['code' => $msg['code'] ?? 'error', 'detail' => $msg['message'] ?? '']);
                restore_terminal($ttyFd, $rawState);
                exit(1);
            }
            if ($type === 'connect_intro') {
                $peer = parse_host_port($msg['peer_addr']);
                $sessionId = $msg['session'];
                $token = $msg['token'];
                $punchUntil = $now + PUNCH_TIMEOUT;
                log_event('info', 'php', 'client', 'session_intro', 'received session intro', ['service' => $service, 'session' => $sessionId, 'peer' => $msg['peer_addr']]);
                $pendingResize = true;
                continue;
            }
            if ($sessionId && $token && ($msg['session'] ?? '') === $sessionId && ($msg['token'] ?? '') === $token) {
                $lastSeen = $now;
                if ($type === 'punch') {
                    @send_json($socket, $peer, hello_message($sessionId, $token, $ttyFd));
                } elseif ($type === 'hello' || $type === 'hello_ack') {
                    $active = true;
                    @send_json($socket, $peer, ['type' => 'hello_ack', 'session' => $sessionId, 'token' => $token]);
                } elseif ($type === 'data' && ($msg['stream'] ?? '') === 'pty') {
                    try {
                        [$chunks, $ack] = reliable_receiver_accept($ptyReceiver, positive_int($msg['seq'] ?? null, 0), decode_data($msg['data'] ?? ''));
                        foreach ($chunks as $chunk) {
                            fwrite(STDOUT, $chunk);
                        }
                        @send_json($socket, $peer, ['type' => 'ack', 'session' => $sessionId, 'token' => $token, 'stream' => 'pty', 'ack' => $ack]);
                    } catch (Throwable $e) {
                        log_event('error', 'php', 'client', 'invalid_payload', 'dropped invalid stdout payload', ['session' => $sessionId]);
                    }
                } elseif ($type === 'ack' && ($msg['stream'] ?? '') === 'stdin') {
                    reliable_sender_ack($stdinSender, positive_int($msg['ack'] ?? null, 0));
                } elseif ($type === 'keepalive') {
                    // Receiving a keepalive is sufficient to refresh $lastSeen.
                } elseif ($type === 'close') {
                    log_event('info', 'php', 'client', 'session_closed', 'session closed by peer', ['session' => $sessionId, 'reason' => $msg['reason'] ?? 'peer_close']);
                    close_client($socket, $peer, $sessionId, $token, $msg['reason'] ?? 'peer_close', $ttyFd, $rawState, 0, false);
                }
            }
        }
    }

    if ($peer && $sessionId && $token && !$active && $now <= $punchUntil && $now - $lastPunch >= PUNCH_EVERY) {
        @send_json($socket, $peer, ['type' => 'punch', 'session' => $sessionId, 'token' => $token]);
        @send_json($socket, $peer, hello_message($sessionId, $token, $ttyFd));
        $lastPunch = $now;
    }
    if ($peer && $sessionId && $token && $now - $lastKeepalive >= KEEPALIVE_EVERY) {
        if ($now - $lastSeen > SESSION_TIMEOUT) {
            log_event('error', 'php', 'client', 'session_timeout', 'session timed out', ['session' => $sessionId]);
            close_client($socket, $peer, $sessionId, $token, 'timeout', $ttyFd, $rawState, 1, true);
        }
        @send_json($socket, $peer, ['type' => 'keepalive', 'session' => $sessionId, 'token' => $token]);
        $lastKeepalive = $now;
    }
    if ($peer && $sessionId && $token) {
        foreach (reliable_sender_retransmit($stdinSender, $sessionId, $token) as $msg) {
            @send_json($socket, $peer, $msg);
        }
    }

    if ($pendingResize && $peer && $sessionId && $token) {
        $pendingResize = false;
        @send_json($socket, $peer, resize_message($sessionId, $token, $ttyFd));
    }

    if ($peer && $sessionId && $token && in_array($inputStream, $read, true)) {
        $chunk = @fread($inputStream, CHUNK_SIZE);
        if ($chunk !== false && $chunk !== '') {
            @send_json($socket, $peer, reliable_sender_push($stdinSender, $sessionId, $token, $chunk));
        } elseif (feof($inputStream)) {
            close_client($socket, $peer, $sessionId, $token, 'stdin_eof', $ttyFd, $rawState, 0, true);
        }
    }
}
