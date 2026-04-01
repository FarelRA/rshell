<?php

const RSHELL_O_RDWR = 2;
const RSHELL_O_NOCTTY = 256;
const RSHELL_TCSANOW = 0;
const RSHELL_TIOCGWINSZ = 0x5413;
const RSHELL_TIOCSWINSZ = 0x5414;
const RSHELL_BRKINT = 2;
const RSHELL_ICRNL = 256;
const RSHELL_INPCK = 16;
const RSHELL_ISTRIP = 32;
const RSHELL_IXON = 1024;
const RSHELL_OPOST = 1;
const RSHELL_CS8 = 48;
const RSHELL_ECHO = 8;
const RSHELL_ICANON = 2;
const RSHELL_IEXTEN = 32768;
const RSHELL_ISIG = 1;
const RSHELL_VTIME = 5;
const RSHELL_VMIN = 6;
const RSHELL_SIGKILL = 9;
const RSHELL_WNOHANG = 1;

function native_available(): bool {
    return class_exists('FFI');
}

function libc_ffi(): ?FFI {
    static $ffi = false;
    if ($ffi !== false) {
        return $ffi;
    }
    if (!native_available()) {
        $ffi = null;
        return null;
    }
    $ffi = FFI::cdef(<<<'CDEF'
        struct termios {
            unsigned int c_iflag;
            unsigned int c_oflag;
            unsigned int c_cflag;
            unsigned int c_lflag;
            unsigned char c_line;
            unsigned char c_cc[32];
            unsigned int c_ispeed;
            unsigned int c_ospeed;
        };
        struct winsize {
            unsigned short ws_row;
            unsigned short ws_col;
            unsigned short ws_xpixel;
            unsigned short ws_ypixel;
        };
        int open(const char *pathname, int flags, ...);
        int close(int fd);
        int ioctl(int fd, unsigned long request, void *argp);
        int tcgetattr(int fd, struct termios *termios_p);
        int tcsetattr(int fd, int optional_actions, const struct termios *termios_p);
        int kill(int pid, int sig);
        int waitpid(int pid, int *wstatus, int options);
    CDEF, 'libc.so.6');
    return $ffi;
}

function libutil_ffi(): ?FFI {
    static $ffi = false;
    if ($ffi !== false) {
        return $ffi;
    }
    if (!native_available()) {
        $ffi = null;
        return null;
    }
    $ffi = FFI::cdef(<<<'CDEF'
        struct termios {
            unsigned int c_iflag;
            unsigned int c_oflag;
            unsigned int c_cflag;
            unsigned int c_lflag;
            unsigned char c_line;
            unsigned char c_cc[32];
            unsigned int c_ispeed;
            unsigned int c_ospeed;
        };
        struct winsize {
            unsigned short ws_row;
            unsigned short ws_col;
            unsigned short ws_xpixel;
            unsigned short ws_ypixel;
        };
        int openpty(int *amaster, int *aslave, char *name, const struct termios *termp, const struct winsize *winp);
        int login_tty(int fd);
    CDEF, 'libutil.so.1');
    return $ffi;
}

function native_open_tty(): ?array {
    $libc = libc_ffi();
    if ($libc === null) {
        return null;
    }
    $fd = $libc->open('/dev/tty', RSHELL_O_RDWR | RSHELL_O_NOCTTY);
    if ($fd < 0) {
        return null;
    }
    $stream = @fopen("php://fd/{$fd}", 'r+');
    if ($stream === false) {
        $libc->close($fd);
        return null;
    }
    stream_set_blocking($stream, false);
    return ['fd' => $fd, 'stream' => $stream];
}

function native_get_winsize(int $fd): ?array {
    $libc = libc_ffi();
    if ($libc === null || $fd < 0) {
        return null;
    }
    $winsize = $libc->new('struct winsize');
    if ($libc->ioctl($fd, RSHELL_TIOCGWINSZ, FFI::addr($winsize)) !== 0) {
        return null;
    }
    return [(int) $winsize->ws_col, (int) $winsize->ws_row];
}

function native_set_winsize(int $fd, int $cols, int $rows): bool {
    $libc = libc_ffi();
    if ($libc === null || $fd < 0 || $cols <= 0 || $rows <= 0) {
        return false;
    }
    $winsize = $libc->new('struct winsize');
    $winsize->ws_col = $cols;
    $winsize->ws_row = $rows;
    return $libc->ioctl($fd, RSHELL_TIOCSWINSZ, FFI::addr($winsize)) === 0;
}

function native_terminal_make_raw(int $fd): ?string {
    $libc = libc_ffi();
    if ($libc === null || $fd < 0) {
        return null;
    }
    $term = $libc->new('struct termios');
    if ($libc->tcgetattr($fd, FFI::addr($term)) !== 0) {
        return null;
    }
    $snapshot = FFI::string(FFI::cast('char *', FFI::addr($term)), FFI::sizeof($term));
    $term->c_iflag &= ~(RSHELL_BRKINT | RSHELL_ICRNL | RSHELL_INPCK | RSHELL_ISTRIP | RSHELL_IXON);
    $term->c_oflag &= ~RSHELL_OPOST;
    $term->c_cflag |= RSHELL_CS8;
    $term->c_lflag &= ~(RSHELL_ECHO | RSHELL_ICANON | RSHELL_IEXTEN | RSHELL_ISIG);
    $term->c_cc[RSHELL_VMIN] = 1;
    $term->c_cc[RSHELL_VTIME] = 0;
    if ($libc->tcsetattr($fd, RSHELL_TCSANOW, FFI::addr($term)) !== 0) {
        return null;
    }
    return $snapshot;
}

function native_terminal_restore(int $fd, ?string $snapshot): void {
    $libc = libc_ffi();
    if ($libc === null || $fd < 0 || $snapshot === null) {
        return;
    }
    $term = $libc->new('struct termios');
    FFI::memcpy(FFI::addr($term), $snapshot, strlen($snapshot));
    $libc->tcsetattr($fd, RSHELL_TCSANOW, FFI::addr($term));
}

function native_openpty(int $cols, int $rows): ?array {
    $libutil = libutil_ffi();
    if ($libutil === null) {
        return null;
    }
    $master = FFI::new('int[1]');
    $slave = FFI::new('int[1]');
    $win = $libutil->new('struct winsize');
    $win->ws_col = max(1, $cols);
    $win->ws_row = max(1, $rows);
    if ($libutil->openpty($master, $slave, null, null, FFI::addr($win)) !== 0) {
        return null;
    }
    return ['master_fd' => (int) $master[0], 'slave_fd' => (int) $slave[0]];
}

function native_login_tty(int $fd): bool {
    $libutil = libutil_ffi();
    if ($libutil === null || $fd < 0) {
        return false;
    }
    return $libutil->login_tty($fd) === 0;
}

function native_kill_pid(int $pid): void {
    $libc = libc_ffi();
    if ($libc !== null && $pid > 0) {
        $libc->kill($pid, RSHELL_SIGKILL);
    }
}

function native_waitpid(int $pid): bool {
    $libc = libc_ffi();
    if ($libc === null || $pid <= 0) {
        return false;
    }
    $status = FFI::new('int[1]');
    return $libc->waitpid($pid, $status, RSHELL_WNOHANG) > 0;
}

function native_close_fd(int $fd): void {
    $libc = libc_ffi();
    if ($libc !== null && $fd >= 0) {
        $libc->close($fd);
    }
}
