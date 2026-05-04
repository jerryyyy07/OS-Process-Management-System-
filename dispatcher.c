/* ==================================================================
 * dispatcher.c  –  Master process for the OS Pipeline project.
 *
 * Responsibilities:
 *   1. Parse command-line arguments.
 *   2. Create the logs/ sub-directory.
 *   3. Create the named FIFO (mkfifo).
 *   4. Create and initialise the POSIX shared-memory segment
 *      (shm_open + ftruncate + mmap).
 *   5. Create the named POSIX semaphore used by processor→reporter.
 *   6. Install signal handlers for SIGINT, SIGTERM, SIGCHLD, SIGUSR1.
 *   7. Fork-and-exec ingester, processor, reporter, redirecting each
 *      child's stdout/stderr to a per-process log file via dup2().
 *   8. Wait in a sigsuspend() loop (no busy-wait) until all children
 *      finish or a shutdown signal arrives.
 *   9. Clean up all IPC resources and print per-child summaries.
 *
 * Usage:
 *   dispatcher <input_dir> <output_dir> <num_threads> <queue_size>
 *              <fifo_path> <shm_name> <sem_name>
 * ================================================================== */

#include "common/common.h"

/* ------------------------------------------------------------------ *
 * Signal flags (set by handlers, inspected in the main loop)         *
 * ------------------------------------------------------------------ */
static volatile sig_atomic_t g_sigchld    = 0;
static volatile sig_atomic_t g_shutdown   = 0;
static volatile sig_atomic_t g_sigusr1    = 0;
static volatile sig_atomic_t g_exit_code  = EXIT_OK;

/* ------------------------------------------------------------------ *
 * IPC resource handles (needed by cleanup())                         *
 * ------------------------------------------------------------------ */
static char   g_fifo_path[MAX_PATH_LEN] = "";
static char   g_shm_name [MAX_PATH_LEN] = "";
static char   g_sem_name [MAX_PATH_LEN] = "";
static void  *g_shm_addr               = MAP_FAILED;
static size_t g_shm_size               = 0;

/* ------------------------------------------------------------------ *
 * Child process records                                               *
 * ------------------------------------------------------------------ */
#define NUM_CHILDREN 3
static const char *child_names[NUM_CHILDREN] = {
    "ingester", "processor", "reporter"
};

static pid_t           child_pids  [NUM_CHILDREN];
static int             child_status[NUM_CHILDREN];
static struct timespec child_start [NUM_CHILDREN];
static struct timespec child_end   [NUM_CHILDREN];
static int             child_done  [NUM_CHILDREN];

/* ------------------------------------------------------------------ *
 * Signal handlers (async-signal-safe: only set atomic flags)         *
 * ------------------------------------------------------------------ */
static void handle_sigchld(int s)  { (void)s; g_sigchld  = 1; }
static void handle_sigint (int s)  { (void)s; g_shutdown = 1;
                                               g_exit_code = EXIT_SIGINT_CODE; }
static void handle_sigterm(int s)  { (void)s; g_shutdown = 1;
                                               g_exit_code = EXIT_SIGTERM_CODE; }
static void handle_sigusr1(int s)  { (void)s; g_sigusr1  = 1; }

static void install_handlers(void)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sigemptyset(&sa.sa_mask);

    sa.sa_handler = handle_sigchld;
    sa.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    sa.sa_handler = handle_sigint;
    sa.sa_flags   = SA_RESTART;
    sigaction(SIGINT,  &sa, NULL);

    sa.sa_handler = handle_sigterm;
    sa.sa_flags   = SA_RESTART;
    sigaction(SIGTERM, &sa, NULL);

    sa.sa_handler = handle_sigusr1;
    sa.sa_flags   = SA_RESTART;
    sigaction(SIGUSR1, &sa, NULL);
}

/* ------------------------------------------------------------------ *
 * cleanup() – unlinks all IPC objects so nothing leaks               *
 * ------------------------------------------------------------------ */
static void cleanup(void)
{
    if (g_shm_addr != MAP_FAILED) {
        munmap(g_shm_addr, g_shm_size);
        g_shm_addr = MAP_FAILED;
    }
    if (g_shm_name[0]) {
        shm_unlink(g_shm_name);
        LOG("Unlinked shared memory: %s", g_shm_name);
        g_shm_name[0] = '\0';
    }
    if (g_sem_name[0]) {
        sem_unlink(g_sem_name);
        LOG("Unlinked semaphore: %s", g_sem_name);
        g_sem_name[0] = '\0';
    }
    if (g_fifo_path[0]) {
        unlink(g_fifo_path);
        LOG("Unlinked FIFO: %s", g_fifo_path);
        g_fifo_path[0] = '\0';
    }
}

/* ------------------------------------------------------------------ *
 * Locate the directory that contains this executable (/proc/self/exe) *
 * ------------------------------------------------------------------ */
static void get_exe_dir(char *out, size_t len)
{
    char buf[MAX_PATH_LEN];
    ssize_t n = readlink("/proc/self/exe", buf, sizeof(buf) - 1);
    if (n > 0) {
        buf[n] = '\0';
        char *slash = strrchr(buf, '/');
        if (slash) { *slash = '\0'; strncpy(out, buf, len - 1); out[len-1]='\0'; return; }
    }
    strncpy(out, ".", len - 1);
    out[len - 1] = '\0';
}

/* ------------------------------------------------------------------ *
 * spawn_child – fork + exec one child process with log redirection.
 *
 * The child:
 *   a) Unblocks all signals (parent had them blocked before fork).
 *   b) Opens its log file.
 *   c) dup2(log_fd, STDOUT) and dup2(log_fd, STDERR)  [OS concept].
 *   d) Closes log_fd.
 *   e) execvp().
 * ------------------------------------------------------------------ */
static pid_t spawn_child(int idx, const char *exe,
                          char *const argv[], const char *log_path)
{
    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        return -1;
    }

    if (pid == 0) {
        /* ---- CHILD ---- */

        /* Unblock all signals inherited from the dispatcher */
        sigset_t empty;
        sigemptyset(&empty);
        sigprocmask(SIG_SETMASK, &empty, NULL);

        /* Open per-process log file */
        int log_fd = open(log_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (log_fd < 0) {
            perror("child: open log");
            _exit(EXIT_IO_ERROR);
        }

        /* Redirect stdout and stderr to the log file (dup2) */
        if (dup2(log_fd, STDOUT_FILENO) < 0 ||
            dup2(log_fd, STDERR_FILENO) < 0) {
            perror("child: dup2");
            _exit(EXIT_IO_ERROR);
        }
        close(log_fd);

        /* Replace this process image with the child executable */
        execvp(exe, argv);
        /* execvp only returns on failure */
        fprintf(stderr, "execvp(%s): %s\n", exe, strerror(errno));
        _exit(EXIT_IPC_FAIL);
    }

    /* ---- PARENT ---- */
    clock_gettime(CLOCK_MONOTONIC, &child_start[idx]);
    child_pids [idx] = pid;
    child_done [idx] = 0;
    child_status[idx] = -1;
    LOG("Spawned %s (PID=%d)", child_names[idx], pid);
    return pid;
}

/* ------------------------------------------------------------------ *
 * reap_children – non-blocking waitpid loop called on SIGCHLD        *
 * ------------------------------------------------------------------ */
static int reap_children(void)
{
    int reaped = 0;
    int status;
    pid_t p;

    while ((p = waitpid(-1, &status, WNOHANG)) > 0) {
        for (int i = 0; i < NUM_CHILDREN; i++) {
            if (child_pids[i] == p && !child_done[i]) {
                clock_gettime(CLOCK_MONOTONIC, &child_end[i]);
                child_status[i] = status;
                child_done[i]   = 1;
                reaped++;
                LOG("Child %s (PID=%d) exited, status=%d",
                    child_names[i], p, WIFEXITED(status) ?
                    WEXITSTATUS(status) : -WTERMSIG(status));
            }
        }
    }
    return reaped;
}

/* ------------------------------------------------------------------ *
 * print_summary – final per-child table printed by the dispatcher    *
 * ------------------------------------------------------------------ */
static void print_summary(void)
{
    fprintf(stdout, "\n╔══════════════════════════════════════════════╗\n");
    fprintf(stdout,   "║          DISPATCHER FINAL SUMMARY             ║\n");
    fprintf(stdout,   "╠═══════════╦═══════╦══════════════╦════════════╣\n");
    fprintf(stdout,   "║ Component ║  PID  ║ Exit Status  ║ Runtime(s) ║\n");
    fprintf(stdout,   "╠═══════════╬═══════╬══════════════╬════════════╣\n");

    for (int i = 0; i < NUM_CHILDREN; i++) {
        double rt = 0.0;
        if (child_done[i]) {
            rt = (double)(child_end[i].tv_sec  - child_start[i].tv_sec)
               + (double)(child_end[i].tv_nsec - child_start[i].tv_nsec) / 1e9;
        }
        int es = child_done[i] && WIFEXITED(child_status[i])
               ? WEXITSTATUS(child_status[i]) : -1;
        fprintf(stdout, "║ %-9s ║ %-5d ║ %-12d ║ %-10.3f ║\n",
                child_names[i], child_pids[i], es, rt);
    }
    fprintf(stdout,   "╚═══════════╩═══════╩══════════════╩════════════╝\n\n");
    fflush(stdout);
}

/* ================================================================== *
 * main                                                                *
 * ================================================================== */
int main(int argc, char *argv[])
{
    /* ---- 1. Argument validation ---- */
    if (argc < 8) {
        fprintf(stderr,
            "Usage: %s <input_dir> <output_dir> <num_threads> <queue_size>"
            " <fifo_path> <shm_name> <sem_name>\n", argv[0]);
        return EXIT_BAD_ARGS;
    }

    const char *input_dir   = argv[1];
    const char *output_dir  = argv[2];
    int         num_threads = atoi(argv[3]);
    int         queue_size  = atoi(argv[4]);
    const char *fifo_path   = argv[5];
    const char *shm_name    = argv[6];
    const char *sem_name    = argv[7];

    if (num_threads < 1 || queue_size < 1) {
        fprintf(stderr, "num_threads and queue_size must be >= 1\n");
        return EXIT_BAD_ARGS;
    }

    strncpy(g_fifo_path, fifo_path, MAX_PATH_LEN - 1);
    strncpy(g_shm_name,  shm_name,  MAX_PATH_LEN - 1);
    strncpy(g_sem_name,  sem_name,  MAX_PATH_LEN - 1);

    LOG("Dispatcher started. input=%s output=%s threads=%d qsize=%d",
        input_dir, output_dir, num_threads, queue_size);

    /* ---- 2. Create logs/ directory ---- */
    char logs_dir[MAX_PATH_LEN];
    snprintf(logs_dir, sizeof(logs_dir), "%s/logs", output_dir);
    if (mkdir(logs_dir, 0755) < 0 && errno != EEXIST) {
        perror("mkdir logs");
        return EXIT_IO_ERROR;
    }

    /* ---- 3. Create named FIFO (ingester → processor data link) ---- */
    if (mkfifo(fifo_path, 0666) < 0 && errno != EEXIST) {
        perror("mkfifo");
        return EXIT_IPC_FAIL;
    }
    LOG("FIFO created: %s", fifo_path);

    /* ---- 4. Create POSIX shared memory (processor → reporter) ---- */
    g_shm_size = sizeof(SharedData);

    /* Remove stale segment if present */
    shm_unlink(shm_name);

    int shm_fd = shm_open(shm_name, O_CREAT | O_RDWR, 0666);
    if (shm_fd < 0) {
        perror("shm_open");
        cleanup();
        return EXIT_IPC_FAIL;
    }
    if (ftruncate(shm_fd, (off_t)g_shm_size) < 0) {
        perror("ftruncate");
        close(shm_fd);
        cleanup();
        return EXIT_IPC_FAIL;
    }
    g_shm_addr = mmap(NULL, g_shm_size,
                      PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (g_shm_addr == MAP_FAILED) {
        perror("mmap");
        close(shm_fd);
        cleanup();
        return EXIT_IPC_FAIL;
    }
    close(shm_fd);
    memset(g_shm_addr, 0, g_shm_size);
    LOG("Shared memory created: %s (%zu bytes)", shm_name, g_shm_size);

    /* ---- 5. Create named POSIX semaphore for reporter readiness ---- */
    sem_unlink(sem_name);   /* remove any stale semaphore */
    sem_t *sem_ready = sem_open(sem_name, O_CREAT | O_EXCL, 0666, 0);
    if (sem_ready == SEM_FAILED) {
        perror("sem_open");
        cleanup();
        return EXIT_IPC_FAIL;
    }
    sem_close(sem_ready);
    LOG("Named semaphore created: %s", sem_name);

    /* ---- 6. Install signal handlers ---- */
    install_handlers();

    /* Block the signals we handle so they cannot arrive between
       fork() and sigsuspend(), preventing any lost-signal race. */
    sigset_t block_set;
    sigemptyset(&block_set);
    sigaddset(&block_set, SIGCHLD);
    sigaddset(&block_set, SIGINT);
    sigaddset(&block_set, SIGTERM);
    sigaddset(&block_set, SIGUSR1);
    sigprocmask(SIG_BLOCK, &block_set, NULL);

    /* ---- 7. Build executable paths ---- */
    char exe_dir[MAX_PATH_LEN];
    get_exe_dir(exe_dir, sizeof(exe_dir));

    char exe_ingester [MAX_PATH_LEN];
    char exe_processor[MAX_PATH_LEN];
    char exe_reporter [MAX_PATH_LEN];
    snprintf(exe_ingester,  sizeof(exe_ingester),  "%s/ingester",  exe_dir);
    snprintf(exe_processor, sizeof(exe_processor), "%s/processor", exe_dir);
    snprintf(exe_reporter,  sizeof(exe_reporter),  "%s/reporter",  exe_dir);

    char s_threads[16], s_queue[16];
    snprintf(s_threads, sizeof(s_threads), "%d", num_threads);
    snprintf(s_queue,   sizeof(s_queue),   "%d", queue_size);

    /* Arg vectors for each child */
    char *args_ingester[]  = { exe_ingester,  (char*)input_dir,
                                (char*)fifo_path, NULL };
    char *args_processor[] = { exe_processor, (char*)fifo_path,
                                (char*)shm_name, (char*)sem_name,
                                s_threads, s_queue, NULL };
    char *args_reporter[]  = { exe_reporter,  (char*)shm_name,
                                (char*)sem_name, (char*)output_dir, NULL };

    /* Log paths */
    char log_i[MAX_PATH_LEN], log_p[MAX_PATH_LEN], log_r[MAX_PATH_LEN];
    snprintf(log_i, sizeof(log_i), "%s/ingester.log",  logs_dir);
    snprintf(log_p, sizeof(log_p), "%s/processor.log", logs_dir);
    snprintf(log_r, sizeof(log_r), "%s/reporter.log",  logs_dir);

    /* ---- 8. Fork-and-exec all three children ---- */
    if (spawn_child(0, exe_ingester,  args_ingester,  log_i) < 0 ||
        spawn_child(1, exe_processor, args_processor, log_p) < 0 ||
        spawn_child(2, exe_reporter,  args_reporter,  log_r) < 0) {
        cleanup();
        return EXIT_IPC_FAIL;
    }

    /* ---- 9. sigsuspend() wait loop (no busy-wait) ---- */

    /* wait_mask = block everything EXCEPT the four signals we handle  */
    sigset_t wait_mask;
    sigfillset(&wait_mask);
    sigdelset(&wait_mask, SIGCHLD);
    sigdelset(&wait_mask, SIGINT);
    sigdelset(&wait_mask, SIGTERM);
    sigdelset(&wait_mask, SIGUSR1);

    int done_count = 0;

    LOG("Entering sigsuspend() wait loop for %d children", NUM_CHILDREN);

    while (done_count < NUM_CHILDREN && !g_shutdown) {
        /* Atomically unblocks the four signals and suspends */
        sigsuspend(&wait_mask);

        /* ---- Handle SIGCHLD: reap exited children ---- */
        if (g_sigchld) {
            g_sigchld = 0;
            done_count += reap_children();
        }

        /* ---- Handle SIGUSR1: dump status, forward to children ---- */
        if (g_sigusr1) {
            g_sigusr1 = 0;
            LOG("SIGUSR1 received – forwarding to children and printing status");
            for (int i = 0; i < NUM_CHILDREN; i++) {
                if (!child_done[i] && child_pids[i] > 0)
                    kill(child_pids[i], SIGUSR1);
            }
        }
    }

    /* ---- 10. Shutdown path (SIGINT / SIGTERM) ---- */
    if (g_shutdown) {
        LOG("Shutdown signal received (%s) – forwarding SIGTERM to children",
            g_exit_code == EXIT_SIGINT_CODE ? "SIGINT" : "SIGTERM");

        for (int i = 0; i < NUM_CHILDREN; i++) {
            if (!child_done[i] && child_pids[i] > 0)
                kill(child_pids[i], SIGTERM);
        }

        /* Give children up to 5 s to exit, then SIGKILL */
        int wait_rounds = 50;
        while (done_count < NUM_CHILDREN && wait_rounds-- > 0) {
            struct timespec ts = { 0, 100000000L }; /* 100 ms */
            nanosleep(&ts, NULL);
            done_count += reap_children();
        }
        /* Force-kill any survivors */
        for (int i = 0; i < NUM_CHILDREN; i++) {
            if (!child_done[i] && child_pids[i] > 0) {
                kill(child_pids[i], SIGKILL);
                waitpid(child_pids[i], &child_status[i], 0);
                child_done[i] = 1;
                done_count++;
                LOG("Force-killed %s (PID=%d)", child_names[i], child_pids[i]);
            }
        }
    } else {
        /* Normal exit: reap any remaining children */
        int remaining;
        do {
            remaining = reap_children();
            done_count += remaining;
        } while (remaining > 0 && done_count < NUM_CHILDREN);
    }

    /* ---- 11. Cleanup IPC resources ---- */
    cleanup();

    /* ---- 12. Print per-child summary ---- */
    print_summary();

    /* Detect if any child exited abnormally */
    for (int i = 0; i < NUM_CHILDREN; i++) {
        if (child_done[i] && WIFSIGNALED(child_status[i])) {
            LOG("WARNING: %s killed by signal %d",
                child_names[i], WTERMSIG(child_status[i]));
            if (g_exit_code == EXIT_OK)
                g_exit_code = EXIT_CHILD_DIED;
        }
    }

    LOG("Dispatcher exiting with code %d", (int)g_exit_code);
    return (int)g_exit_code;
}
