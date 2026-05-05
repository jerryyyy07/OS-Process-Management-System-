#include "common/common.h"
#define NUM_CHILDREN 3



// --- Global Signal Flags ---
static volatile sig_atomic_t g_sigchld   = 0;
static volatile sig_atomic_t g_shutdown  = 0;
static volatile sig_atomic_t g_sigusr1   = 0;
static volatile sig_atomic_t g_exit_code = EXIT_OK;

// --- IPC Resources ---
static char   g_fifo_path[MAX_PATH_LEN] = "";
static char   g_shm_name[MAX_PATH_LEN]  = "";
static char   g_sem_name[MAX_PATH_LEN]  = "";
static void  *g_shm_addr                = MAP_FAILED;
static size_t g_shm_size                = 0;

// --- Child Process Tracking ---
static const char *child_names[NUM_CHILDREN] = { "ingester", "processor", "reporter" };
static pid_t           child_pids[NUM_CHILDREN];
static int             child_status[NUM_CHILDREN];
static struct timespec child_start[NUM_CHILDREN];
static struct timespec child_end[NUM_CHILDREN];
static int             child_done[NUM_CHILDREN];

// --- Signal Handlers ---
static void handle_sigchld(int s) { (void)s; g_sigchld = 1; }
static void handle_sigusr1(int s) { (void)s; g_sigusr1 = 1; }
static void handle_sigint (int s) { (void)s; g_shutdown = 1; g_exit_code = EXIT_SIGINT_CODE; }
static void handle_sigterm(int s) { (void)s; g_shutdown = 1; g_exit_code = EXIT_SIGTERM_CODE; }

static void install_handlers(void) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;

    sa.sa_handler = handle_sigchld;
    sa.sa_flags |= SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    sa.sa_flags &= ~SA_NOCLDSTOP;
    sa.sa_handler = handle_sigint;  sigaction(SIGINT,  &sa, NULL);
    sa.sa_handler = handle_sigterm; sigaction(SIGTERM, &sa, NULL);
    sa.sa_handler = handle_sigusr1; sigaction(SIGUSR1, &sa, NULL);
}

// --- Cleanup IPC Resources ---
static void cleanup(void) {
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

// --- Utilities ---
static void get_exe_dir(char *out, size_t len) {
    char buf[MAX_PATH_LEN];
    ssize_t n = readlink("/proc/self/exe", buf, sizeof(buf) - 1);
    if (n > 0) {
        buf[n] = '\0';
        char *slash = strrchr(buf, '/');
        if (slash) {
            *slash = '\0';
            strncpy(out, buf, len - 1);
            out[len-1] = '\0';
            return;
        }
    }
    strncpy(out, ".", len - 1);
    out[len - 1] = '\0';
}

static pid_t spawn_child(int idx, const char *exe, char *const argv[], const char *log_path) {
    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        return -1;
    }

    if (pid == 0) {
        // Child Process
        sigset_t empty;
        sigemptyset(&empty);
        sigprocmask(SIG_SETMASK, &empty, NULL);

        int log_fd = open(log_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (log_fd < 0) {
            perror("open log");
            _exit(EXIT_IO_ERROR);
        }

        dup2(log_fd, STDOUT_FILENO);
        dup2(log_fd, STDERR_FILENO);
        close(log_fd);

        execvp(exe, argv);
        fprintf(stderr, "execvp(%s) failed: %s\n", exe, strerror(errno));
        _exit(EXIT_IPC_FAIL);
    }

    // Parent Process
    clock_gettime(CLOCK_MONOTONIC, &child_start[idx]);
    child_pids[idx] = pid;
    child_done[idx] = 0;
    child_status[idx] = -1;
    LOG("Spawned %s (PID: %d)", child_names[idx], pid);
    
    return pid;
}

static int reap_children(void) {
    int reaped = 0, status;
    pid_t p;

    while ((p = waitpid(-1, &status, WNOHANG)) > 0) {
        for (int i = 0; i < NUM_CHILDREN; i++) {
            if (child_pids[i] == p && !child_done[i]) {
                clock_gettime(CLOCK_MONOTONIC, &child_end[i]);
                child_status[i] = status;
                child_done[i] = 1;
                reaped++;
                
                int code = WIFEXITED(status) ? WEXITSTATUS(status) : -WTERMSIG(status);
                LOG("Child %s (PID: %d) exited with status: %d", child_names[i], p, code);
            }
        }
    }
    return reaped;
}

static void print_summary(void) {
    printf("\n----------------------------------------------------------\n");
    printf("                  EXECUTION SUMMARY                       \n");
    printf("----------------------------------------------------------\n");
    printf("%-12s | %-8s | %-12s | %-12s\n", "Component", "PID", "Exit Status", "Runtime (s)");
    printf("----------------------------------------------------------\n");

    for (int i = 0; i < NUM_CHILDREN; i++) {
        double rt = 0.0;
        if (child_done[i]) {
            rt = (child_end[i].tv_sec - child_start[i].tv_sec) + 
                 (child_end[i].tv_nsec - child_start[i].tv_nsec) / 1e9;
        }
        int status = (child_done[i] && WIFEXITED(child_status[i])) ? WEXITSTATUS(child_status[i]) : -1;
        printf("%-12s | %-8d | %-12d | %-12.3f\n", child_names[i], child_pids[i], status, rt);
    }
    printf("----------------------------------------------------------\n\n");
    fflush(stdout);
}

// --- Main Program ---
int main(int argc, char *argv[]) {
    if (argc < 8) {
        fprintf(stderr, "Usage: %s <input_dir> <output_dir> <num_threads> <queue_size> <fifo_path> <shm_name> <sem_name>\n", argv[0]);
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
        fprintf(stderr, "Error: num_threads and queue_size must be >= 1.\n");
        return EXIT_BAD_ARGS;
    }

    strncpy(g_fifo_path, fifo_path, MAX_PATH_LEN - 1);
    strncpy(g_shm_name,  shm_name,  MAX_PATH_LEN - 1);
    strncpy(g_sem_name,  sem_name,  MAX_PATH_LEN - 1);

    LOG("Dispatcher started. threads=%d, queue=%d", num_threads, queue_size);

    // Setup Logs Directory
    char logs_dir[MAX_PATH_LEN];
    snprintf(logs_dir, sizeof(logs_dir), "%s/logs", output_dir);
    if (mkdir(logs_dir, 0755) < 0 && errno != EEXIST) {
        perror("mkdir logs");
        return EXIT_IO_ERROR;
    }

    // Setup FIFO
    if (mkfifo(fifo_path, 0666) < 0 && errno != EEXIST) {
        perror("mkfifo");
        return EXIT_IPC_FAIL;
    }

    // Setup Shared Memory
    g_shm_size = sizeof(SharedData);
    shm_unlink(shm_name);
    int shm_fd = shm_open(shm_name, O_CREAT | O_RDWR, 0666);
    if (shm_fd < 0 || ftruncate(shm_fd, g_shm_size) < 0) {
        perror("shm setup failed");
        cleanup();
        return EXIT_IPC_FAIL;
    }
    
    g_shm_addr = mmap(NULL, g_shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    close(shm_fd);
    if (g_shm_addr == MAP_FAILED) {
        perror("mmap");
        cleanup();
        return EXIT_IPC_FAIL;
    }
    memset(g_shm_addr, 0, g_shm_size);

    // Setup Semaphore
    sem_unlink(sem_name);
    sem_t *sem_ready = sem_open(sem_name, O_CREAT | O_EXCL, 0666, 0);
    if (sem_ready == SEM_FAILED) {
        perror("sem_open");
        cleanup();
        return EXIT_IPC_FAIL;
    }
    sem_close(sem_ready);

    // Install Signals & Block during setup
    install_handlers();
    sigset_t block_set;
    sigemptyset(&block_set);
    sigaddset(&block_set, SIGCHLD);
    sigaddset(&block_set, SIGINT);
    sigaddset(&block_set, SIGTERM);
    sigaddset(&block_set, SIGUSR1);
    sigprocmask(SIG_BLOCK, &block_set, NULL);

    // Prepare paths and arguments
    char exe_dir[MAX_PATH_LEN], exe_i[MAX_PATH_LEN], exe_p[MAX_PATH_LEN], exe_r[MAX_PATH_LEN];
    get_exe_dir(exe_dir, sizeof(exe_dir));
    snprintf(exe_i, sizeof(exe_i), "%s/ingester", exe_dir);
    snprintf(exe_p, sizeof(exe_p), "%s/processor", exe_dir);
    snprintf(exe_r, sizeof(exe_r), "%s/reporter", exe_dir);

    char s_threads[16], s_queue[16];
    snprintf(s_threads, sizeof(s_threads), "%d", num_threads);
    snprintf(s_queue, sizeof(s_queue), "%d", queue_size);

    char *args_i[] = { exe_i, (char*)input_dir, (char*)fifo_path, NULL };
    char *args_p[] = { exe_p, (char*)fifo_path, (char*)shm_name, (char*)sem_name, s_threads, s_queue, NULL };
    char *args_r[] = { exe_r, (char*)shm_name, (char*)sem_name, (char*)output_dir, NULL };

    char log_i[MAX_PATH_LEN], log_p[MAX_PATH_LEN], log_r[MAX_PATH_LEN];
    snprintf(log_i, sizeof(log_i), "%s/ingester.log", logs_dir);
    snprintf(log_p, sizeof(log_p), "%s/processor.log", logs_dir);
    snprintf(log_r, sizeof(log_r), "%s/reporter.log", logs_dir);

    // Spawn Workers
    if (spawn_child(0, exe_i, args_i, log_i) < 0 ||
        spawn_child(1, exe_p, args_p, log_p) < 0 ||
        spawn_child(2, exe_r, args_r, log_r) < 0) {
        cleanup();
        return EXIT_IPC_FAIL;
    }

    // Wait Loop
    sigset_t wait_mask;
    sigfillset(&wait_mask);
    sigdelset(&wait_mask, SIGCHLD);
    sigdelset(&wait_mask, SIGINT);
    sigdelset(&wait_mask, SIGTERM);
    sigdelset(&wait_mask, SIGUSR1);

    int done_count = 0;
    while (done_count < NUM_CHILDREN && !g_shutdown) {
        sigsuspend(&wait_mask);

        if (g_sigchld) {
            g_sigchld = 0;
            done_count += reap_children();
        }

        if (g_sigusr1) {
            g_sigusr1 = 0;
            LOG("Received SIGUSR1. Forwarding to active children.");
            for (int i = 0; i < NUM_CHILDREN; i++) {
                if (!child_done[i] && child_pids[i] > 0)
                    kill(child_pids[i], SIGUSR1);
            }
        }
    }

    // Shutdown Process
    if (g_shutdown) {
        LOG("Received shutdown signal. Terminating children.");
        for (int i = 0; i < NUM_CHILDREN; i++) {
            if (!child_done[i] && child_pids[i] > 0)
                kill(child_pids[i], SIGTERM);
        }

        int wait_rounds = 50; 
        while (done_count < NUM_CHILDREN && wait_rounds-- > 0) {
            struct timespec ts = { 0, 100000000L }; // 100ms
            nanosleep(&ts, NULL);
            done_count += reap_children();
        }
        
        for (int i = 0; i < NUM_CHILDREN; i++) {
            if (!child_done[i] && child_pids[i] > 0) {
                kill(child_pids[i], SIGKILL);
                waitpid(child_pids[i], &child_status[i], 0);
                child_done[i] = 1;
                done_count++;
                LOG("Force-killed %s (PID: %d)", child_names[i], child_pids[i]);
            }
        }
    } else {
        int remaining;
        do {
            remaining = reap_children();
            done_count += remaining;
        } while (remaining > 0 && done_count < NUM_CHILDREN);
    }

    cleanup();
    print_summary();

    for (int i = 0; i < NUM_CHILDREN; i++) {
        if (child_done[i] && WIFSIGNALED(child_status[i])) {
            LOG("WARNING: %s terminated abnormally (Signal: %d)", child_names[i], WTERMSIG(child_status[i]));
            if (g_exit_code == EXIT_OK) g_exit_code = EXIT_CHILD_DIED;
        }
    }

    LOG("Dispatcher exiting with code %d", (int)g_exit_code);
    return (int)g_exit_code;
}