/* ==================================================================
 * processor.c  –  Stage 2 of the OS Pipeline.
 *
 * Responsibilities:
 *   1. Open the named FIFO for reading.
 *   2. Spawn a dedicated reader thread + N worker threads (POSIX
 *      pthreads with explicit pthread_attr_t for stack size and
 *      detach state).
 *   3. Maintain a bounded queue of size Q protected by:
 *        - a mutex (pthread_mutex_t) for the queue itself, and
 *        - two unnamed semaphores:
 *            sem_empty  (initial value Q) – counts free slots
 *            sem_full   (initial value 0) – counts filled slots
 *   4. Reader thread: pulls ChunkHeader + data from FIFO, mallocs a
 *      buffer, and enqueues a QueueItem.  On EOF sentinel, enqueues
 *      N poison-pill items so every worker exits cleanly.
 *   5. Worker threads: dequeue items, parse CSV rows, and update the
 *      shared aggregation table under a second mutex (agg_mutex).
 *      Retail logic:  first column = category key;
 *                     remaining numeric columns summed = revenue.
 *   6. After all workers exit (joined by main), serialise the
 *      aggregation table into the POSIX shared-memory segment and
 *      sem_post() the named ready-semaphore for the reporter.
 *   7. Handle SIGTERM (graceful shutdown) and SIGUSR1 (stats dump).
 *
 * Usage (launched by dispatcher):
 *   processor <fifo_path> <shm_name> <sem_name> <num_threads> <queue_size>
 * ================================================================== */

#include "common/common.h"

/* ================================================================== *
 * Bounded queue                                                        *
 * ================================================================== */
typedef struct {
    QueueItem      *items;
    int             capacity;
    int             head;
    int             tail;
    int             count;
    pthread_mutex_t mutex;
    sem_t           sem_empty;   /* counting semaphore: free slots    */
    sem_t           sem_full;    /* counting semaphore: filled slots  */
} BoundedQueue;

static int bq_init(BoundedQueue *q, int cap)
{
    q->items = (QueueItem *)malloc((size_t)cap * sizeof(QueueItem));
    if (!q->items) return -1;
    q->capacity = cap;
    q->head = q->tail = q->count = 0;

    if (pthread_mutex_init(&q->mutex, NULL) != 0)          goto err_items;
    if (sem_init(&q->sem_empty, 0, (unsigned)cap) != 0)    goto err_mutex;
    if (sem_init(&q->sem_full,  0, 0)             != 0)    goto err_empty;
    return 0;

err_empty: sem_destroy(&q->sem_empty);
err_mutex: pthread_mutex_destroy(&q->mutex);
err_items: free(q->items);
    return -1;
}

/* Producer: blocks until a free slot is available */
static void bq_enqueue(BoundedQueue *q, const QueueItem *item)
{
    sem_wait(&q->sem_empty);
    pthread_mutex_lock(&q->mutex);
    q->items[q->tail] = *item;
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;
    pthread_mutex_unlock(&q->mutex);
    sem_post(&q->sem_full);
}

/* Consumer: blocks until an item is available */
static void bq_dequeue(BoundedQueue *q, QueueItem *item)
{
    sem_wait(&q->sem_full);
    pthread_mutex_lock(&q->mutex);
    *item = q->items[q->head];
    q->head = (q->head + 1) % q->capacity;
    q->count--;
    pthread_mutex_unlock(&q->mutex);
    sem_post(&q->sem_empty);
}

static void bq_destroy(BoundedQueue *q)
{
    sem_destroy(&q->sem_empty);
    sem_destroy(&q->sem_full);
    pthread_mutex_destroy(&q->mutex);
    free(q->items);
}

/* ================================================================== *
 * Aggregation table (process-local; serialised to shm at the end)    *
 * ================================================================== */
static CategoryEntry   g_agg[MAX_CATEGORIES];
static int             g_num_cats   = 0;
static ProductEntry    g_prod_agg[MAX_PRODUCTS];
static int             g_num_prods  = 0;
static uint64_t        g_total_rows = 0;
static pthread_mutex_t g_agg_mutex  = PTHREAD_MUTEX_INITIALIZER;

/* Find-or-create a category entry; returns index or -1 if table full */
static int agg_find_or_create(const char *cat)
{
    for (int i = 0; i < g_num_cats; i++)
        if (strncmp(g_agg[i].category, cat, MAX_CATEGORY_LEN) == 0)
            return i;
    if (g_num_cats >= MAX_CATEGORIES) return -1;
    int idx = g_num_cats++;
    memset(&g_agg[idx], 0, sizeof(CategoryEntry));
    strncpy(g_agg[idx].category, cat, MAX_CATEGORY_LEN - 1);
    g_agg[idx].min_single = 1e308;  /* will be updated on first row */
    return idx;
}

/* Find-or-create a product entry; returns index or -1 if table full */
static int agg_prod_find_or_create(const char *prod)
{
    for (int i = 0; i < g_num_prods; i++)
        if (strncmp(g_prod_agg[i].product, prod, MAX_PRODUCT_LEN) == 0)
            return i;
    if (g_num_prods >= MAX_PRODUCTS) return -1;
    int idx = g_num_prods++;
    memset(&g_prod_agg[idx], 0, sizeof(ProductEntry));
    strncpy(g_prod_agg[idx].product, prod, MAX_PRODUCT_LEN - 1);
    return idx;
}

/* ================================================================== *
 * Retail CSV parsing                                                   *
 *                                                                      *
 * Format: category,num1[,num2,...]                                     *
 *   – col 0  : category key (string)                                   *
 *   – col 1+ : numeric values; all are summed → revenue for this row   *
 *                                                                      *
 * Non-numeric tokens after col 0 (e.g. product names) are skipped.    *
 * ================================================================== */
static void parse_and_aggregate(const char *line)
{
    /* Work on a local copy so strtok_r can modify it */
    char buf[MAX_LINE_LEN];
    strncpy(buf, line, MAX_LINE_LEN - 1);
    buf[MAX_LINE_LEN - 1] = '\0';

    /* Strip trailing newline / CR */
    char *nl = strchr(buf, '\n'); if (nl) *nl = '\0';
    char *cr = strchr(buf, '\r'); if (cr) *cr = '\0';
    if (buf[0] == '\0' || buf[0] == '#') return;

    /* Skip header line if present */
    if (strncmp(buf, "Category,ProductName", 20) == 0) return;

    char *saveptr = NULL;
    char *cat_tok = strtok_r(buf, ",", &saveptr);
    if (!cat_tok || cat_tok[0] == '\0') return;

    char *prod_tok = strtok_r(NULL, ",", &saveptr);
    if (!prod_tok || prod_tok[0] == '\0') return;

    char category[MAX_CATEGORY_LEN];
    strncpy(category, cat_tok, MAX_CATEGORY_LEN - 1);
    category[MAX_CATEGORY_LEN - 1] = '\0';

    char product[MAX_PRODUCT_LEN];
    strncpy(product, prod_tok, MAX_PRODUCT_LEN - 1);
    product[MAX_PRODUCT_LEN - 1] = '\0';

    /* Sum all numeric tokens that follow the product */
    double revenue    = 0.0;
    int    has_number = 0;
    char  *token;

    while ((token = strtok_r(NULL, ",", &saveptr)) != NULL) {
        char   *endptr = NULL;
        double  val    = strtod(token, &endptr);
        /* Accept the value only if the entire token was consumed */
        if (endptr && endptr != token &&
            (*endptr == '\0' || *endptr == '\r' || *endptr == '\n')) {
            revenue    += val;
            has_number  = 1;
        }
    }

    if (!has_number) return;

    /* ---- Update aggregation table under mutex ---- */
    pthread_mutex_lock(&g_agg_mutex);

    int idx = agg_find_or_create(category);
    if (idx >= 0) {
        g_agg[idx].total_revenue += revenue;
        g_agg[idx].record_count++;
        if (revenue > g_agg[idx].max_single) g_agg[idx].max_single = revenue;
        if (revenue < g_agg[idx].min_single) g_agg[idx].min_single = revenue;
    }

    int p_idx = agg_prod_find_or_create(product);
    if (p_idx >= 0) {
        g_prod_agg[p_idx].total_revenue += revenue;
    }

    g_total_rows++;
    pthread_mutex_unlock(&g_agg_mutex);
}

/* ================================================================== *
 * Worker thread                                                        *
 * ================================================================== */
typedef struct {
    BoundedQueue *queue;
    int           thread_id;
    uint64_t      rows_done;   /* local counter; added to g_total_rows at end */
} WorkerArg;

static void *worker_fn(void *arg)
{
    WorkerArg *wa = (WorkerArg *)arg;
    uint64_t   local_rows = 0;

    LOG("Worker thread %d started", wa->thread_id);

    while (1) {
        QueueItem item;
        bq_dequeue(wa->queue, &item);

        if (item.is_poison) {
            LOG("Worker %d: poison pill received – exiting (rows=%llu)",
                wa->thread_id, (unsigned long long)local_rows);
            free(item.data); /* data is NULL for poison, but free(NULL) is safe */
            break;
        }

        /* Parse every line in this chunk */
        char *p     = item.data;
        char *start = p;

        while (*p != '\0') {
            if (*p == '\n') {
                *p = '\0';
                if (start < p) {
                    parse_and_aggregate(start);
                    local_rows++;
                }
                start = p + 1;
            }
            p++;
        }
        /* Handle final line without trailing newline */
        if (start < p && *start != '\0') {
            parse_and_aggregate(start);
            local_rows++;
        }

        free(item.data);
    }

    wa->rows_done = local_rows;
    return NULL;
}

/* ================================================================== *
 * Reader thread (FIFO → bounded queue)                                *
 * ================================================================== */
typedef struct {
    int           fifo_fd;
    BoundedQueue *queue;
    int           num_workers;
} ReaderArg;

static void *reader_fn(void *arg)
{
    ReaderArg *ra       = (ReaderArg *)arg;
    int        fifo_fd  = ra->fifo_fd;
    uint32_t   chunks_read = 0;

    LOG("Reader thread started");

    while (1) {
        ChunkHeader hdr;
        ssize_t r = read_all(fifo_fd, &hdr, sizeof(hdr));

        if (r == 0) {
            /* FIFO closed unexpectedly – treat as EOF */
            LOG("Reader: FIFO closed without EOF sentinel");
            break;
        }
        if (r < (ssize_t)sizeof(hdr)) {
            LOG("Reader: short header read (%zd bytes)", r);
            break;
        }

        if (hdr.is_eof) {
            LOG("Reader: EOF sentinel received after %u data chunks",
                chunks_read);
            break;
        }

        /* Read the CSV payload */
        char *data = (char *)malloc(hdr.byte_count + 1);
        if (!data) {
            LOG("Reader: malloc(%u) failed", hdr.byte_count);
            break;
        }
        r = read_all(fifo_fd, data, hdr.byte_count);
        if (r < (ssize_t)hdr.byte_count) {
            LOG("Reader: short data read (%zd / %u bytes)", r, hdr.byte_count);
            free(data);
            break;
        }
        data[hdr.byte_count] = '\0';

        QueueItem item;
        item.data      = data;
        item.size      = hdr.byte_count;
        item.chunk_id  = hdr.chunk_id;
        item.is_poison = 0;

        bq_enqueue(ra->queue, &item);
        chunks_read++;
        LOG("Reader: enqueued chunk #%u (%u bytes, file_id=%u)",
            hdr.chunk_id, hdr.byte_count, hdr.source_file_id);
    }

    /* Send one poison pill per worker so they all exit */
    LOG("Reader: sending %d poison pills", ra->num_workers);
    for (int i = 0; i < ra->num_workers; i++) {
        QueueItem poison = { NULL, 0, 0, 1 };
        bq_enqueue(ra->queue, &poison);
    }

    return NULL;
}

/* ================================================================== *
 * Signal handling                                                      *
 * ================================================================== */
static volatile sig_atomic_t g_shutdown  = 0;
static volatile sig_atomic_t g_do_stats  = 0;
static volatile sig_atomic_t g_exit_code = EXIT_OK;

static void handle_sigterm(int s) {
    (void)s; g_shutdown = 1; g_exit_code = EXIT_SIGTERM_CODE;
}
static void handle_sigusr1(int s) { (void)s; g_do_stats = 1; }

static void install_handlers(void)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;

    sa.sa_handler = handle_sigterm;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);

    sa.sa_handler = handle_sigusr1;
    sigaction(SIGUSR1, &sa, NULL);
}

/* ================================================================== *
 * main                                                                 *
 * ================================================================== */
int main(int argc, char *argv[])
{
    if (argc < 6) {
        fprintf(stderr,
            "Usage: processor <fifo_path> <shm_name> <sem_name>"
            " <num_threads> <queue_size>\n");
        return EXIT_BAD_ARGS;
    }

    const char *fifo_path   = argv[1];
    const char *shm_name    = argv[2];
    const char *sem_name    = argv[3];
    int         num_threads = atoi(argv[4]);
    int         queue_size  = atoi(argv[5]);

    if (num_threads < 1 || queue_size < 1) {
        fprintf(stderr, "num_threads and queue_size must be >= 1\n");
        return EXIT_BAD_ARGS;
    }

    install_handlers();
    LOG("Processor started. fifo=%s shm=%s sem=%s threads=%d queue=%d",
        fifo_path, shm_name, sem_name, num_threads, queue_size);

    /* ---- Open FIFO for reading (blocks until ingester opens write end) ---- */
    LOG("Opening FIFO for reading…");
    int fifo_fd = open(fifo_path, O_RDONLY);
    if (fifo_fd < 0) {
        LOG("Cannot open FIFO %s: %s", fifo_path, strerror(errno));
        return EXIT_IPC_FAIL;
    }
    LOG("FIFO opened for reading");

    /* ---- Initialise bounded queue ---- */
    BoundedQueue queue;
    if (bq_init(&queue, queue_size) < 0) {
        LOG("Failed to initialise bounded queue");
        close(fifo_fd);
        return EXIT_IPC_FAIL;
    }

    /* ---- Create worker threads with explicit pthread_attr_t ---- */
    /*
     * Thread attributes (explained in report):
     *   – detach state : PTHREAD_CREATE_JOINABLE so main can join them
     *   – stack size   : 2 MiB per worker (sufficient for parsing buffers)
     */
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    pthread_attr_setstacksize(&attr, 2 * 1024 * 1024); /* 2 MiB */

    pthread_t *worker_threads = (pthread_t *)malloc(
                                    (size_t)num_threads * sizeof(pthread_t));
    WorkerArg *worker_args    = (WorkerArg *)malloc(
                                    (size_t)num_threads * sizeof(WorkerArg));
    if (!worker_threads || !worker_args) {
        LOG("malloc failed for thread arrays");
        free(worker_threads); free(worker_args);
        bq_destroy(&queue);
        close(fifo_fd);
        return EXIT_IPC_FAIL;
    }

    for (int i = 0; i < num_threads; i++) {
        worker_args[i].queue     = &queue;
        worker_args[i].thread_id = i;
        worker_args[i].rows_done = 0;
        if (pthread_create(&worker_threads[i], &attr,
                           worker_fn, &worker_args[i]) != 0) {
            LOG("pthread_create failed for worker %d: %s", i, strerror(errno));
            /* Poison the queue so already-started threads exit */
            for (int j = 0; j < i; j++) {
                QueueItem p = { NULL, 0, 0, 1 };
                bq_enqueue(&queue, &p);
            }
            for (int j = 0; j < i; j++)
                pthread_join(worker_threads[j], NULL);
            pthread_attr_destroy(&attr);
            free(worker_threads); free(worker_args);
            bq_destroy(&queue);
            close(fifo_fd);
            return EXIT_IPC_FAIL;
        }
    }
    pthread_attr_destroy(&attr);

    /* ---- Create reader thread (uses default attr; it's also joined) ---- */
    ReaderArg reader_arg;
    reader_arg.fifo_fd     = fifo_fd;
    reader_arg.queue       = &queue;
    reader_arg.num_workers = num_threads;

    pthread_t reader_thread;
    if (pthread_create(&reader_thread, NULL, reader_fn, &reader_arg) != 0) {
        LOG("pthread_create failed for reader: %s", strerror(errno));
        free(worker_threads); free(worker_args);
        bq_destroy(&queue);
        close(fifo_fd);
        return EXIT_IPC_FAIL;
    }
    LOG("Spawned %d worker thread(s) + 1 reader thread", num_threads);

    /* ---- Periodically print stats if SIGUSR1 arrives ---- */
    /* Main thread simply waits for reader to finish, then joins workers */
    pthread_join(reader_thread, NULL);
    LOG("Reader thread finished – joining workers");

    for (int i = 0; i < num_threads; i++) {
        pthread_join(worker_threads[i], NULL);
        LOG("Worker %d joined (rows_done=%llu)",
            i, (unsigned long long)worker_args[i].rows_done);
    }

    pthread_mutex_destroy(&g_agg_mutex);

    if (g_do_stats) {
        LOG("STATS: categories=%d total_rows=%llu",
            g_num_cats, (unsigned long long)g_total_rows);
    }

    LOG("Aggregation complete. categories=%d total_rows=%llu",
        g_num_cats, (unsigned long long)g_total_rows);

    free(worker_threads);
    free(worker_args);
    bq_destroy(&queue);
    close(fifo_fd);

    /* ---- Serialise aggregation table → POSIX shared memory ---- */
    int shm_fd = shm_open(shm_name, O_RDWR, 0666);
    if (shm_fd < 0) {
        LOG("shm_open(%s) failed: %s", shm_name, strerror(errno));
        return EXIT_IPC_FAIL;
    }

    SharedData *shm = (SharedData *)mmap(NULL, sizeof(SharedData),
                                          PROT_READ | PROT_WRITE,
                                          MAP_SHARED, shm_fd, 0);
    close(shm_fd);
    if (shm == MAP_FAILED) {
        LOG("mmap failed: %s", strerror(errno));
        return EXIT_IPC_FAIL;
    }

    shm->num_categories = g_num_cats;
    shm->num_products   = g_num_prods;
    shm->total_records  = g_total_rows;
    for (int i = 0; i < g_num_cats; i++)
        shm->categories[i] = g_agg[i];
    for (int i = 0; i < g_num_prods; i++)
        shm->products[i] = g_prod_agg[i];

    munmap(shm, sizeof(SharedData));
    LOG("Aggregation data written to shared memory (%s)", shm_name);

    /* ---- Signal reporter that data is ready (sem_post) ---- */
    sem_t *sem_ready = sem_open(sem_name, 0);
    if (sem_ready == SEM_FAILED) {
        LOG("sem_open(%s) failed: %s", sem_name, strerror(errno));
        return EXIT_IPC_FAIL;
    }
    sem_post(sem_ready);
    sem_close(sem_ready);
    LOG("Named semaphore %s posted – reporter can proceed", sem_name);

    LOG("Processor exiting with code %d", (int)g_exit_code);
    return (int)g_exit_code;
}
