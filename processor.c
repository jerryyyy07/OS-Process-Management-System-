/*
 * processor.c
 *
 * Stage 2 of the OS Pipeline.
 * Reads chunks from the FIFO, processes them using a thread pool,
 * aggregates the data, and writes the results to shared memory.
 */

#include "common/common.h"

// --- Global Signal & State Flags ---
static volatile sig_atomic_t g_shutdown  = 0;
static volatile sig_atomic_t g_do_stats  = 0;
static volatile sig_atomic_t g_exit_code = EXIT_OK;

// --- Signal Handlers ---
static void handle_sigterm(int s) { (void)s; g_shutdown = 1; g_exit_code = EXIT_SIGTERM_CODE; }
static void handle_sigusr1(int s) { (void)s; g_do_stats = 1; }

static void install_handlers(void) {
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

// --- Bounded Queue ---
typedef struct {
    QueueItem      *items;
    int             capacity;
    int             head;
    int             tail;
    int             count;
    pthread_mutex_t mutex;
    sem_t           sem_empty; // Free slots
    sem_t           sem_full;  // Filled slots
} BoundedQueue;

static int bq_init(BoundedQueue *q, int cap) {
    q->items = (QueueItem *)malloc((size_t)cap * sizeof(QueueItem));
    if (!q->items) return -1;
    
    q->capacity = cap;
    q->head = q->tail = q->count = 0;

    if (pthread_mutex_init(&q->mutex, NULL) != 0)       goto err_items;
    if (sem_init(&q->sem_empty, 0, (unsigned)cap) != 0) goto err_mutex;
    if (sem_init(&q->sem_full,  0, 0) != 0)             goto err_empty;
    
    return 0;

err_empty: sem_destroy(&q->sem_empty);
err_mutex: pthread_mutex_destroy(&q->mutex);
err_items: free(q->items);
    return -1;
}

static void bq_enqueue(BoundedQueue *q, const QueueItem *item) {
    sem_wait(&q->sem_empty);
    pthread_mutex_lock(&q->mutex);
    
    q->items[q->tail] = *item;
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;
    
    pthread_mutex_unlock(&q->mutex);
    sem_post(&q->sem_full);
}

static void bq_dequeue(BoundedQueue *q, QueueItem *item) {
    sem_wait(&q->sem_full);
    pthread_mutex_lock(&q->mutex);
    
    *item = q->items[q->head];
    q->head = (q->head + 1) % q->capacity;
    q->count--;
    
    pthread_mutex_unlock(&q->mutex);
    sem_post(&q->sem_empty);
}

static void bq_destroy(BoundedQueue *q) {
    sem_destroy(&q->sem_empty);
    sem_destroy(&q->sem_full);
    pthread_mutex_destroy(&q->mutex);
    free(q->items);
}

// --- Aggregation State ---
static CategoryEntry   g_agg[MAX_CATEGORIES];
static int             g_num_cats   = 0;
static ProductEntry    g_prod_agg[MAX_PRODUCTS];
static int             g_num_prods  = 0;
static uint64_t        g_total_rows = 0;
static pthread_mutex_t g_agg_mutex  = PTHREAD_MUTEX_INITIALIZER;

static int agg_find_or_create(const char *cat) {
    for (int i = 0; i < g_num_cats; i++) {
        if (strncmp(g_agg[i].category, cat, MAX_CATEGORY_LEN) == 0) return i;
    }
    if (g_num_cats >= MAX_CATEGORIES) return -1;
    
    int idx = g_num_cats++;
    memset(&g_agg[idx], 0, sizeof(CategoryEntry));
    snprintf(g_agg[idx].category, MAX_CATEGORY_LEN, "%s", cat);
    g_agg[idx].min_single = 1e308; // Initialize min to a very large number
    return idx;
}

static int agg_prod_find_or_create(const char *prod) {
    for (int i = 0; i < g_num_prods; i++) {
        if (strncmp(g_prod_agg[i].product, prod, MAX_PRODUCT_LEN) == 0) return i;
    }
    if (g_num_prods >= MAX_PRODUCTS) return -1;
    
    int idx = g_num_prods++;
    memset(&g_prod_agg[idx], 0, sizeof(ProductEntry));
    snprintf(g_prod_agg[idx].product, MAX_PRODUCT_LEN, "%s", prod);
    return idx;
}

// --- Data Processing ---
static void parse_and_aggregate(const char *line) {
    char buf[MAX_LINE_LEN];
    strncpy(buf, line, MAX_LINE_LEN - 1);
    buf[MAX_LINE_LEN - 1] = '\0';

    // Strip trailing newlines
    char *nl = strchr(buf, '\n'); if (nl) *nl = '\0';
    char *cr = strchr(buf, '\r'); if (cr) *cr = '\0';
    if (buf[0] == '\0' || buf[0] == '#') return;

    // Skip CSV header
    if (strncmp(buf, "Category,ProductName", 20) == 0) return;

    char *saveptr = NULL;
    char *cat_tok = strtok_r(buf, ",", &saveptr);
    if (!cat_tok || cat_tok[0] == '\0') return;

    char *prod_tok = strtok_r(NULL, ",", &saveptr);
    if (!prod_tok || prod_tok[0] == '\0') return;

    char category[MAX_CATEGORY_LEN], product[MAX_PRODUCT_LEN];
    strncpy(category, cat_tok, MAX_CATEGORY_LEN - 1);
    category[MAX_CATEGORY_LEN - 1] = '\0';
    
    strncpy(product, prod_tok, MAX_PRODUCT_LEN - 1);
    product[MAX_PRODUCT_LEN - 1] = '\0';

    // Sum numeric values
    double revenue = 0.0;
    int has_number = 0;
    char *token;

    while ((token = strtok_r(NULL, ",", &saveptr)) != NULL) {
        char *endptr = NULL;
        double val = strtod(token, &endptr);
        if (endptr && endptr != token && (*endptr == '\0' || *endptr == '\r' || *endptr == '\n')) {
            revenue += val;
            has_number = 1;
        }
    }

    if (!has_number) return;

    // Thread-safe update of the aggregation table
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

// --- Threads ---
typedef struct {
    BoundedQueue *queue;
    int           thread_id;
    uint64_t      rows_done;
} WorkerArg;

static void *worker_fn(void *arg) {
    WorkerArg *wa = (WorkerArg *)arg;
    uint64_t local_rows = 0;

    LOG("Worker thread %d started", wa->thread_id);

    while (1) {
        QueueItem item;
        bq_dequeue(wa->queue, &item);

        if (item.is_poison) {
            LOG("Worker %d exiting (Processed %llu rows)", wa->thread_id, (unsigned long long)local_rows);
            free(item.data); 
            break;
        }

        char *p = item.data;
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
        
        if (start < p && *start != '\0') {
            parse_and_aggregate(start);
            local_rows++;
        }

        free(item.data);
    }

    wa->rows_done = local_rows;
    return NULL;
}

typedef struct {
    int           fifo_fd;
    BoundedQueue *queue;
    int           num_workers;
} ReaderArg;

static void *reader_fn(void *arg) {
    ReaderArg *ra = (ReaderArg *)arg;
    uint32_t chunks_read = 0;

    LOG("Reader thread started");

    while (1) {
        ChunkHeader hdr;
        ssize_t r = read_all(ra->fifo_fd, &hdr, sizeof(hdr));

        if (r == 0) {
            LOG("Reader: FIFO closed unexpectedly");
            break;
        }
        if (r < (ssize_t)sizeof(hdr)) {
            LOG("Reader: Short header read");
            break;
        }
        if (hdr.is_eof) {
            LOG("Reader: EOF sentinel received after %u chunks", chunks_read);
            break;
        }

        char *data = (char *)malloc(hdr.byte_count + 1);
        if (!data) {
            LOG("Reader: Memory allocation failed");
            break;
        }

        r = read_all(ra->fifo_fd, data, hdr.byte_count);
        if (r < (ssize_t)hdr.byte_count) {
            LOG("Reader: Short data read");
            free(data);
            break;
        }
        data[hdr.byte_count] = '\0';

        QueueItem item = {
            .data = data,
            .size = hdr.byte_count,
            .chunk_id = hdr.chunk_id,
            .is_poison = 0
        };

        bq_enqueue(ra->queue, &item);
        chunks_read++;
    }

    // Deploy poison pills to shut down workers cleanly
    for (int i = 0; i < ra->num_workers; i++) {
        QueueItem poison = { NULL, 0, 0, 1 };
        bq_enqueue(ra->queue, &poison);
    }

    return NULL;
}

// --- Main Program ---
int main(int argc, char *argv[]) {
    if (argc < 6) {
        fprintf(stderr, "Usage: processor <fifo_path> <shm_name> <sem_name> <num_threads> <queue_size>\n");
        return EXIT_BAD_ARGS;
    }

    const char *fifo_path   = argv[1];
    const char *shm_name    = argv[2];
    const char *sem_name    = argv[3];
    int         num_threads = atoi(argv[4]);
    int         queue_size  = atoi(argv[5]);

    if (num_threads < 1 || queue_size < 1) {
        fprintf(stderr, "Error: num_threads and queue_size must be >= 1\n");
        return EXIT_BAD_ARGS;
    }

    install_handlers();
    LOG("Processor started. threads=%d, queue=%d", num_threads, queue_size);

    // Open FIFO
    int fifo_fd = open(fifo_path, O_RDONLY);
    if (fifo_fd < 0) {
        LOG("Cannot open FIFO %s: %s", fifo_path, strerror(errno));
        return EXIT_IPC_FAIL;
    }

    BoundedQueue queue;
    if (bq_init(&queue, queue_size) < 0) {
        LOG("Failed to initialize bounded queue");
        close(fifo_fd);
        return EXIT_IPC_FAIL;
    }

    // Setup Worker Threads (2MB Stack Size)
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    pthread_attr_setstacksize(&attr, 2 * 1024 * 1024); 

    pthread_t *worker_threads = (pthread_t *)malloc((size_t)num_threads * sizeof(pthread_t));
    WorkerArg *worker_args    = (WorkerArg *)malloc((size_t)num_threads * sizeof(WorkerArg));
    
    if (!worker_threads || !worker_args) {
        LOG("Failed to allocate memory for threads");
        free(worker_threads); free(worker_args);
        bq_destroy(&queue);
        close(fifo_fd);
        return EXIT_IPC_FAIL;
    }

    for (int i = 0; i < num_threads; i++) {
        worker_args[i].queue = &queue;
        worker_args[i].thread_id = i;
        worker_args[i].rows_done = 0;
        
        if (pthread_create(&worker_threads[i], &attr, worker_fn, &worker_args[i]) != 0) {
            LOG("Failed to create worker thread %d", i);
            // Initiate shutdown sequence for active threads
            for (int j = 0; j < i; j++) {
                QueueItem p = { NULL, 0, 0, 1 };
                bq_enqueue(&queue, &p);
            }
            for (int j = 0; j < i; j++) pthread_join(worker_threads[j], NULL);
            
            pthread_attr_destroy(&attr);
            free(worker_threads); free(worker_args);
            bq_destroy(&queue);
            close(fifo_fd);
            return EXIT_IPC_FAIL;
        }
    }
    pthread_attr_destroy(&attr);

    // Setup Reader Thread
    ReaderArg reader_arg = { fifo_fd, &queue, num_threads };
    pthread_t reader_thread;
    
    if (pthread_create(&reader_thread, NULL, reader_fn, &reader_arg) != 0) {
        LOG("Failed to create reader thread");
        free(worker_threads); free(worker_args);
        bq_destroy(&queue);
        close(fifo_fd);
        return EXIT_IPC_FAIL;
    }

    // Wait for all processing to complete
    pthread_join(reader_thread, NULL);
    for (int i = 0; i < num_threads; i++) {
        pthread_join(worker_threads[i], NULL);
    }

    pthread_mutex_destroy(&g_agg_mutex);

    LOG("Aggregation complete. Categories=%d, Rows=%llu", g_num_cats, (unsigned long long)g_total_rows);

    free(worker_threads);
    free(worker_args);
    bq_destroy(&queue);
    close(fifo_fd);

    // Export Data to Shared Memory
    int shm_fd = shm_open(shm_name, O_RDWR, 0666);
    if (shm_fd < 0) {
        LOG("Failed to open shared memory: %s", strerror(errno));
        return EXIT_IPC_FAIL;
    }

    SharedData *shm = (SharedData *)mmap(NULL, sizeof(SharedData), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    close(shm_fd);
    
    if (shm == MAP_FAILED) {
        LOG("Shared memory mapping failed: %s", strerror(errno));
        return EXIT_IPC_FAIL;
    }

    shm->num_categories = g_num_cats;
    shm->num_products   = g_num_prods;
    shm->total_records  = g_total_rows;
    for (int i = 0; i < g_num_cats; i++)  shm->categories[i] = g_agg[i];
    for (int i = 0; i < g_num_prods; i++) shm->products[i]   = g_prod_agg[i];

    munmap(shm, sizeof(SharedData));
    
    // Alert the reporter via Semaphore
    sem_t *sem_ready = sem_open(sem_name, 0);
    if (sem_ready == SEM_FAILED) {
        LOG("Failed to open semaphore: %s", strerror(errno));
        return EXIT_IPC_FAIL;
    }
    sem_post(sem_ready);
    sem_close(sem_ready);

    LOG("Processor exiting with code %d", (int)g_exit_code);
    return (int)g_exit_code;
}