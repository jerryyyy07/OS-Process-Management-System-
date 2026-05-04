#ifndef COMMON_H
#define COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <semaphore.h>
#include <pthread.h>
#include <signal.h>
#include <errno.h>
#include <time.h>
#include <dirent.h>
#include <stdint.h>

#define MAX_PATH_LEN 512
#define MAX_LINE_LEN 1024
#define MAX_CATEGORY_LEN 64
#define MAX_PRODUCT_LEN 64
#define MAX_CATEGORIES 100
#define MAX_PRODUCTS 1000
#define TOP_N_REPORT 5
#define CHUNK_ROWS 1000

/* Standard Exit Codes */
#define EXIT_OK 0
#define EXIT_BAD_ARGS 10
#define EXIT_IPC_FAIL 20
#define EXIT_CHILD_DIED 30
#define EXIT_IO_ERROR 40
#define EXIT_SIGINT_CODE 130
#define EXIT_SIGTERM_CODE 143

/* Custom Logging Macro */
#define LOG(fmt, ...) do { \
    time_t now = time(NULL); \
    struct tm *t = localtime(&now); \
    fprintf(stderr, "[%02d:%02d:%02d] [PID:%d PPID:%d] " fmt "\n", \
            t->tm_hour, t->tm_min, t->tm_sec, \
            getpid(), getppid(), ##__VA_ARGS__); \
} while(0)

/* IPC Structures */

typedef struct {
    uint32_t chunk_id;
    uint32_t byte_count;
    uint32_t source_file_id;
    int      is_eof;
} ChunkHeader;

typedef struct {
    char   category[MAX_CATEGORY_LEN];
    double total_revenue;
    uint64_t record_count;
    double max_single;
    double min_single;
} CategoryEntry;

typedef struct {
    char   product[MAX_PRODUCT_LEN];
    double total_revenue;
} ProductEntry;

typedef struct {
    int num_categories;
    int num_products;
    uint64_t total_records;
    CategoryEntry categories[MAX_CATEGORIES];
    ProductEntry products[MAX_PRODUCTS];
} SharedData;

/* Queue item for processor threads */
typedef struct {
    char    *data;
    size_t   size;
    uint32_t chunk_id;
    int      is_poison;
} QueueItem;

/* Helper functions for robust I/O */
static inline ssize_t read_all(int fd, void *buf, size_t count) {
    size_t left = count;
    char *ptr = (char*)buf;
    while (left > 0) {
        ssize_t res = read(fd, ptr, left);
        if (res < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (res == 0) break; /* EOF */
        left -= res;
        ptr += res;
    }
    return count - left;
}

static inline ssize_t write_all(int fd, const void *buf, size_t count) {
    size_t left = count;
    const char *ptr = (const char*)buf;
    while (left > 0) {
        ssize_t res = write(fd, ptr, left);
        if (res < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (res == 0) break; /* Should not happen for write */
        left -= res;
        ptr += res;
    }
    return count - left;
}

#endif /* COMMON_H */
