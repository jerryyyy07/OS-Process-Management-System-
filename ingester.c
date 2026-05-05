/*
 * ingester.c
 *
 * Stage 1 of the OS Pipeline.
 * Scans the input directory for CSV files, chunks the data, 
 * and writes it to a named FIFO for the processor.
 */

#include "common/common.h"

// --- Global Signal & State Flags ---
static volatile sig_atomic_t g_do_stats  = 0;
static volatile sig_atomic_t g_shutdown  = 0;
static volatile sig_atomic_t g_exit_code = EXIT_OK;

// --- Statistics ---
static unsigned long g_files_processed = 0;
static unsigned long g_chunks_sent     = 0;
static unsigned long g_bytes_sent      = 0;

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

// --- Chunking & Transmission ---
static int write_chunk(int fifo_fd, const char *buf, size_t len, uint32_t chunk_id, uint32_t file_id) {
    ChunkHeader hdr = {
        .chunk_id = chunk_id,
        .byte_count = (uint32_t)len,
        .source_file_id = file_id,
        .is_eof = 0
    };

    if (write_all(fifo_fd, &hdr, sizeof(hdr)) != (ssize_t)sizeof(hdr)) {
        LOG("write_chunk: failed writing header");
        return -1;
    }
    if (write_all(fifo_fd, buf, len) != (ssize_t)len) {
        LOG("write_chunk: failed writing data");
        return -1;
    }

    g_chunks_sent++;
    g_bytes_sent += (unsigned long)len;
    LOG("Sent chunk #%u from file_id=%u (%zu bytes)", chunk_id, file_id, len);
    
    return 0;
}

static void send_eof(int fifo_fd) {
    ChunkHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.is_eof = 1;
    
    if (write_all(fifo_fd, &hdr, sizeof(hdr)) != (ssize_t)sizeof(hdr)) {
        LOG("send_eof: write failed - processor may hang");
    } else {
        LOG("EOF sentinel sent");
    }
}

// --- File Processing ---
static int process_csv_file(int fifo_fd, const char *path, uint32_t file_id, uint32_t *chunk_id) {
    FILE *fp = fopen(path, "r");
    if (!fp) {
        LOG("Cannot open %s: %s", path, strerror(errno));
        return -1;
    }

    size_t buf_cap = (size_t)CHUNK_ROWS * MAX_LINE_LEN;
    char *buf = (char *)malloc(buf_cap);
    if (!buf) {
        fclose(fp);
        return -1;
    }

    char line[MAX_LINE_LEN];
    int row_count = 0;
    size_t buf_used = 0;

    while (!g_shutdown && fgets(line, sizeof(line), fp)) {
        size_t len = strlen(line);

        // Skip blank or comment lines
        if (len == 0 || line[0] == '\n' || line[0] == '#') continue;

        // Ensure trailing newline
        if (line[len - 1] != '\n' && (buf_used + len + 1) < buf_cap) {
            line[len] = '\n';
            line[len + 1] = '\0';
            len++;
        }

        // Flush chunk if buffer is full
        if (buf_used + len >= buf_cap) {
            if (write_chunk(fifo_fd, buf, buf_used, (*chunk_id)++, file_id) < 0) {
                free(buf);
                fclose(fp);
                return -1;
            }
            buf_used = 0;
            row_count = 0;
        }

        memcpy(buf + buf_used, line, len);
        buf_used += len;
        row_count++;

        // Flush chunk if row limit reached
        if (row_count >= CHUNK_ROWS) {
            if (write_chunk(fifo_fd, buf, buf_used, (*chunk_id)++, file_id) < 0) {
                free(buf);
                fclose(fp);
                return -1;
            }
            buf_used = 0;
            row_count = 0;
        }

        // Print stats on SIGUSR1
        if (g_do_stats) {
            g_do_stats = 0;
            LOG("STATS: files_processed=%lu chunks_sent=%lu bytes_sent=%lu",
                g_files_processed, g_chunks_sent, g_bytes_sent);
        }
    }

    // Flush remaining rows
    if (buf_used > 0 && !g_shutdown) {
        if (write_chunk(fifo_fd, buf, buf_used, (*chunk_id)++, file_id) < 0) {
            free(buf);
            fclose(fp);
            return -1;
        }
    }

    free(buf);
    fclose(fp);
    g_files_processed++;
    LOG("Finished file: %s (%lu chunks total so far)", path, g_chunks_sent);
    
    return 0;
}

// --- Main Program ---
int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: ingester <input_dir> <fifo_path>\n");
        return EXIT_BAD_ARGS;
    }

    const char *input_dir = argv[1];
    const char *fifo_path = argv[2];

    install_handlers();
    LOG("Ingester started. input_dir=%s fifo=%s", input_dir, fifo_path);

    // Scan for CSV files
    DIR *dir = opendir(input_dir);
    if (!dir) {
        LOG("Cannot open input directory %s: %s", input_dir, strerror(errno));
        return EXIT_IO_ERROR;
    }

    char *csv_files[1024];
    int nfiles = 0;
    struct dirent *ent;

    while ((ent = readdir(dir)) != NULL) {
        const char *name = ent->d_name;
        size_t nlen = strlen(name);
        if (nlen > 4 && strcmp(name + nlen - 4, ".csv") == 0) {
            char path[MAX_PATH_LEN];
            snprintf(path, sizeof(path), "%s/%s", input_dir, name);
            csv_files[nfiles] = strdup(path);
            if (csv_files[nfiles]) nfiles++;
            if (nfiles >= 1024) break;
        }
    }
    closedir(dir);

    LOG("Found %d CSV file(s)", nfiles);

    // Open FIFO (Blocks until processor is ready)
    LOG("Opening FIFO for writing (blocking until processor connects)...");
    int fifo_fd = open(fifo_path, O_WRONLY);
    if (fifo_fd < 0) {
        LOG("Cannot open FIFO %s for writing: %s", fifo_path, strerror(errno));
        for (int i = 0; i < nfiles; i++) free(csv_files[i]);
        return EXIT_IPC_FAIL;
    }
    LOG("FIFO opened for writing");

    // Process Files
    uint32_t chunk_id = 0;
    for (int i = 0; i < nfiles && !g_shutdown; i++) {
        LOG("Processing file %d/%d: %s", i + 1, nfiles, csv_files[i]);
        process_csv_file(fifo_fd, csv_files[i], (uint32_t)i, &chunk_id);
    }

    // Cleanup & Exit
    for (int i = 0; i < nfiles; i++) {
        if (csv_files[i]) free(csv_files[i]);
    }

    send_eof(fifo_fd);
    close(fifo_fd);

    LOG("Ingester done. files=%lu chunks=%lu bytes=%lu", g_files_processed, g_chunks_sent, g_bytes_sent);
    return (int)g_exit_code;
}