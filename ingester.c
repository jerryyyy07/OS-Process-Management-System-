/* ==================================================================
 * ingester.c  –  Stage 1 of the OS Pipeline.
 *
 * Responsibilities:
 *   1. Scan the input directory for *.csv files.
 *   2. Open the named FIFO for writing (blocks until processor opens
 *      its read end – this is the intended synchronisation point).
 *   3. For each CSV file, accumulate up to CHUNK_ROWS lines into a
 *      buffer, then write a ChunkHeader + raw CSV text into the FIFO.
 *   4. After all files are processed, write a single EOF sentinel
 *      chunk (is_eof = 1, byte_count = 0).
 *   5. Handle SIGTERM gracefully: flush any pending chunk, write EOF,
 *      and exit with EXIT_SIGTERM_CODE.
 *   6. Handle SIGUSR1: print live statistics to stderr.
 *
 * Usage (launched by dispatcher):
 *   ingester <input_dir> <fifo_path>
 * ================================================================== */

#include "common/common.h"

/* ------------------------------------------------------------------ *
 * Live statistics (updated in main; printed on SIGUSR1)              *
 * ------------------------------------------------------------------ */
static volatile sig_atomic_t g_do_stats   = 0;
static volatile sig_atomic_t g_shutdown   = 0;
static volatile sig_atomic_t g_exit_code  = EXIT_OK;

static unsigned long g_files_processed = 0;
static unsigned long g_chunks_sent     = 0;
static unsigned long g_bytes_sent      = 0;

static void handle_sigterm(int s) {
    (void)s;
    g_shutdown  = 1;
    g_exit_code = EXIT_SIGTERM_CODE;
}
static void handle_sigusr1(int s) { (void)s; g_do_stats = 1; }

static void install_handlers(void)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sigemptyset(&sa.sa_mask);

    sa.sa_handler = handle_sigterm;
    sa.sa_flags   = SA_RESTART;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);

    sa.sa_handler = handle_sigusr1;
    sa.sa_flags   = SA_RESTART;
    sigaction(SIGUSR1, &sa, NULL);
}

/* ------------------------------------------------------------------ *
 * write_chunk – prepend ChunkHeader and send raw CSV text to FIFO    *
 * ------------------------------------------------------------------ */
static int write_chunk(int fifo_fd, const char *buf, size_t len,
                        uint32_t chunk_id, uint32_t file_id)
{
    ChunkHeader hdr;
    hdr.chunk_id       = chunk_id;
    hdr.byte_count     = (uint32_t)len;
    hdr.source_file_id = file_id;
    hdr.is_eof         = 0;

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

/* ------------------------------------------------------------------ *
 * send_eof – write the EOF sentinel chunk to signal end-of-stream    *
 * ------------------------------------------------------------------ */
static void send_eof(int fifo_fd)
{
    ChunkHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.is_eof = 1;
    if (write_all(fifo_fd, &hdr, sizeof(hdr)) != (ssize_t)sizeof(hdr))
        LOG("send_eof: write failed – processor may hang");
    else
        LOG("EOF sentinel sent");
}

/* ------------------------------------------------------------------ *
 * process_csv_file – read one CSV file and write chunks to FIFO      *
 * ------------------------------------------------------------------ */
static int process_csv_file(int fifo_fd, const char *path,
                             uint32_t file_id, uint32_t *chunk_id)
{
    FILE *fp = fopen(path, "r");
    if (!fp) {
        LOG("Cannot open %s: %s", path, strerror(errno));
        return -1;
    }

    /* Allocate chunk buffer: CHUNK_ROWS × MAX_LINE_LEN bytes max */
    size_t buf_cap = (size_t)CHUNK_ROWS * MAX_LINE_LEN;
    char  *buf     = (char *)malloc(buf_cap);
    if (!buf) {
        fclose(fp);
        return -1;
    }

    char   line[MAX_LINE_LEN];
    int    row_count = 0;
    size_t buf_used  = 0;

    while (!g_shutdown && fgets(line, sizeof(line), fp)) {
        size_t len = strlen(line);

        /* Skip blank or comment lines */
        if (len == 0 || line[0] == '\n' || line[0] == '#')
            continue;

        /* Ensure line ends with newline for the processor's line parser */
        if (line[len - 1] != '\n' && (buf_used + len + 1) < buf_cap) {
            line[len]     = '\n';
            line[len + 1] = '\0';
            len++;
        }

        /* Copy into chunk buffer */
        if (buf_used + len >= buf_cap) {
            /* Buffer full: flush this chunk */
            if (write_chunk(fifo_fd, buf, buf_used, (*chunk_id)++, file_id) < 0)
                goto err;
            buf_used  = 0;
            row_count = 0;
        }

        memcpy(buf + buf_used, line, len);
        buf_used += len;
        row_count++;

        if (row_count >= CHUNK_ROWS) {
            if (write_chunk(fifo_fd, buf, buf_used, (*chunk_id)++, file_id) < 0)
                goto err;
            buf_used  = 0;
            row_count = 0;
        }

        /* Print stats if SIGUSR1 was received */
        if (g_do_stats) {
            g_do_stats = 0;
            LOG("STATS: files_processed=%lu chunks_sent=%lu bytes_sent=%lu",
                g_files_processed, g_chunks_sent, g_bytes_sent);
        }
    }

    /* Flush remaining rows */
    if (buf_used > 0 && !g_shutdown) {
        if (write_chunk(fifo_fd, buf, buf_used, (*chunk_id)++, file_id) < 0)
            goto err;
    }

    free(buf);
    fclose(fp);
    g_files_processed++;
    LOG("Finished file: %s (%lu chunks total so far)", path, g_chunks_sent);
    return 0;

err:
    free(buf);
    fclose(fp);
    return -1;
}

/* ================================================================== *
 * main                                                                *
 * ================================================================== */
int main(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: ingester <input_dir> <fifo_path>\n");
        return EXIT_BAD_ARGS;
    }

    const char *input_dir = argv[1];
    const char *fifo_path = argv[2];

    install_handlers();
    LOG("Ingester started. input_dir=%s fifo=%s", input_dir, fifo_path);

    /* ---- Collect *.csv files from input directory ---- */
    DIR *dir = opendir(input_dir);
    if (!dir) {
        LOG("Cannot open input directory %s: %s", input_dir, strerror(errno));
        return EXIT_IO_ERROR;
    }

    /* Build sorted list of CSV file paths */
    char  *csv_files[1024];
    int    nfiles = 0;
    struct dirent *ent;

    while ((ent = readdir(dir)) != NULL) {
        const char *name = ent->d_name;
        size_t      nlen = strlen(name);
        if (nlen > 4 && strcmp(name + nlen - 4, ".csv") == 0) {
            char path[MAX_PATH_LEN];
            snprintf(path, sizeof(path), "%s/%s", input_dir, name);
            csv_files[nfiles] = strdup(path);
            if (csv_files[nfiles]) nfiles++;
            if (nfiles >= 1024) break;
        }
    }
    closedir(dir);

    if (nfiles == 0) {
        LOG("No *.csv files found in %s", input_dir);
        /* Still need to open FIFO and send EOF so processor can finish */
    }
    LOG("Found %d CSV file(s)", nfiles);

    /* ---- Open FIFO for writing (blocks until processor opens read end) ---- */
    LOG("Opening FIFO for writing (will block until processor connects)…");
    int fifo_fd = open(fifo_path, O_WRONLY);
    if (fifo_fd < 0) {
        LOG("Cannot open FIFO %s for writing: %s", fifo_path, strerror(errno));
        for (int i = 0; i < nfiles; i++) free(csv_files[i]);
        return EXIT_IPC_FAIL;
    }
    LOG("FIFO opened for writing");

    /* ---- Process each CSV file ---- */
    uint32_t chunk_id = 0;
    for (int i = 0; i < nfiles && !g_shutdown; i++) {
        LOG("Processing file %d/%d: %s", i + 1, nfiles, csv_files[i]);
        process_csv_file(fifo_fd, csv_files[i], (uint32_t)i, &chunk_id);
        free(csv_files[i]);
    }
    /* Free any remaining paths on early shutdown */
    for (int i = 0; i < nfiles; i++) if (csv_files[i]) free(csv_files[i]);

    /* ---- Send EOF sentinel so processor terminates cleanly ---- */
    send_eof(fifo_fd);

    close(fifo_fd);
    LOG("Ingester done. files=%lu chunks=%lu bytes=%lu",
        g_files_processed, g_chunks_sent, g_bytes_sent);

    return (int)g_exit_code;
}
