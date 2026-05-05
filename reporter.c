/*
 * reporter.c
 *
 * Stage 3 of the OS Pipeline.
 * Waits for the processor to finish, reads aggregated data from shared memory,
 * sorts it, and generates final CSV and text reports.
 */

#include "common/common.h"

// --- Sorting Helpers ---
static int compare_revenue(const void *a, const void *b) {
    const CategoryEntry *ca = (const CategoryEntry *)a;
    const CategoryEntry *cb = (const CategoryEntry *)b;
    if (ca->total_revenue < cb->total_revenue) return 1;
    if (ca->total_revenue > cb->total_revenue) return -1;
    return 0;
}

static int compare_prod_revenue(const void *a, const void *b) {
    const ProductEntry *pa = (const ProductEntry *)a;
    const ProductEntry *pb = (const ProductEntry *)b;
    if (pa->total_revenue < pb->total_revenue) return 1;
    if (pa->total_revenue > pb->total_revenue) return -1;
    return 0;
}

// --- Main Program ---
int main(int argc, char *argv[]) {
    if (argc < 4) {
        fprintf(stderr, "Usage: reporter <shm_name> <sem_name> <output_dir>\n");
        return EXIT_BAD_ARGS;
    }

    const char *shm_name   = argv[1];
    const char *sem_name   = argv[2];
    const char *output_dir = argv[3];

    LOG("Reporter started. Waiting for data on semaphore: %s", sem_name);

    // --- Wait for Processor ---
    sem_t *sem_ready = sem_open(sem_name, 0);
    if (sem_ready == SEM_FAILED) {
        LOG("Failed to open semaphore: %s", strerror(errno));
        return EXIT_IPC_FAIL;
    }
    
    // Block until the processor posts to this semaphore
    sem_wait(sem_ready);
    sem_close(sem_ready);
    LOG("Data is ready. Accessing shared memory...");

    // --- Read from Shared Memory ---
    int shm_fd = shm_open(shm_name, O_RDONLY, 0666);
    if (shm_fd < 0) {
        LOG("Failed to open shared memory");
        return EXIT_IPC_FAIL;
    }

    SharedData *shm = mmap(NULL, sizeof(SharedData), PROT_READ, MAP_SHARED, shm_fd, 0);
    close(shm_fd);
    if (shm == MAP_FAILED) {
        LOG("Memory mapping failed");
        return EXIT_IPC_FAIL;
    }

    // Make local copies for sorting to avoid modifying the shared state
    int num_cats = shm->num_categories;
    CategoryEntry categories[MAX_CATEGORIES];
    memcpy(categories, shm->categories, num_cats * sizeof(CategoryEntry));
    qsort(categories, num_cats, sizeof(CategoryEntry), compare_revenue);

    int num_prods = shm->num_products;
    ProductEntry products[MAX_PRODUCTS];
    memcpy(products, shm->products, num_prods * sizeof(ProductEntry));
    qsort(products, num_prods, sizeof(ProductEntry), compare_prod_revenue);

    // --- Generate CSV Report ---
    char csv_path[MAX_PATH_LEN];
    snprintf(csv_path, sizeof(csv_path), "%s/report.csv", output_dir);
    
    FILE *f_csv = fopen(csv_path, "w");
    if (f_csv) {
        fprintf(f_csv, "Category,Total_Revenue,Record_Count,Max_Single,Min_Single\n");
        for (int i = 0; i < num_cats; i++) {
            fprintf(f_csv, "%s,%.2f,%llu,%.2f,%.2f\n",
                    categories[i].category, 
                    categories[i].total_revenue,
                    (unsigned long long)categories[i].record_count,
                    categories[i].max_single, 
                    categories[i].min_single);
        }
        fclose(f_csv);
        LOG("Saved CSV report to %s", csv_path);
    } else {
        LOG("Failed to create CSV report file");
    }

    // --- Generate Text Report (using dup2 redirection) ---
    char txt_path[MAX_PATH_LEN];
    snprintf(txt_path, sizeof(txt_path), "%s/report.txt", output_dir);
    
    int txt_fd = open(txt_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (txt_fd >= 0) {
        // Save the original standard output file descriptor
        int saved_stdout = dup(STDOUT_FILENO);
        
        // Redirect standard output into the text file
        dup2(txt_fd, STDOUT_FILENO);
        close(txt_fd);

        // Standard printf calls now write directly to report.txt
        time_t now = time(NULL);
        char *t_str = ctime(&now);
        t_str[strlen(t_str)-1] = '\0'; // remove newline

        printf("============================================================\n");
        printf("             PARALLEL DATA PIPELINE AUDIT REPORT            \n");
        printf("============================================================\n");
        printf(" TIMESTAMP : %s\n", t_str);
        printf(" STATUS    : SUCCESS\n");
        printf("============================================================\n\n");

        printf("1. PIPELINE METRICS\n");
        printf("------------------------------------------------------------\n");
        printf("  Total Rows Processed : %llu\n", (unsigned long long)shm->total_records);
        printf("  Unique Categories    : %d\n", num_cats);
        printf("  Unique Products      : %d\n\n", num_prods);
        
        printf("2. REVENUE ANALYSIS BY CATEGORY\n");
        printf("------------------------------------------------------------\n");
        int print_count = (num_cats < TOP_N_REPORT) ? num_cats : TOP_N_REPORT;
        for (int i = 0; i < print_count; i++) {
            printf("  (%d) %-15s : $ %10.2f [ %llu rows ]\n", 
                   i + 1, categories[i].category, categories[i].total_revenue, 
                   (unsigned long long)categories[i].record_count);
        }
        
        printf("\n3. TOP PERFORMING PRODUCTS\n");
        printf("------------------------------------------------------------\n");
        int prod_print_count = (num_prods < TOP_N_REPORT) ? num_prods : TOP_N_REPORT;
        for (int i = 0; i < prod_print_count; i++) {
            printf("  [#%d] %-15s : $ %10.2f\n", 
                   i + 1, products[i].product, products[i].total_revenue);
        }
        printf("\n============================================================\n");
        fflush(stdout);

        // Restore standard output back to the terminal
        dup2(saved_stdout, STDOUT_FILENO);
        close(saved_stdout);
        LOG("Saved Text report to %s", txt_path);
    } else {
        LOG("Failed to create Text report file");
    }

    // --- Cleanup & Signal Parent ---
    munmap(shm, sizeof(SharedData));
    
    LOG("All reports generated successfully. Notifying dispatcher...");
    
    // Send a signal back to the dispatcher indicating completion
    kill(getppid(), SIGUSR1); 

    return EXIT_OK;
}