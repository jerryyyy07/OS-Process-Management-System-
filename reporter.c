/* ==================================================================
 * reporter.c  –  Stage 3 of the OS Pipeline.
 * ================================================================== */
#include "common/common.h"

// Helper function to sort categories by revenue descending
int compare_revenue(const void *a, const void *b) {
    const CategoryEntry *ca = (const CategoryEntry *)a;
    const CategoryEntry *cb = (const CategoryEntry *)b;
    if (ca->total_revenue < cb->total_revenue) return 1;
    if (ca->total_revenue > cb->total_revenue) return -1;
    return 0;
}

// Helper function to sort products by revenue descending
int compare_prod_revenue(const void *a, const void *b) {
    const ProductEntry *pa = (const ProductEntry *)a;
    const ProductEntry *pb = (const ProductEntry *)b;
    if (pa->total_revenue < pb->total_revenue) return 1;
    if (pa->total_revenue > pb->total_revenue) return -1;
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        fprintf(stderr, "Usage: reporter <shm_name> <sem_name> <output_dir>\n");
        return EXIT_BAD_ARGS;
    }

    const char *shm_name   = argv[1];
    const char *sem_name   = argv[2];
    const char *output_dir = argv[3];

    LOG("Reporter started. Waiting on semaphore %s...", sem_name);

    /* ---- 1. Wait on the named semaphore (No busy-waiting) ---- */
    sem_t *sem_ready = sem_open(sem_name, 0);
    if (sem_ready == SEM_FAILED) {
        LOG("Reporter failed to open semaphore: %s", strerror(errno));
        return EXIT_IPC_FAIL;
    }
    sem_wait(sem_ready);
    sem_close(sem_ready);
    LOG("Semaphore unblocked. Data is ready.");

    /* ---- 2. Read from Shared Memory ---- */
    int shm_fd = shm_open(shm_name, O_RDONLY, 0666);
    if (shm_fd < 0) return EXIT_IPC_FAIL;

    SharedData *shm = mmap(NULL, sizeof(SharedData), PROT_READ, MAP_SHARED, shm_fd, 0);
    close(shm_fd);
    if (shm == MAP_FAILED) return EXIT_IPC_FAIL;

    /* Copy data locally to sort it without modifying shared memory */
    int num_cats = shm->num_categories;
    CategoryEntry entries[MAX_CATEGORIES];
    memcpy(entries, shm->categories, num_cats * sizeof(CategoryEntry));
    qsort(entries, num_cats, sizeof(CategoryEntry), compare_revenue);

    int num_prods = shm->num_products;
    ProductEntry prod_entries[MAX_PRODUCTS];
    memcpy(prod_entries, shm->products, num_prods * sizeof(ProductEntry));
    qsort(prod_entries, num_prods, sizeof(ProductEntry), compare_prod_revenue);

    /* ---- 3. Write report.csv (Machine-readable) ---- */
    char csv_path[MAX_PATH_LEN];
    snprintf(csv_path, sizeof(csv_path), "%s/report.csv", output_dir);
    FILE *f_csv = fopen(csv_path, "w");
    if (f_csv) {
        fprintf(f_csv, "Category,Total_Revenue,Record_Count,Max_Single,Min_Single\n");
        for (int i = 0; i < num_cats; i++) {
            fprintf(f_csv, "%s,%.2f,%llu,%.2f,%.2f\n",
                    entries[i].category, entries[i].total_revenue,
                    (unsigned long long)entries[i].record_count,
                    entries[i].max_single, entries[i].min_single);
        }
        fclose(f_csv);
    }

    /* ---- 4. Write report.txt using dup()/dup2() demonstration ---- */
    char txt_path[MAX_PATH_LEN];
    snprintf(txt_path, sizeof(txt_path), "%s/report.txt", output_dir);
    
    int txt_fd = open(txt_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (txt_fd >= 0) {
        /* Save current stdout */
        int saved_stdout = dup(STDOUT_FILENO);
        
        /* Redirect stdout to report.txt */
        dup2(txt_fd, STDOUT_FILENO);
        close(txt_fd);

        /* Print report using standard printf */
        printf("========================================\n");
        printf(" RETAIL TRANSACTIONS REPORT (TOP %d)\n", TOP_N_REPORT);
        printf("========================================\n");
        printf("Total Records Processed: %llu\n", (unsigned long long)shm->total_records);
        printf("Distinct Categories: %d\n", num_cats);
        printf("Distinct Products: %d\n\n", num_prods);
        
        printf("--- TOP CATEGORIES ---\n");
        int print_count = (num_cats < TOP_N_REPORT) ? num_cats : TOP_N_REPORT;
        for (int i = 0; i < print_count; i++) {
            printf("%d. %-20s | Revenue: $%.2f (Rows: %llu)\n", 
                   i+1, entries[i].category, entries[i].total_revenue, 
                   (unsigned long long)entries[i].record_count);
        }
        
        printf("\n--- TOP PRODUCTS ---\n");
        int prod_print_count = (num_prods < TOP_N_REPORT) ? num_prods : TOP_N_REPORT;
        for (int i = 0; i < prod_print_count; i++) {
            printf("%d. %-20s | Revenue: $%.2f\n", 
                   i+1, prod_entries[i].product, prod_entries[i].total_revenue);
        }
        printf("========================================\n");
        fflush(stdout);

        /* Restore stdout */
        dup2(saved_stdout, STDOUT_FILENO);
        close(saved_stdout);
    }

    /* ---- 5. Cleanup and Signal Parent ---- */
    munmap(shm, sizeof(SharedData));
    
    LOG("Reports generated in %s. Signaling dispatcher...", output_dir);
    kill(getppid(), SIGUSR1); /* Signal dispatcher that report is ready */

    return EXIT_OK;
}