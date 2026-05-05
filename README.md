# OS Process Management System

A multi-process, multi-threaded data pipeline written in C. This project demonstrates core OS concepts like concurrency and inter-process communication (IPC) in a practical way.

## How It Works

The system is broken down into four main parts that work together:

- **Dispatcher**: The boss. It sets up the IPC resources (shared memory, semaphores, and FIFOs) and spins up the other child processes.
- **Ingester**: Reads your raw input CSV files and feeds the data into a named pipe (FIFO).
- **Processor**: The heavy lifter. It pulls data from the FIFO and uses a pool of worker threads and a bounded queue to process everything, safely dropping the results into shared memory.
- **Reporter**: Picks up the finished data from shared memory and generates the final output reports.

## Getting Started

Make sure you have `gcc` and `make` installed on your system.

### Building
You can compile everything manually using the included Makefile:
```bash
make
```
*(Use `make clean` to wipe out the compiled binaries).*

### Running the Pipeline
The easiest way to run it is with the provided `run.sh` script.

**Usage:**
```bash
./run.sh -i <input_dir> -o <output_dir> -n <num_threads> -q <queue_size> [-c]
```

- `-i`: Where your input `.csv` files live.
- `-o`: Where you want the final reports to go.
- `-n`: How many worker threads the processor should use.
- `-q`: The size of the processor's task queue.
- `-c`: (Optional) Do a fresh `make clean && make` before running.

**Quick Example:**
```bash
# Runs the pipeline with 4 threads and a queue size of 10
./run.sh -i ./input_data -o ./reports -n 4 -q 10 -c
```

Once running, the dispatcher will give you a neat summary of the pipeline's status and total latency when it wraps up.
