import decentdb
import datetime
import random
import string
import psutil
import time
import gc
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from rich.table import Table
from rich.live import Live

# Constants
DB_PATH = "test_memory_leak.ddb"
NUM_CYCLES = 100  # Number of test cycles to run
NUM_OPERATIONS_PER_CYCLE = 10  # Operations per cycle
NUM_RECORDS = 100  # Records to insert/query per operation

console = Console()

def generate_random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def perform_database_cycle(conn, cycle_num, progress):
    task = progress.add_task(f"[cyan]Cycle {cycle_num}: Performing operations...", total=8)
    
    # Step 1: Create table
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS test_table (
            id INT64 PRIMARY KEY,
            name TEXT NOT NULL,
            value FLOAT64,
            created_at TIMESTAMP
        )
    """)
    conn.commit()
    progress.advance(task)

    # Step 2: Insert records
    insert_task = progress.add_task("[green]Inserting records...", total=NUM_RECORDS)
    for i in range(NUM_RECORDS):
        cur.execute(
            "INSERT INTO test_table VALUES (?, ?, ?, ?)",
            (i, generate_random_string(), random.uniform(0, 1000), datetime.datetime.now())
        )
        progress.advance(insert_task)
    conn.commit()
    progress.advance(task)

    # Step 3: Query records
    cur.execute("SELECT * FROM test_table LIMIT ?", (NUM_RECORDS,))
    rows = cur.fetchall()
    progress.advance(task)

    # Step 4: Create index
    cur.execute("CREATE INDEX IF NOT EXISTS idx_test_name ON test_table(name)")
    conn.commit()
    progress.advance(task)

    # Step 5: Insert more records with index
    for i in range(NUM_RECORDS, NUM_RECORDS * 2):
        cur.execute(
            "INSERT INTO test_table VALUES (?, ?, ?, ?)",
            (i, generate_random_string(), random.uniform(0, 1000), datetime.datetime.now())
        )
    conn.commit()
    progress.advance(task)

    # Step 6: Query with index
    cur.execute("SELECT * FROM test_table WHERE name = ?", (rows[0][1],))
    indexed_rows = cur.fetchall()
    progress.advance(task)

    # Step 7: Delete index
    cur.execute("DROP INDEX IF EXISTS idx_test_name")
    conn.commit()
    progress.advance(task)

    # Step 8: Delete table
    cur.execute("DROP TABLE IF EXISTS test_table")
    conn.commit()
    progress.advance(task)

    cur.close()

def get_memory_usage():
    process = psutil.Process()
    mem_info = process.memory_info()
    return mem_info.rss / (1024 * 1024)  # RSS in MB

def main():
    console.print("[bold green]Starting DecentDB Memory Leak Test[/bold green]")
    console.print(f"Running {NUM_CYCLES} cycles with {NUM_OPERATIONS_PER_CYCLE} operations each")
    console.print(f"Inserting/Quering {NUM_RECORDS} records per operation\n")

    initial_memory = get_memory_usage()
    memory_table = Table(title="Memory Usage Tracker")
    memory_table.add_column("Cycle", style="cyan")
    memory_table.add_column("Memory (MB)", style="magenta")
    memory_table.add_column("Delta (MB)", style="yellow")
    memory_table.add_column("Time (s)", style="green")

    with Live(memory_table, refresh_per_second=4, console=console) as live:
        last_memory = initial_memory
        start_time = time.time()

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
            transient=True
        ) as progress:
            cycle_task = progress.add_task("[bold]Overall Progress", total=NUM_CYCLES)

            for cycle in range(1, NUM_CYCLES + 1):
                # Open connection
                conn = decentdb.connect(DB_PATH, mode="open_or_create")
                
                perform_database_cycle(conn, cycle, progress)
                
                # Close connection
                conn.close()
                
                # Force garbage collection
                gc.collect()
                
                # Get current memory
                current_memory = get_memory_usage()
                delta = current_memory - last_memory
                elapsed = time.time() - start_time
                
                # Add to table
                memory_table.add_row(
                    str(cycle),
                    f"{current_memory:.2f}",
                    f"{delta:+.2f}",
                    f"{elapsed:.2f}"
                )
                
                last_memory = current_memory
                progress.advance(cycle_task)

                # Optional: Add sleep to observe
                time.sleep(0.1)

    final_memory = get_memory_usage()
    total_delta = final_memory - initial_memory
    
    console.print("\n[bold]Test Completed[/bold]")
    console.print(f"Initial Memory: {initial_memory:.2f} MB")
    console.print(f"Final Memory: {final_memory:.2f} MB")
    console.print(f"Total Delta: {total_delta:+.2f} MB")
    
    if abs(total_delta) < 1.0:  # Arbitrary threshold for "no leak"
        console.print("[bold green]No significant memory leak detected![/bold green]")
    else:
        console.print("[bold red]Potential memory leak detected! Check deltas.[/bold red]")

if __name__ == "__main__":
    main()
