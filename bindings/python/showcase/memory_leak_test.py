import os
import time
import psutil
import tempfile
import string
import random
import decentdb
from collections import deque
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
from rich.console import Group

def get_memory_mb():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)

def generate_random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def create_layout():
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="main"),
        Layout(name="footer", size=3)
    )
    return layout

def main(iterations=2000, records_per_txn=100):
    records_per_iter = records_per_txn // 2 if records_per_txn > 1 else 1 # we insert twice per iteration
    history_size = 20
    
    start_memory = get_memory_mb()
    stats_history = deque(maxlen=history_size)
    
    # Track overall stats for leak detection
    warmup_iterations = 100
    warmup_memory = None
    max_memory = start_memory
    
    # Progress Bar
    progress = Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        expand=True
    )
    task_id = progress.add_task("[cyan]Running memory leak test...", total=iterations)
    
    db_path = os.path.join(tempfile.gettempdir(), f"decentdb_memtest_{int(time.time())}.ddb")
    start_time_total = time.time()
    
    def generate_table():
        table = Table(title="Live Performance & Memory Stats", expand=True)
        table.add_column("Iteration", justify="right", style="cyan")
        table.add_column("Current Mem (MB)", justify="right", style="magenta")
        table.add_column("Mem Delta (MB)", justify="right", style="red")
        table.add_column("Iter Time (ms)", justify="right", style="green")
        table.add_column("Status", style="blue")
        
        for row in stats_history:
            table.add_row(
                str(row["iter"]),
                f"{row['mem_mb']:.2f}",
                f"{row['mem_delta']:.2f}",
                f"{row['iter_time_ms']:.1f}",
                row["status"]
            )
        return table

    layout = create_layout()
    layout["header"].update(Panel(f"[bold white]DecentDB Engine & Bindings Memory Leak Test[/bold white] (DB: {db_path})", style="on blue"))
    layout["footer"].update(Panel(progress, style="on black"))
    layout["main"].update(generate_table())

    with Live(layout, refresh_per_second=10) as live:
        for i in range(1, iterations + 1):
            iter_start_time = time.time()
            status = "Success"
            
            try:
                # 1. Open Connection
                conn = decentdb.connect(db_path)
                cursor = conn.cursor()
                
                # 2. Create Table
                cursor.execute("CREATE TABLE memtest (id INT, data TEXT)")
                
                # 3. Insert Records
                for j in range(records_per_iter):
                    cursor.execute("INSERT INTO memtest VALUES (?, ?)", (j, generate_random_string()))
                conn.commit()
                
                # 4. Query Records
                cursor.execute("SELECT count(*) FROM memtest WHERE data LIKE '%a%'")
                cursor.fetchone()
                
                # 5. Add Index
                cursor.execute("CREATE INDEX idx_data ON memtest(data)")
                conn.commit()
                
                # 6. Insert more records (index active)
                for j in range(records_per_iter, records_per_iter * 2):
                    cursor.execute("INSERT INTO memtest VALUES (?, ?)", (j, generate_random_string()))
                conn.commit()
                
                # 7. Query records via exact match (index)
                cursor.execute("SELECT * FROM memtest WHERE data = 'test'")
                cursor.fetchall()
                
                # 8. Delete some records
                cursor.execute("DELETE FROM memtest WHERE id < 10")
                conn.commit()
                
                # 9. Drop index
                cursor.execute("DROP INDEX idx_data")
                conn.commit()
                
                # 10. Drop table
                cursor.execute("DROP TABLE memtest")
                conn.commit()
                
                # 11. Close Connection
                cursor.close()
                conn.close()
                
            except Exception as e:
                status = f"[red]Error: {str(e)}[/red]"
            
            iter_end_time = time.time()
            current_memory = get_memory_mb()
            
            if current_memory > max_memory:
                max_memory = current_memory
                
            if i == warmup_iterations:
                warmup_memory = current_memory
            
            stats_history.append({
                "iter": i,
                "mem_mb": current_memory,
                "mem_delta": current_memory - start_memory,
                "iter_time_ms": (iter_end_time - iter_start_time) * 1000,
                "status": status
            })
            
            # Update UI
            progress.update(task_id, advance=1)
            layout["main"].update(generate_table())

    # Cleanup
    if os.path.exists(db_path):
        os.remove(db_path)
        
    from rich.console import Console
    console = Console()
    
    # Calculate Leaks
    if warmup_memory is None:
        warmup_memory = start_memory
        
    # We compare the memory at the end of the test to the memory after the initial warmup phase.
    # Python's GC and internal allocations can cause some early growth, so we ignore the first 100 iters.
    final_memory = current_memory
    leak_delta = final_memory - warmup_memory
    
    # Threshold for a leak (e.g., 5 MB growth after warmup is suspicious)
    LEAK_THRESHOLD_MB = 5.0 
    
    is_success = leak_delta <= LEAK_THRESHOLD_MB
    
    summary_text = (
        f"[bold]Total Iterations:[/bold] {iterations}\n"
        f"[bold]Records per Iteration:[/bold] {records_per_iter * 2}\n"
        f"[bold]Start Memory:[/bold] {start_memory:.2f} MB\n"
        f"[bold]Warmup Memory (Iter {warmup_iterations}):[/bold] {warmup_memory:.2f} MB\n"
        f"[bold]Peak Memory:[/bold] {max_memory:.2f} MB\n"
        f"[bold]Final Memory:[/bold] {final_memory:.2f} MB\n"
        f"[bold]Memory Growth (Post-Warmup):[/bold] {leak_delta:.2f} MB\n\n"
    )
    
    if is_success:
        summary_text += f"[bold green]SUCCESS:[/bold green] No memory leaks detected. Engine memory stayed well under the {LEAK_THRESHOLD_MB} MB growth threshold."
        panel = Panel(summary_text, title="[bold green]Test Completed Successfully[/bold green]", border_style="green")
    else:
        summary_text += f"[bold red]FAILED:[/bold red] Potential memory leak detected! Engine memory grew by {leak_delta:.2f} MB after warmup, exceeding the {LEAK_THRESHOLD_MB} MB threshold."
        panel = Panel(summary_text, title="[bold red]Memory Leak Detected[/bold red]", border_style="red")
        
    console.print("\n")
    console.print(panel)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="DecentDB memory leak test showcase")
    parser.add_argument("--iterations", type=int, default=2000, help="Number of test iterations to run")
    parser.add_argument("--records-per-transaction", dest="records", type=int, default=100, help="Records to insert per iteration")
    args = parser.parse_args()
    
    main(iterations=args.iterations, records_per_txn=args.records)
