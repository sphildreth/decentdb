#!/usr/bin/env python3
"""
Memory leak test for DecentDB Python bindings and engine core.
Based on the existing test_memory_leak.py but enhanced with rich output.
"""

import sys
import os
import tempfile
import gc
import psutil
import time
from pathlib import Path

# Add the bindings directory to Python path to import decentdb
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import decentdb
except ImportError as e:
    print(f"Failed to import decentdb: {e}")
    print("Make sure the DecentDB Python bindings are built and available.")
    sys.exit(1)

try:
    from rich.console import Console
    from rich.live import Live
    from rich.table import Table
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
    from rich.layout import Layout
    from rich.panel import Panel
except ImportError as e:
    print(f"Failed to import rich: {e}")
    print("Install required packages: pip install rich psutil")
    sys.exit(1)


def get_memory_mb():
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


def main():
    console = Console()
    max_iterations = 500  # Same as existing test
    
    console.print("[bold blue]DecentDB Memory Leak Test[/bold blue]")
    console.print(f"Running {max_iterations} iterations of open/close/select operations...\n")
    
    # Create a temporary directory for our test database
    temp_dir = tempfile.mkdtemp(prefix="decentdb_memtest_")
    db_path = os.path.join(temp_dir, "test_leak.ddb")
    console.print(f"Using database: {db_path}")
    
    # Track memory usage
    memory_samples = []
    process = psutil.Process(os.getpid())
    
    with Live(console=console, refresh_per_second=4) as live:
        # Create layout
        layout = Layout()
        layout.split_column(
            Layout(name="progress", size=3),
            Layout(name="stats", ratio=1),
            Layout(name="details", size=5)
        )
        
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
        )
        task_id = progress.add_task("Testing memory leaks...", total=max_iterations)
        
        # Stats table
        stats_table = Table(show_header=True, header_style="bold magenta")
        stats_table.add_column("Metric", style="dim")
        stats_table.add_column("Value")
        
        # Details table
        details_table = Table(show_header=True, header_style="bold blue")
        details_table.add_column("Iteration", width=12)
        details_table.add_column("Memory (MB)", width=12)
        details_table.add_column("Delta (MB)", width=12)
        details_table.add_column("Status", width=20)
        
        layout["progress"].update(progress)
        layout["stats"].update(Panel(stats_table, title="Memory Statistics"))
        layout["details"].update(Panel(details_table, title="Recent Iterations"))
        
        try:
            # Phase 1: Create and populate the database (like the existing test)
            console.print("[yellow]Phase 1: Creating and populating test database...[/yellow]")
            db = decentdb.connect(db_path)
            cur = db.cursor()
            cur.execute("CREATE TABLE leak_test(id int, data text)")
            
            # Insert 1000 rows with 1KB text each (~1MB data)
            for i in range(1000):
                cur.execute("INSERT INTO leak_test VALUES (?, ?)", (i, "a" * 1000))
                if i % 100 == 0:
                    progress.update(task_id, advance=100/500.0)  # Spread progress over iterations
            
            db.commit()
            db.close()
            console.print("[green]✓ Database created and populated[/green]")
            
            # Force garbage collection and measure baseline
            gc.collect()
            mem_before = get_memory_mb()
            baseline_memory = mem_before
            
            # Update stats
            stats_table.rows = []
            stats_table.add_row("Baseline Memory (MB)", f"{baseline_memory:.2f}")
            stats_table.add_row("Current Memory (MB)", f"{mem_before:.2f}")
            stats_table.add_row("Delta from Baseline (MB)", "0.00")
            stats_table.add_row("Iterations Completed", "0/500")
            
            live.update(layout)
            
            # Phase 2: Repeated open/close/select operations
            console.print("[yellow]Phase 2: Running open/close/select cycles...[/yellow]")
            
            for i in range(max_iterations):
                try:
                    # Open database
                    db = decentdb.connect(db_path)
                    cur = db.cursor()
                    
                    # Execute query
                    cur.execute("SELECT * FROM leak_test")
                    rows = cur.fetchall()
                    
                    # Verify we got expected data
                    assert len(rows) == 1000
                    assert rows[0][0] == 0  # First row id
                    assert rows[0][1] == "a" * 1000  # First row data
                    assert rows[999][0] == 999  # Last row id
                    assert rows[999][1] == "a" * 1000  # Last row data
                    
                    # Close database
                    db.close()
                    
                    # Measure memory after each iteration (less frequently to reduce overhead)
                    if i % 50 == 0 or i == max_iterations - 1:
                        gc.collect()  # Periodic garbage collection
                        mem_after = get_memory_mb()
                        delta = mem_after - baseline_memory
                        
                        memory_samples.append({
                            'iteration': i,
                            'memory': mem_after,
                            'delta': delta,
                            'status': 'OK'
                        })
                        
                        # Update progress
                        progress.update(task_id, advance=50 if i < max_iterations - 50 else (max_iterations - i))
                        
                        # Update stats table
                        stats_table = Table(show_header=True, header_style="bold magenta")
                        stats_table.add_column("Metric", style="dim")
                        stats_table.add_column("Value")
                        recent_samples = memory_samples[-10:] if len(memory_samples) >= 10 else memory_samples
                        if recent_samples:
                            avg_delta = sum(s['delta'] for s in recent_samples) / len(recent_samples)
                            max_delta = max(s['delta'] for s in recent_samples)
                            min_delta = min(s['delta'] for s in recent_samples)
                        else:
                            avg_delta = max_delta = min_delta = 0
                        
                        stats_table.add_row("Baseline Memory (MB)", f"{baseline_memory:.2f}")
                        stats_table.add_row("Current Memory (MB)", f"{mem_after:.2f}")
                        stats_table.add_row("Delta from Baseline (MB)", f"{delta:.2f}")
                        stats_table.add_row("Avg Delta (last 10)", f"{avg_delta:.2f}")
                        stats_table.add_row("Max Delta (last 10)", f"{max_delta:.2f}")
                        stats_table.add_row("Min Delta (last 10)", f"{min_delta:.2f}")
                        stats_table.add_row("Iterations Completed", f"{i+1}/{max_iterations}")
                        
                        # Update details table (last 5 iterations)
                        details_table = Table(show_header=True, header_style="bold blue")
                        details_table.add_column("Iteration", width=12)
                        details_table.add_column("Memory (MB)", width=12)
                        details_table.add_column("Delta (MB)", width=12)
                        details_table.add_column("Status", width=20)
                        for sample in memory_samples[-5:]:
                            details_table.add_row(
                                str(sample['iteration']),
                                f"{sample['memory']:.2f}",
                                f"{sample['delta']:.2f}",
                                sample['status']
                            )
                        # Add placeholder rows if we don't have 5 samples yet
                        for _ in range(max(0, 5 - len(memory_samples))):
                            details_table.add_row("-", "0.00", "0.00", "WAITING")
                    
                    # Update live display less frequently
                    if i % 10 == 0:
                        live.update(layout)
                        
                except Exception as e:
                    console.print(f"[red]Error in iteration {i}: {e}[/red]")
                    memory_samples.append({
                        'iteration': i,
                        'memory': get_memory_mb(),
                        'delta': 0,
                        'status': f'ERROR: {str(e)[:30]}'
                    })
            
            # Final measurement
            gc.collect()
            mem_after = get_memory_mb()
            total_delta = mem_after - baseline_memory
            
            # Final analysis
            progress.update(task_id, description="[green]Test completed![/green]")
            
            console.print("\n")
            console.print("[bold]Final Results:[/bold]")
            console.print(f"  Baseline Memory: {baseline_memory:.2f} MB")
            console.print(f"  Final Memory: {mem_after:.2f} MB")
            console.print(f"  Total Delta: {total_delta:.2f} MB")
            console.print(f"  Average Delta per Iteration: {total_delta/max_iterations:.4f} MB")
            
            # Determine if there's a potential leak
            if total_delta > 10.0:  # More than 10 MB total increase (same threshold as existing test)
                console.print("\n[bold red]⚠️  POTENTIAL MEMORY LEAK DETECTED[/bold red]")
                console.print(f"   Total memory increase of {total_delta:.2f} MB over {max_iterations} iterations")
                console.print("   This suggests the DecentDB engine or Python bindings may have memory leaks.")
            else:
                console.print("\n[bold green]✅ NO SIGNIFICANT MEMORY LEAK DETECTED[/bold green]")
                console.print(f"   Memory change of {total_delta:.2f} MB over {max_iterations} iterations is within acceptable bounds.")
                console.print("   The DecentDB engine and Python bindings appear to manage memory correctly.")
                
        except Exception as e:
            console.print(f"[red]Unexpected error during test: {e}[/red]")
            import traceback
            traceback.print_exc()
        finally:
            # Clean up temporary directory
            try:
                import shutil
                shutil.rmtree(temp_dir)
                console.print(f"\nCleaned up temporary directory: {temp_dir}")
            except Exception as e:
                console.print(f"[yellow]Warning: Could not clean up temporary directory {temp_dir}: {e}[/yellow]")


if __name__ == "__main__":
    main()