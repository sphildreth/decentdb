Create a Python showcase script that proves the DecentDB engine core and the DecentDB Python bindings (both in this repository) do not have memory leaks over time. 

I would expect this script to open/close connections, add tables,   insert/query records, add indexes, insert/query records more with indexed columsn, delete indexes, 
delete tables, close connections, etc. Normal database operations that would help expose memory leaks. Use the Python rich  framework to make a modern slick output as 
the script runs showing its progress on each step. Display stats on memory usage, database performance stats as the test(s) progress. Your goal is to basically make 
something that proves  there are (or there are not) memory leaks with either the DecentDB engine core or the DecentDB Python bindings. 

Name this {model}-python-memory-leak-tests.py 
