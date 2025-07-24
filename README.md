# daskify

Daskify is a small package to help interface with Dask and Distributed
workflows. It's written largely to run on SLURM clusters, but can be
easily modified to run locally or other distributed computing
networks.

## Background
I would recommend you familiarise yourself with distributed computing
and dask terminology, but I will outline a short description of some
of the key topics here.

### Distributed computing
Often in scientific computing problems, the same function needs to be
run again and again with small changes to parameters or initialisation
conditions. Other programming languages include core
parallelisation functions (e.g. MATLAB's parfor) which let users run
the code simultaneously.

Python, by default, does not allow this, its limited by something
called the Global Interpreter Lock (GIL). This prevents Python from running
multiple processes in the same _instance_ of Python. This means that
you can run multiple Python scripts at the same time on the same
computer, but a single script can only be ran on a single thread at
any one time.

### Workers
One way around this is to generate independent Python processes,
often called _workers_ and then run your code across multiple workers at
the same time. This can drastically increase workflow speed in some
cases. The number of workers directly relates to the number of
independent Python processes being run.

### Clients/Clusters
Managing independent Python processes can be complex, often this is
achieved through an intermediary referred to as a Client, or sometimes
a Cluster. This manages passing data and code between the main Python
process and the workers. When using distributed computing, you
typically need to start a Client to manage to the
workers. Confusingly, the Python instance which runs the Client is
also sometimes called the Client Process.

### Scattering
Data exisiting on the main Client Python process cannot be edited by
multiple Python processes at the same time. When you run a distributed
worker function and pass data from your client process, either your
workers are required to only access the data one at a time, or each
one copies the data over to their processes. This can lead to large
wait times as the data is being moved unnecessarily.

Instead you can _scatter_ the data using the 'scatter_data'
function. This will generate a copy of the data which all workers can
access at the same time. Note this scattered data is read only.

### Futures
Dask (and distributed computing in general) keeps track of remote
processes and functions using _futures_. These are objects which point
to the distributed data location (or predicted future
location). Daskify should handle most of the future object management
for you, but you should be aware.


## Workflow

### Start up
To run, simply make a Daskified object and then run the 'start_cluster'
function. It should spin up a distributed client with the set number
of workers. 

### Scatter data
Before running any functions, be sure to scatter any relevant data to
your workers. Typically small data arrays, or single floats/strings
don't need to be scattered.

### Run
To run functions simply use the 'Daskify.run' function. This function
takes the function you want the workers to run, and any combination of
iterable arguments you want to pass. So in a simple case, if you want
to run a function called 'my_func' on a worker you would use
'''
cluster = Daskified()
cluster.start_cluster
scattered_data = cluster.scatter_data(data)
cluster.run(my_func, args=[scattered_data])
'''

The run function also has a key word "iterables" argument. If you want
to say run through a series of parameters you can pass the parameter
range as a list to iterables

'''
cluster.run(my_func, args=[scattered_data], iterables=[range(10)])
'''

Will run my_func passing scattered_data 10 times, each one of which
receives one value from range(10). 

Finally, run also contains a loops key word argument. This runs the
same function with the same arguments as many times as requested. So

'''
cluster.run(my_func, args=[scattered_data], loops=5)
'''
Will run my_func 5 times, each time passing the same scattered_data
object to the worker.

### Checking progress
Daskified has 2 functions for checking how workers are doing

#### check_progress
check_progresses checks every scheduled future's current condition,
often these are 'pending' 'finished' or 'error'. It'll then print the
number of each of these at the time of checking

#### monitor_progress
monitor_progress keeps checking the progress of the most recently
submitted futures and will continuously update the condition with a
progress bar. By default monitor_progress counts the number of
finished or error statuses.

#### Gather results
Once your results are finished you can gather them using
cluster.gather_data(). You can either pass a specific key, futures, or
nothing to this function. If you pass nothing, it'll gather the most
recent futures


