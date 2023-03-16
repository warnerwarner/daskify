from distributed import Client
from dask_jobqueue import SLURMCluster
import numpy as np
import time
from tqdm import tqdm
import distributed
from itertools import product
from functools import partial
import dask



tqdm = partial(tqdm, position=0, leave=True)


class Daskified():
    """
    Class to control and hold Dask clients and distributed data. Contains all
    futures that have been generated, to try and make keeping track of data 
    somewhat easier
    """

    def __init__(self,
                 job_extra=['--time=10:00:0 --output=/dev/null --error=/dev/null'],
                 memory='50GB', size=125, cores=1, processes=1, queue='cpu'):
        """Initilisation

        Args:
            job_extra (list, optional): Additional arguments for the dask client
            memory (str, optional): Memory of each worker
            size (int, optional): Number of workers
            cores (int, optional): Number of cores per worker
            processes (int, optional): Number of proccesses per worker
            queue (str, optional): Which queue to pass to
        """
        self.job_extra = job_extra
        self.memory = memory
        self.size = size
        self.cores = cores
        self.processes = processes
        self.queue = queue
        self.client = None
        self.scattered = {}
        self.old_futures = []
        self.current_futures = None

    def start_cluster(self, testing=False, local=False):
        """Starts a cluster

        Args:
            testing (bool, optional): If in testing, overwrites default params
                                      to use only 2 workers with low memory
        """
        if testing:
            self.memory = '2GB'
            self.size = 2
        if local:
            self.size = 2
            cluster = distributed.LocalCluster(n_workers=self.size)
        else:
            cluster = SLURMCluster(queue=self.queue,
                                cores=self.cores,
                                processes=self.processes,
                                memory=self.memory,
                                job_extra=self.job_extra)
        cluster.scale(self.size)
        client = Client(cluster)
        self.client = client

    def scatter_data(self, key, data):
        """Scatter data to the client

        Args:
            data (TYPE): Data to spread to the client
        """
        scattered_data = self.client.scatter(data)
        self.scattered[key] = scattered_data

    def gather_data(self, future_or_key):
        """Gather scattered data from the client

        Args:
            future (TYPE): futures to gather data from

        Returns:
            TYPE: The gathered data
        """
        if isinstance(future_or_key, distributed.client.Future):
            gathered = self.client.gather(future_or_key)
        else:
            gathered = self.client.gather(self.scattered[future_or_key])
        return gathered

    def check_progress(self):
        """Check the progress of the most recently generated futures
        """
        status = np.array([i.status for i in self.current_futures])
        for i in np.unique(status):
            print(i, len(status[status == i]))

    def monitor_progress(self, futures=None):
        """Continuously monitor recently generated futures

        Args:
            futures (None, optional): Optional futures to monitor. If None
                                      it selects the most recently generated.
        """
        if futures is None:
            futures = self.current_futures
        status = np.array([i.status for i in futures])
        while not all(status == 'finished'):
            for i in np.unique(status):
                print(i, len(status[status == i]))
            time.sleep(0.05)

    def collect_results(self):
        """Collect the results

        Returns:
            TYPE: Results from the most recent futures
        """
        status = np.array([i.status for i in self.current_futures])
        if not all(status == 'finished'):
            print('Note - not all processes are finished')
        data = self.client.gather(self.current_futures)
        return data

    def gridsearch(self, func, *iterables):
        """Iterates through the passed iterables and generates separate jobs
           for each one
        
        Args:
            func (function): The function to be ran
            *iterables: Lists of iterables to be passed. For parameters with
                        single values, simply generate a list with them inside.
                        e.g. [value]
        
        Returns:
            iterable_combos: Tuples of the iterable parameters
            futures: The dask futures
        """
        futures = []
        iterable_combos = list(product(*iterables))
        for i in iterable_combos:
            futures.append(dask.delayed(func)(*i))
                           
        futures = self.client.compute(futures)
        self._update_current_futures(futures)
        return iterable_combos, futures

    def _update_current_futures(self, new_futures):
        """Updates the internal parameter which contains the most recent futures
           Moves old futures into a list, incase they are required later.
        
        Args:
            new_futures (list): List of the new features
        """
        if self.current_futures is None:
            self.current_futures = new_futures
        else:
            old_futures = self.current_futures
            self.old_futures.append(old_futures)
            self.current_futures = new_futures
    
    def shutdown_cluster(self):
        """Shutdown the client
        """
        self.client.shutdown()