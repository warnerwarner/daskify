"""Interface with Dask and data.

Daskify, high level interface for interacting with distributed computing
Maintains history of dask futures and distributed datasets
"""

import time
from functools import partial
from itertools import product
from typing import Any, Callable, Literal

import dask
import distributed
import numpy as np
from dask_jobqueue import SLURMCluster
from tqdm import tqdm

tqdm = partial(tqdm, position=0, leave=True)


class Daskified:
    """Class to control and hold Dask clients and distributed data.

    Contains all futures that have been generated, to try and make
    keeping track of data somewhat easier
    """

    def __init__(
        self,
        job_extra: list = ["--time=10:00:0 --output=/dev/null --error=/dev/null"],
        memory: str = "50GB",
        size: int = 125,
        cores: int = 1,
        processes: int = 1,
        queue: str = "cpu",
    ) -> None:
        """Initialise class.

        Parameters
        ----------
        job_extra : list
            extra parameters for the job
        memory : str
            amount of memory per worker
        size : int
            number of workers to request
        cores : int
            number of cores to run the cluster over
        processes : int

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

    def start_cluster(self, mode: Literal["local", "slurm", "test"] = "slurm") -> None:
        """Start cluster.

        Starts running a cluster object.

        Parameters
        ----------
        mode : Literal["local", "slurm", "test"]
            Type of cluster client to run. local - a local cluster
            only on the current machine, defaults to a small number of
            workers. slurm - a full blown slurm cluster. test - a small
            slurm cluster for testing purposes.

        """
        if mode == "test":
            self.memory = "2GB"
            self.size = 2
        if mode == "local":
            self.size = 2
            cluster = distributed.LocalCluster(n_workers=self.size)
        elif mode == "slurm":
            cluster = SLURMCluster(
                queue=self.queue,
                cores=self.cores,
                processes=self.processes,
                memory=self.memory,
                job_extra=self.job_extra,
            )
        cluster.scale(self.size)
        client = distributed.Client(cluster)
        self.client = client

    def scatter_data(self, key: str, data: Any) -> None:
        """Scatter (distribute) data across the dask cluster.

        Parameters
        ----------
        key : str
            key to keep track of distributed datasets
        data : Any
            Data to be distributed to across the cluster

        """
        scattered_data = self.client.scatter(data)
        self.scattered[key] = scattered_data

    def gather_data(self, future_or_key: distributed.client.Future | str) -> Any:
        """Gather scattered data from the client

        Args:
            future_or_key (distributed.clinet.Future | str): Future object to gather, or a str
                                               to gather

        Returns:
            Any: The gathered data

        """
        if isinstance(future_or_key, distributed.client.Future):
            gathered = self.client.gather(future_or_key)
        else:
            gathered = self.client.gather(self.scattered[future_or_key])
        return gathered

    def check_progress(self) -> None:
        """Check the progress of the most recently generated futures"""
        status = np.array([i.status for i in self.current_futures])
        for i in np.unique(status):
            print(i, len(status[status == i]))

    def monitor_progress(self, futures: distributed.client.Future = None) -> None:
        """Continuously monitor recently generated futures

        Args:
            futures (None, optional): Optional futures to monitor. If None
                                      it selects the most recently generated.

        """
        if futures is None:
            futures = self.current_futures
        status = np.array([i.status for i in futures])
        while not all(status == "finished"):
            for i in np.unique(status):
                print(i, len(status[status == i]))
            time.sleep(0.05)

    def collect_results(self) -> Any:
        """Collect the results

        Returns:
            Any: Results from the most recent futures

        """
        status = np.array([i.status for i in self.current_futures])
        if not all(status == "finished"):
            print("Note - not all processes are finished")
        data = self.client.gather(self.current_futures)
        return data

    def gridsearch(
        self, func: Callable, *iterables,
    ) -> tuple[list[Any], list[distributed.client.Future]]:
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

    def _update_current_futures(
        self, new_futures: list[distributed.client.Future],
    ) -> None:
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

    def shutdown_cluster(self) -> None:
        """Shutdown the client"""
        self.client.shutdown()
