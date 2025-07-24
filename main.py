"""Interface with Dask and data.

Daskify, high level interface for interacting with distributed computing
Maintains history of dask futures and distributed datasets
"""

from __future__ import annotations

import time
from functools import partial
from itertools import product
from typing import TYPE_CHECKING, Any, Callable, Literal

import dask
import distributed
import numpy as np
from dask_jobqueue import SLURMCluster
from tqdm import tqdm

if TYPE_CHECKING:
    from collections.abc import Iterable

tqdm = partial(tqdm, position=0, leave=True)


class Daskified:
    """Class to control and hold Dask clients and distributed data.

    Contains all futures that have been generated, to try and make
    keeping track of data somewhat easier
    """

    def __init__(
        self,
        job_extra: list | None = None,
        memory: str = "50GB",
        size: int = 125,
        cores: int = 1,
        processes: int = 1,
        queue: str = "cpu",
    ) -> None:
        """Initialise.

        Parameters
        ----------
        job_extra : list, optional
            Extra arguments for the client job.
        memory : str
            How much memory per worker, only used for SLURMCluster
        size : int
            number of jobs to run
        cores : int
            number of cores per job
        processes : int
            how many processess to run on each node
        queue : str
            which queue to run distributed jobs on

        """
        if job_extra is None:
            job_extra = ["--time=10:00:0 --output=/dev/null --error=/dev/null"]
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

    def gather_data(self, future_or_key: distributed.client.Future | str = None) -> Any:
        """Gather data from distributed back to main thread.

        Parameters
        ----------
        future_or_key : distributed.client.Future | str
            Future object or key to query the future dictionary. Used
            to indicated what data to return.

        Returns
        -------
        Any
            Returns a copy of the scattered data

        """
        if isinstance(future_or_key, distributed.client.Future):
            self.client.gather(future_or_key)
        elif isinstance(future_or_key, str):
            gathered = self.client.gather(self.scattered[future_or_key])
        else:
            gathered = self.client.gather(self.current_futures)
        return gathered

    def check_progress(self) -> None:
        """Check progress of the most recently submitted future tasks."""
        status = np.array([i.status for i in self.current_futures])
        for i in np.unique(status):
            print(i, len(np.where(status) == i)[0])

    def monitor_progress(
        self,
        futures: list[distributed.client.Future] | None = None,
    ) -> None:
        """Continuously monitors most recently submitted future jobs.

        Parameters
        ----------
        futures : distributed.client.Future
            Futures to monitor.

        """
        if futures is None:
            futures = self.current_futures

        with tqdm(total=len(futures)) as pbar:
            completed = set()
            while len(completed) < len(futures):
                status = np.array([i.status for i in futures])
                finished = {
                    i for i, s in enumerate(status) if s in ("finished", "error")
                }
                new = finished - completed
                pbar.update(len(new))
                completed |= new

                time.sleep(0.05)
        print(f'finished:{len(np.where(status == 'finished')[0])}, error:f{len(np.where(status == 'error')[0])}')

    def collect_results(self) -> Any:
        """Collect results from most recently submitted futures.

        Returns
        -------
        Any
            The returned output from the futures task

        """
        status = np.array([i.status for i in self.current_futures])
        if not all(status == "finished"):
            pass
        return self.client.gather(self.current_futures)

    def run(
        self,
        func: Callable,
        args: list[Any] = None,
        iterables: list[Iterable] = None,
        loops: int = 1,
    ) -> tuple[list[Any], list[distributed.client.Future]]:
        """Run jobs with every combination of all iterables passed.

        Iterates through all optional iterables passed to the
        function. Will run a job with every combination. Total number
        of jobs will be the product of the length of iterables

        Parameters
        ----------
        func : Callable
            Function to run with dask futures
        *iterables : Iterable
            Iterables containing parameters to pass to func.

        Returns
        -------
        tuple[list[Any], list[distributed.client.Future]]
            Returns both the combination of all iterables along with
            their associated dask futures

        """
        futures = []
        iterable_combos = list(product(*iterables))
        for _ in range(loops):
            for i in iterable_combos:
                futures.append(dask.delayed(func)(*args, *i))

        futures = self.client.compute(futures)
        self._update_current_futures(futures)
        return iterable_combos, futures

    def _update_current_futures(
        self,
        new_futures: list[distributed.client.Future],
    ) -> None:
        """Hidden function to update the most recent set of futures.

        Parameters
        ----------
        new_futures : list[distributed.client.Future]
            List of new futures

        """
        if self.current_futures is None:
            self.current_futures = new_futures
        else:
            old_futures = self.current_futures
            self.old_futures.append(old_futures)
            self.current_futures = new_futures

    def shutdown_cluster(self) -> None:
        """Shutdown currently running cluster."""
        self.client.shutdown()
