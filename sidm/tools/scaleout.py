"""Module to define classes and methods that are helpful for scaleout"""

from dask.distributed import Client, PipInstall


def make_dask_client(address):
    """Create dask client that includes dependency installer"""
    dependencies = [
        "git+https://github.com/btcardwell/SIDM.git",
    ]
    client = Client(address)
    client.register_worker_plugin(PipInstall(packages=dependencies))
    return client
