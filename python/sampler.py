"""
    Sampler for DataBase collection
"""

from torch.utils.data import Sampler
from typing import Generator

from ustore.ucset import Collection


class UStoreSampler(Sampler):
    """
    Custom Sampler for samplig collection keys
    """

    def __init__(self, dataset: Collection, batch_size: int, num_samples: int) -> Generator[list, None, None]:
        """
        Custom Sampler for samplig collection keys
        """
        self.db = dataset
        self.batch_size = batch_size
        self.num_samples = num_samples

    def __iter__(self):
        """
        Sample keys with batch size
        """
        for _ in range(self.num_samples):
            yield self.db.sample_keys(self.batch_size).tolist()

    def __len__(self) -> int:
        """
        Return collection size
        """
        return len(self.db)
