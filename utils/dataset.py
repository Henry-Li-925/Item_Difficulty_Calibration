import os
import random
import torch
from torch.utils.data import Dataset, Sampler, random_split


class LogDataSet(Dataset):
    def __init__(self, df):
        self.label = df.iloc[:,:3]
        self.target = df.iloc[:, 3]
        self.log_idx2id = self.label['problem_log_id'].to_dict()
        self.user_idx2id = self.label['student_user_id'].sort_values().drop_duplicates().reset_index(drop=True).to_dict()
        self.item_idx2id = self.label['problem_id'].sort_values().drop_duplicates().reset_index(drop=True).to_dict()
        self.log_id2idx = {v: k for k, v in self.log_idx2id.items()}
        self.user_id2idx = {v: k for k, v in self.user_idx2id.items()}
        self.item_id2idx = {v: k for k, v in self.item_idx2id.items()}


    def __len__(self):
        return len(self.log_idx2id)

    def __getitem__(self, idx):
        user_idx = torch.tensor(self.user_id2idx[self.label.iloc[idx, 1]], dtype=torch.int64)
        item_idx = torch.tensor(self.item_id2idx[self.label.iloc[idx, 2]], dtype=torch.int64)
        target = torch.tensor(self.target.iloc[idx], dtype=torch.float)
        return user_idx, item_idx, target


class ChunkLogSamplerWithoutReplacement(Sampler):
    """
    A Sampler that collects samples that meet some criterion, e.g., the correctness is either 0 or 1,
    in batches. 
    """
    def __init__(self, dataset, batch_size, chunk_size = 100000, binary = True):
        self.dataset = dataset
        self.binary = binary
        self.batch_size = batch_size
        self.chunk_size = chunk_size
        assert batch_size % 2 == 0, "Batch size must be an even number."

        self.len = len(self.dataset)

        self.indices_pool = []

    def refill_pool(self):
        self.indices_pool.clear()

        sampled_indices = random.sample(range(self.len), min(self.chunk_size, self.len))

        for idx in sampled_indices:
            _, target = self.dataset[idx]

            if self.binary:
                if target in [0.0, 1.0]:
                    self.indices_pool.append(idx)
            else:
                self.indices_pool.append(idx)
        
        random.shuffle(self.indices_pool)


    def __iter__(self):
        return 

        
        
    def __len__(self):
        return self.len // self.batch_size + 1


