#!/usr/bin/env python3
"""
FerumFS + PyTorch DataLoader demo.

Shows that PyTorch training code works with FerumFS without any
modification. This is the core ML claim: any framework that reads from
POSIX paths works unchanged.

Prerequisites:
  pip install torch numpy
  FerumFS mounted at MOUNT (default /tmp/mnt1)

Run:
  python3 examples/pytorch_dataloader.py
"""

import os
import sys
import time
import tempfile

import numpy as np
import torch
from torch.utils.data import Dataset, DataLoader

MOUNT = os.environ.get("FERUMFS_MOUNT", "/tmp/mnt1")
DATA_DIR = os.path.join(MOUNT, "dataset")
NUM_SAMPLES = 1000
EMBED_DIM = 256


def prepare_dataset():
    """Generate sample tensors on the mount if they don't exist."""
    if os.path.isdir(DATA_DIR) and len(os.listdir(DATA_DIR)) >= NUM_SAMPLES:
        print(f"Dataset already present at {DATA_DIR}")
        return

    print(f"Generating {NUM_SAMPLES} samples on {DATA_DIR}...")
    os.makedirs(DATA_DIR, exist_ok=True)

    rng = np.random.default_rng(42)
    for i in range(NUM_SAMPLES):
        # Simulate a tokenized sequence (looks like an embedding batch).
        sample = torch.from_numpy(
            rng.standard_normal((EMBED_DIM,)).astype(np.float32)
        )
        torch.save(sample, os.path.join(DATA_DIR, f"sample_{i:05d}.pt"))

    total_bytes = sum(
        os.path.getsize(os.path.join(DATA_DIR, f))
        for f in os.listdir(DATA_DIR)
    )
    print(f"Generated {NUM_SAMPLES} samples, {total_bytes / 1024 / 1024:.1f} MiB total")


class FerumDataset(Dataset):
    """Loads tensors directly from a FerumFS mount.

    Identical to what you'd write for a local disk — no FerumFS-specific
    code, no plugins, no special API.
    """

    def __init__(self, root: str):
        self.root = root
        self.files = sorted(
            os.path.join(root, f)
            for f in os.listdir(root)
            if f.endswith(".pt")
        )

    def __len__(self) -> int:
        return len(self.files)

    def __getitem__(self, idx: int):
        return torch.load(self.files[idx], weights_only=True)


def benchmark(num_workers: int, batch_size: int = 32, epochs: int = 1):
    ds = FerumDataset(DATA_DIR)
    dl = DataLoader(
        ds,
        batch_size=batch_size,
        shuffle=True,
        num_workers=num_workers,
        pin_memory=False,
    )

    print(f"\nnum_workers={num_workers}, batch_size={batch_size}, epochs={epochs}")

    t0 = time.perf_counter()
    total_samples = 0
    for _ in range(epochs):
        for batch in dl:
            # Simulate a "forward pass" stub — real training would call
            # model(batch) here.
            total_samples += batch.shape[0]
    elapsed = time.perf_counter() - t0

    samples_per_sec = total_samples / elapsed
    mib_per_sec = (samples_per_sec * EMBED_DIM * 4) / 1024 / 1024

    print(f"  Time: {elapsed * 1000:.0f} ms")
    print(f"  Throughput: {samples_per_sec:.0f} samples/s ({mib_per_sec:.1f} MiB/s)")
    return samples_per_sec, mib_per_sec


def main():
    if not os.path.ismount(MOUNT) and not os.path.isdir(MOUNT):
        print(f"ERROR: {MOUNT} is not mounted and does not exist.", file=sys.stderr)
        print("Start a FerumFS cluster and mount it first.", file=sys.stderr)
        sys.exit(1)

    if not os.path.isdir(MOUNT):
        print(f"ERROR: {MOUNT} is not a directory", file=sys.stderr)
        sys.exit(1)

    print(f"FerumFS mount: {MOUNT}")
    prepare_dataset()

    # Sequential (single worker) — single-GPU-like workload.
    _ = benchmark(num_workers=0, batch_size=32)

    # Parallel workers — what a multi-GPU node actually does.
    for nw in [2, 4, 8]:
        _ = benchmark(num_workers=nw, batch_size=32)

    print("\nDone. PyTorch read FerumFS data with no code changes.")


if __name__ == "__main__":
    main()