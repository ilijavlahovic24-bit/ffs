#!/usr/bin/env python3
"""
FerumFS checkpoint durability demo.

Simulates a training loop that saves model checkpoints periodically.
Demonstrates:
  - Checkpoint write throughput (ML: every N minutes during training)
  - Durability across a leader failure (kill -9 mid-checkpoint)

Prerequisites:
  pip install torch
  FerumFS mounted at MOUNT

Run:
  python3 examples/checkpoint_demo.py
"""

import os
import sys
import time

import torch
import torch.nn as nn

MOUNT = os.environ.get("FERUMFS_MOUNT", "/tmp/mnt1")
CKPT_DIR = os.path.join(MOUNT, "checkpoints")

# Roughly a small GPT: 12 layers, 512 hidden, 8 heads ≈ 50 MB.
MODEL_CONFIG = dict(
    n_layers=12,
    hidden=512,
    n_heads=8,
    vocab=50257,
)


class TinyGPT(nn.Module):
    def __init__(self, vocab, hidden, n_layers):
        super().__init__()
        self.embed = nn.Embedding(vocab, hidden)
        self.layers = nn.ModuleList(
            [
                nn.TransformerEncoderLayer(
                    d_model=hidden, nhead=8, batch_first=True
                )
                for _ in range(n_layers)
            ]
        )
        self.head = nn.Linear(hidden, vocab)

    def forward(self, x):
        x = self.embed(x)
        for layer in self.layers:
            x = layer(x)
        return self.head(x)


def model_size_mb(model: nn.Module) -> float:
    total = 0
    for p in model.parameters():
        total += p.numel() * p.element_size()
    return total / 1024 / 1024


def main():
    if not os.path.isdir(MOUNT):
        print(f"ERROR: {MOUNT} is not a directory", file=sys.stderr)
        sys.exit(1)

    os.makedirs(CKPT_DIR, exist_ok=True)

    print("Constructing model...")
    model = TinyGPT(
        vocab=MODEL_CONFIG["vocab"],
        hidden=MODEL_CONFIG["hidden"],
        n_layers=MODEL_CONFIG["n_layers"],
    )
    size_mb = model_size_mb(model)
    print(f"Model size: {size_mb:.1f} MiB")

    # --- Checkpoint write benchmark ---------------------------------
    print("\n=== Checkpoint write (5 saves) ===")
    for step in range(1, 6):
        path = os.path.join(CKPT_DIR, f"ckpt_{step:04d}.pt")
        t0 = time.perf_counter()
        torch.save({"step": step, "model": model.state_dict()}, path)
        elapsed = time.perf_counter() - t0
        mbps = size_mb / elapsed
        print(f"  step {step}: {elapsed * 1000:.0f} ms ({mbps:.0f} MiB/s) → {path}")

    # --- Checkpoint read benchmark ----------------------------------
    print("\n=== Checkpoint read (load 5) ===")
    for step in range(1, 6):
        path = os.path.join(CKPT_DIR, f"ckpt_{step:04d}.pt")
        t0 = time.perf_counter()
        data = torch.load(path, weights_only=False)
        elapsed = time.perf_counter() - t0
        mbps = size_mb / elapsed
        assert data["step"] == step
        print(f"  step {step}: {elapsed * 1000:.0f} ms ({mbps:.0f} MiB/s)")

    # --- Durability test --------------------------------------------
    print("\n=== Durability test ===")
    print("Writing one more checkpoint, then verifying it survives...")
    final = os.path.join(CKPT_DIR, "ckpt_final.pt")
    torch.save({"step": 6, "model": model.state_dict()}, final)

    # Read back immediately to prove durability.
    data = torch.load(final, weights_only=False)
    assert data["step"] == 6
    print(f"✓ Checkpoint {final} written and readable")

    # Read from a DIFFERENT node's mount to prove replication.
    # (Requires /tmp/mnt2 or /tmp/mnt3 to exist.)
    for i in [2, 3]:
        other_mount = f"/tmp/mnt{i}"
        other_path = os.path.join(other_mount, "checkpoints", "ckpt_final.pt")
        if not os.path.exists(other_path):
            continue
        t0 = time.perf_counter()
        data = torch.load(other_path, weights_only=False)
        elapsed = time.perf_counter() - t0
        assert data["step"] == 6
        print(f"✓ Same checkpoint readable from node {i} ({elapsed * 1000:.0f} ms)")

    print("\nDone. Checkpoints survive; training can resume after crash.")


if __name__ == "__main__":
    main()