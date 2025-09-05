from __future__ import annotations
import torch
import torch.nn as nn

class SmallGRU(nn.Module):
    """
    Lightweight GRU for univariate/multi-indicator sequences.
    Input:  (batch, seq_len, n_features)
    Output: prob_up in [0,1]
    """
    def __init__(self, n_features: int = 6, hidden: int = 32, dropout: float = 0.1):
        super().__init__()
        self.gru = nn.GRU(input_size=n_features, hidden_size=hidden, num_layers=1, batch_first=True)
        self.dp  = nn.Dropout(dropout)
        self.head = nn.Sequential(
            nn.Linear(hidden, 32),
            nn.ReLU(),
            nn.Linear(32, 1),
            nn.Sigmoid()
        )

    def forward(self, x):
        out, _ = self.gru(x)
        h = out[:, -1, :]            # last step
        h = self.dp(h)
        return self.head(h).squeeze(-1)  # (B,)
