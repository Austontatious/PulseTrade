import torch
import torch.nn as nn


class StudentDiffusionDecoder(nn.Module):
    """
    Lightweight conditional diffusion-ish decoder that maps Gaussian noise and a
    context summary into multi-step returns. Sized for daily horizons to keep
    training predictable and fast.
    """

    def __init__(self, context_dim=64, hidden=256, layers=4, horizon=20, num_symbols=128):
        super().__init__()
        self.horizon = horizon
        self.num_symbols = num_symbols
        dims = [context_dim + 32] + [hidden] * layers + [horizon * num_symbols]
        blocks = []
        for i in range(len(dims) - 1):
            blocks.append(nn.Linear(dims[i], dims[i + 1]))
            if i < len(dims) - 2:
                blocks.append(nn.ReLU())
        self.net = nn.Sequential(*blocks)

    def forward(self, z, ctx):
        x = torch.cat([z, ctx], dim=-1)
        out = self.net(x)
        return out.view(-1, self.horizon, self.num_symbols)

    @torch.no_grad()
    def sample(self, num_symbols, context_len, horizon, num_paths, conditioning: dict):
        device = next(self.parameters()).device
        ctx = torch.zeros((num_paths, 64), device=device)
        if "vix" in conditioning:
            ctx[:, 0] = float(conditioning["vix"])
        z = torch.randn(num_paths, 32, device=device)
        paths = self.forward(z, ctx)
        H = min(horizon, self.horizon)
        S = min(num_symbols, self.num_symbols)
        return paths[:, :H, :S]
