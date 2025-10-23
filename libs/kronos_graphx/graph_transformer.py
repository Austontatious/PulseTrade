import torch
import torch.nn as nn
import torch.nn.functional as F


class GraphAttentionModel(nn.Module):
    """
    Learns context-dependent attention over symbols via transformer-style self-attention
    on compact per-symbol statistics derived from recent windows of returns.
    """

    def __init__(self, symbols=128, ctx_dim=16, heads=4, layers=2, ff=128):
        super().__init__()
        self.S = symbols
        self.ctx_dim = ctx_dim
        self.embed = nn.Linear(ctx_dim, ff)
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=ff, nhead=heads, dim_feedforward=ff * 2, batch_first=True
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=layers)
        self.out = nn.Linear(ff, ff)

    def _contextify(self, window: torch.Tensor) -> torch.Tensor:
        """
        Reduce [B, T, S] windows into per-symbol context summaries [B, S, ctx_dim].
        Features: mean, std, skew, kurtosis proxies padded to ctx_dim.
        """

        mean = window.mean(dim=1)
        std = window.std(dim=1).clamp_min(1e-6)
        normed = (window - mean.unsqueeze(1)) / std.unsqueeze(1)
        skew = normed.pow(3).mean(dim=1)
        kurt = normed.pow(4).mean(dim=1)
        ctx = torch.stack([mean, std, skew, kurt], dim=-1)
        if self.ctx_dim > ctx.shape[-1]:
            pad = torch.zeros(
                ctx.shape[0],
                ctx.shape[1],
                self.ctx_dim - ctx.shape[-1],
                device=ctx.device,
                dtype=ctx.dtype,
            )
            ctx = torch.cat([ctx, pad], dim=-1)
        elif self.ctx_dim < ctx.shape[-1]:
            ctx = ctx[..., : self.ctx_dim]
        return ctx

    @torch.no_grad()
    def context_attention(self, window: torch.Tensor) -> torch.Tensor:
        """
        Produce row-stochastic attention matrices from return windows.
        Args:
            window: [B, T, S] tensor of recent returns
        Returns:
            [B, S, S] attention matrices
        """

        if window.shape[-1] != self.S:
            raise ValueError(f"Expected {self.S} symbols, got {window.shape[-1]}")
        ctx = self._contextify(window)
        h = self.embed(ctx)
        h = self.encoder(h)
        h = self.out(h)
        q = F.normalize(h, dim=-1)
        A = torch.matmul(q, q.transpose(1, 2))
        A = F.relu(A)
        A = A / (A.sum(dim=-1, keepdim=True) + 1e-9)
        return A
