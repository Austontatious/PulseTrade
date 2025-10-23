# kronos-nbeats

Minimal FastAPI service that fits a small NeuralForecast N-BEATS model on the request payload and returns mean and quantile forecasts. Replace `_lazy_model_fit` with a persistent artifact loader when production checkpoints are available under `/models`.
