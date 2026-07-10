# Indicator Quirks

## pandas-ta `bbands()` — std Parameter Bug

`bbands()` in pandas-ta has a known issue with the `std` parameter that causes incorrect Bollinger Band calculations.

**Do not use `pandas_ta.bbands()` directly in trading bots.**

Use manual Bollinger Band calculations instead:

```python
import pandas as pd

def bollinger_bands(series: pd.Series, window: int, num_std: float):
    sma = series.rolling(window).mean()
    std = series.rolling(window).std()
    upper = sma + num_std * std
    lower = sma - num_std * std
    return upper, sma, lower
```

This is the safe, tested pattern used across all active bots.
