# Collect Crypto Data

> *Please pay **ATTENTION** that the data is collected from [Binance](https://www.binance.com/) and the data might not be perfect. We recommend users to prepare their own data if they have high-quality dataset. For more information, users can refer to the [related document](https://qlib.readthedocs.io/en/latest/component/data.html#converting-csv-format-into-qlib-format)*

## Requirements

```bash
pip install -r requirements.txt
```

## Usage of the dataset
> *Crypto dataset supports Data retrieval function and basic backtest function with OHLC data from Binance.*

## Collector Data


### Crypto Data

#### 1d from Binance

```bash

# download from Binance API
python collector.py download_data --source_dir ~/.qlib/crypto_data/source/1d --start 2015-01-01 --end 2025-03-20 --delay 1 --interval 1d

# 1mniu
python collector.py download_data --source_dir ~/.qlib/crypto_data/source/1min --start 2015-01-01 --end 2025-03-20 --delay 1 --interval 1min

# normalize
python collector.py normalize_data --source_dir ~/.qlib/crypto_data/source/1d --normalize_dir ~/.qlib/crypto_data/source/1d_nor --interval 1d --date_field_name date

# dump data
cd qlib/scripts
python dump_bin.py dump_all --csv_path ~/.qlib/crypto_data/source/1d_nor --qlib_dir ~/.qlib/qlib_data/crypto_data --freq day --date_field_name date --include_fields prices,total_volumes,market_caps

```

### using data

```python
import qlib
from qlib.data import D

qlib.init(provider_uri="~/.qlib/qlib_data/crypto_data")
df = D.features(D.instruments(market="all"), ["$prices", "$total_volumes","$market_caps"], freq="day")
```


### Help
```bash
python collector.py collector_data --help
```

## Parameters

- interval: 1d
- delay: 1
