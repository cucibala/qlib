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
# download daily data from Binance API
python collector.py download_data --source_dir /code/crypto_data/source/1d --start 2021-01-01 --end 2025-03-15 --delay 1 --interval 1d

# normalize daily data
python collector.py normalize_data --source_dir /code/crypto_data/source/1d --normalize_dir /code/crypto_data/source/1d_nor --interval 1d --date_field_name date

# dump daily data
cd qlib/scripts
python dump_bin.py dump_all --csv_path /code/crypto_data/source/1d_nor --qlib_dir /code/qlib_data/crypto_data/1d --freq day --date_field_name date --include_fields open,high,low,prices,volume,total_volumes,market_caps
```

#### 1min from Binance

```bash
# download 1min data from Binance API 
# 注意：1分钟数据量较大，建议缩短日期范围
python collector.py download_data --source_dir ~/.qlib/crypto_data/source/1min --start 2023-01-01 --end 2023-01-31 --delay 1 --interval 1min

# normalize 1min data
python collector.py normalize_data --source_dir ~/.qlib/crypto_data/source/1min --normalize_dir ~/.qlib/crypto_data/source/1min_nor --interval 1min --date_field_name date

# dump 1min data
cd qlib/scripts
python dump_bin.py dump_all --csv_path ~/.qlib/crypto_data/source/1min_nor --qlib_dir ~/.qlib/qlib_data/crypto_data/1min --freq 1min --date_field_name date --include_fields open,high,low,prices,volume,total_volumes,market_caps
```

### 注意事项

> **重要提示**: 在最新版本中，日期字段统一使用 `pd.Timestamp` 类型，而不是 `datetime.date`，以避免类型比较错误。对于日线数据和分钟数据，都使用相同的日期时间处理方式。

### Using data

#### Daily data
```python
import qlib
from qlib.data import D

# 使用日线数据
qlib.init(provider_uri="~/.qlib/qlib_data/crypto_data/1d")
df = D.features(D.instruments(market="all"), ["$prices", "$volume", "$open", "$high", "$low"], freq="day")
```

#### 1min data
```python
import qlib
from qlib.data import D

# 使用1分钟数据
qlib.init(provider_uri="~/.qlib/qlib_data/crypto_data/1min")
df = D.features(D.instruments(market="all"), ["$prices", "$volume", "$open", "$high", "$low"], freq="1min")
```

### Help
```bash
python collector.py collector_data --help
```

## Parameters

- interval: 支持 1d（日线数据） 和 1min（1分钟数据）
- delay: 1（调用API之间的延迟时间，秒）
