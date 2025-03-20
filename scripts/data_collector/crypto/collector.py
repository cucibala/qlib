import abc
import sys
import datetime
from abc import ABC
from pathlib import Path

import fire
import pandas as pd
from loguru import logger
from dateutil.tz import tzlocal

CUR_DIR = Path(__file__).resolve().parent
sys.path.append(str(CUR_DIR.parent.parent))
from data_collector.base import BaseCollector, BaseNormalize, BaseRun
from data_collector.utils import deco_retry

from binance.client import Client
from time import mktime
from datetime import datetime as dt
import time


_BINANCE_CRYPTO_SYMBOLS = None


def get_binance_crypto_symbols(qlib_data_path: [str, Path] = None) -> list:
    """get crypto symbols in binance

    Returns
    -------
        crypto symbols in binance exchange
    """
    global _BINANCE_CRYPTO_SYMBOLS  # pylint: disable=W0603

    @deco_retry
    def _get_binance():
        try:
            client = Client()
            exchange_info = client.get_exchange_info()
            symbols_info = pd.DataFrame(exchange_info['symbols'])
            # 过滤出以USDT交易的交易对
            usdt_symbols = symbols_info[symbols_info['quoteAsset'] == 'USDT']
            # 获取交易对ID
            _symbols = usdt_symbols['symbol'].apply(lambda x: x.replace('USDT', '')).to_list()
        except Exception as e:
            logger.warning(f"request error: {e}")
            raise ValueError("request error") from e
        return _symbols

    if _BINANCE_CRYPTO_SYMBOLS is None:
        _all_symbols = _get_binance()
        _BINANCE_CRYPTO_SYMBOLS = sorted(set(_all_symbols))

    return _BINANCE_CRYPTO_SYMBOLS


class CryptoCollector(BaseCollector):
    def __init__(
        self,
        save_dir: [str, Path],
        start=None,
        end=None,
        interval="1d",
        max_workers=1,
        max_collector_count=2,
        delay=1,  # delay need to be one
        check_data_length: int = None,
        limit_nums: int = None,
    ):
        """

        Parameters
        ----------
        save_dir: str
            crypto save dir
        max_workers: int
            workers, default 4
        max_collector_count: int
            default 2
        delay: float
            time.sleep(delay), default 0
        interval: str
            freq, value from [1min, 1d], default 1min
        start: str
            start datetime, default None
        end: str
            end datetime, default None
        check_data_length: int
            check data length, if not None and greater than 0, each symbol will be considered complete if its data length is greater than or equal to this value, otherwise it will be fetched again, the maximum number of fetches being (max_collector_count). By default None.
        limit_nums: int
            using for debug, by default None
        """
        super(CryptoCollector, self).__init__(
            save_dir=save_dir,
            start=start,
            end=end,
            interval=interval,
            max_workers=max_workers,
            max_collector_count=max_collector_count,
            delay=delay,
            check_data_length=check_data_length,
            limit_nums=limit_nums,
        )

        self.init_datetime()
        self.client = Client()

    def init_datetime(self):
        try:
            if self.interval == self.INTERVAL_1min:
                # 将DEFAULT_START_DATETIME_1MIN转换为Timestamp再比较
                default_start = pd.Timestamp(self.DEFAULT_START_DATETIME_1MIN)
                self.start_datetime = pd.Timestamp(self.start_datetime) if self.start_datetime else default_start
                self.start_datetime = max(self.start_datetime, default_start)
            elif self.interval == self.INTERVAL_1d:
                # 对于日线数据也进行类型转换以保持一致性
                if self.start_datetime:
                    self.start_datetime = pd.Timestamp(self.start_datetime)
            else:
                raise ValueError(f"interval error: {self.interval}")

            # 确保日期格式一致，都转换为Timestamp
            self.start_datetime = pd.Timestamp(self.start_datetime)
            self.end_datetime = pd.Timestamp(self.end_datetime) if self.end_datetime else pd.Timestamp(self.DEFAULT_END_DATETIME_1D)
            
            # 进行时区转换
            self.start_datetime = self.convert_datetime(self.start_datetime, self._timezone)
            self.end_datetime = self.convert_datetime(self.end_datetime, self._timezone)
            
            logger.info(f"Collection period: {self.start_datetime} to {self.end_datetime}")
        except Exception as e:
            logger.error(f"初始化日期时出错: {e}")
            raise

    @staticmethod
    def convert_datetime(dt: [pd.Timestamp, datetime.date, str], timezone):
        try:
            # 统一处理各种类型的日期输入
            if dt is None:
                logger.warning("输入的日期为None，将使用当前时间")
                dt = pd.Timestamp.now()
            elif isinstance(dt, str):
                dt = pd.Timestamp(dt)
            elif isinstance(dt, datetime.date) and not isinstance(dt, datetime.datetime):
                dt = pd.Timestamp(dt)
            
            # 时区处理
            if not isinstance(dt, pd.Timestamp):
                dt = pd.Timestamp(dt)
                
            # 添加或转换时区
            if dt.tzinfo is None:
                dt = dt.tz_localize(timezone)
            else:
                dt = dt.tz_convert(timezone)
                
            # 转换为时间戳再转回来，以标准化格式
            dt_timestamp = dt.timestamp()
            dt = pd.Timestamp(dt_timestamp, tz=tzlocal(), unit="s")
            
            return dt
        except Exception as e:
            logger.error(f"日期转换错误: {e}, 输入类型: {type(dt)}, 值: {dt}")
            # 返回一个默认值而不是直接抛出异常，以增强健壮性
            return pd.Timestamp.now()

    @property
    @abc.abstractmethod
    def _timezone(self):
        raise NotImplementedError("rewrite get_timezone")

    @staticmethod
    def get_data_from_remote(symbol, interval, start, end):
        error_msg = f"{symbol}-{interval}-{start}-{end}"
        try:
            client = Client()
            # 将时间转换为毫秒级时间戳，增加异常处理
            try:
                start_ts = int(pd.Timestamp(start).timestamp() * 1000)
                end_ts = int(pd.Timestamp(end).timestamp() * 1000)
            except Exception as e:
                logger.error(f"时间戳转换错误: {e}, start: {start}, end: {end}")
                # 使用默认值
                start_ts = int((pd.Timestamp.now() - pd.Timedelta(days=7)).timestamp() * 1000)
                end_ts = int(pd.Timestamp.now().timestamp() * 1000)
            
            # 将qlib的时间间隔映射到币安的时间间隔
            interval_map = {
                "1d": Client.KLINE_INTERVAL_1DAY,
                "1min": Client.KLINE_INTERVAL_1MINUTE,
                # 可以根据需要添加更多间隔映射
            }
            
            binance_interval = interval_map.get(interval)
            if not binance_interval:
                raise ValueError(f"不支持的时间间隔: {interval}")
            
            # 获取K线数据
            klines = client.get_historical_klines(
                symbol=symbol + "USDT",  # 添加USDT后缀
                interval=binance_interval,
                start_str=start_ts,
                end_str=end_ts
            )
            
            if not klines:
                logger.warning(f"No data for {error_msg}")
                return None
            
            # 构建数据框
            df = pd.DataFrame(
                klines,
                columns=[
                    'open_time', 'open', 'high', 'low', 'close', 'volume',
                    'close_time', 'quote_asset_volume', 'number_of_trades',
                    'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
                ]
            )
            
            # 转换时间戳为日期时间，统一使用pd.Timestamp
            df['date'] = pd.to_datetime(df['open_time'], unit='ms')
            
            # 转换数值列为浮点数，添加错误处理
            for col in ['open', 'high', 'low', 'close', 'volume']:
                try:
                    df[col] = df[col].astype(float)
                except Exception as e:
                    logger.warning(f"转换列 {col} 为浮点数失败: {e}")
                    df[col] = df[col].astype(str).str.replace(',', '').astype(float)
            
            # 创建market_cap和total_volumes列模拟CoinGecko数据结构
            try:
                df['market_cap'] = df['close'].astype(float) * df['volume'].astype(float)
                df['total_volumes'] = df['volume'].astype(float)
            except Exception as e:
                logger.warning(f"计算market_cap或total_volumes失败: {e}")
                df['market_cap'] = 0.0
                df['total_volumes'] = 0.0
            
            # 选择所需的列并添加OHLC列
            result_df = df[['date', 'market_cap', 'total_volumes', 'close', 'open', 'high', 'low', 'volume']]
            result_df.rename(columns={'close': 'prices'}, inplace=True)
            
            return result_df
            
        except Exception as e:
            logger.error(f"{error_msg}:{e}")
            return None

    def get_data(
        self, symbol: str, interval: str, start_datetime: pd.Timestamp, end_datetime: pd.Timestamp
    ) -> [pd.DataFrame]:
        def _get_simple(start_, end_):
            self.sleep()
            _remote_interval = interval
            return self.get_data_from_remote(
                symbol,
                interval=_remote_interval,
                start=start_,
                end=end_,
            )

        if interval == self.INTERVAL_1d or interval == self.INTERVAL_1min:
            _result = _get_simple(start_datetime, end_datetime)
        else:
            raise ValueError(f"cannot support {interval}")
        return _result


class CryptoCollector1d(CryptoCollector, ABC):
    def get_instrument_list(self):
        logger.info("get binance crypto symbols......")
        symbols = get_binance_crypto_symbols()
        logger.info(f"get {len(symbols)} symbols.")
        return symbols

    def normalize_symbol(self, symbol):
        return symbol

    @property
    def _timezone(self):
        return "Asia/Shanghai"


class CryptoCollector1min(CryptoCollector, ABC):
    def get_instrument_list(self):
        logger.info("get binance crypto symbols for 1min data......")
        symbols = get_binance_crypto_symbols()
        logger.info(f"get {len(symbols)} symbols.")
        return symbols

    def normalize_symbol(self, symbol):
        return symbol

    @property
    def _timezone(self):
        return "Asia/Shanghai"


class CryptoNormalize(BaseNormalize):
    DAILY_FORMAT = "%Y-%m-%d"
    MINUTE_FORMAT = "%Y-%m-%d %H:%M:%S"

    @staticmethod
    def normalize_crypto(
        df: pd.DataFrame,
        calendar_list: list = None,
        date_field_name: str = "date",
        symbol_field_name: str = "symbol",
        interval: str = "1d",
    ):
        if df is None or df.empty:
            logger.warning("输入数据为空")
            return pd.DataFrame()
            
        try:
            df = df.copy()
            
            # 确保日期字段存在
            if date_field_name not in df.columns:
                logger.error(f"数据中找不到日期字段: {date_field_name}")
                return pd.DataFrame()
            
            # 确保日期字段是一致的类型
            try:
                df[date_field_name] = pd.to_datetime(df[date_field_name])
            except Exception as e:
                logger.error(f"日期转换失败: {e}")
                return pd.DataFrame()
                
            df.set_index(date_field_name, inplace=True)
            df.index = pd.to_datetime(df.index)
            df = df[~df.index.duplicated(keep="first")]
            
            if calendar_list is not None:
                try:
                    # 确保calendar_list中的日期是datetime类型
                    calendar_list = pd.to_datetime(calendar_list)
                    min_date = pd.Timestamp(df.index.min())
                    max_date = pd.Timestamp(df.index.max()) + pd.Timedelta(hours=23, minutes=59)
                    
                    df = df.reindex(
                        pd.DataFrame(index=calendar_list)
                        .loc[min_date:max_date]
                        .index
                    )
                except Exception as e:
                    logger.error(f"重索引失败: {e}")
                    # 不进行重索引，继续处理
            
            df.sort_index(inplace=True)
            df.index.names = [date_field_name]
            
            return df.reset_index()
            
        except Exception as e:
            logger.error(f"数据标准化过程中出错: {e}")
            return pd.DataFrame()

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.normalize_crypto(
            df,
            self._calendar_list,
            self._date_field_name,
            self._symbol_field_name,
            getattr(self, "interval", "1d"),
        )
        return df


class CryptoNormalize1d(CryptoNormalize):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.interval = "1d"
        
    def _get_calendar_list(self):
        return None


class CryptoNormalize1min(CryptoNormalize):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.interval = "1min"
        
    def _get_calendar_list(self):
        return None


class Run(BaseRun):
    def __init__(self, source_dir=None, normalize_dir=None, max_workers=1, interval="1d"):
        """

        Parameters
        ----------
        source_dir: str
            The directory where the raw data collected from the Internet is saved, default "Path(__file__).parent/source"
        normalize_dir: str
            Directory for normalize data, default "Path(__file__).parent/normalize"
        max_workers: int
            Concurrent number, default is 1
        interval: str
            freq, value from [1min, 1d], default 1d
        """
        super().__init__(source_dir, normalize_dir, max_workers, interval)

    @property
    def collector_class_name(self):
        return f"CryptoCollector{self.interval}"

    @property
    def normalize_class_name(self):
        return f"CryptoNormalize{self.interval}"

    @property
    def default_base_dir(self) -> [Path, str]:
        return CUR_DIR

    def download_data(
        self,
        max_collector_count=2,
        delay=0,
        start=None,
        end=None,
        check_data_length: int = None,
        limit_nums=None,
    ):
        """download data from Internet

        Parameters
        ----------
        max_collector_count: int
            default 2
        delay: float
            time.sleep(delay), default 0
        interval: str
            freq, value from [1min, 1d], default 1d
        start: str
            start datetime, default "2000-01-01"
        end: str
            end datetime, default ``pd.Timestamp(datetime.datetime.now() + pd.Timedelta(days=1))``
        check_data_length: int # if this param useful?
            check data length, if not None and greater than 0, each symbol will be considered complete if its data length is greater than or equal to this value, otherwise it will be fetched again, the maximum number of fetches being (max_collector_count). By default None.
        limit_nums: int
            using for debug, by default None

        Examples
        ---------
            # get daily data
            $ python collector.py download_data --source_dir ~/.qlib/crypto_data/source/1d --start 2015-01-01 --end 2021-11-30 --delay 1 --interval 1d
            
            # get 1min data
            $ python collector.py download_data --source_dir ~/.qlib/crypto_data/source/1min --start 2023-01-01 --end 2023-01-31 --delay 1 --interval 1min
        """

        super(Run, self).download_data(max_collector_count, delay, start, end, check_data_length, limit_nums)

    def normalize_data(self, date_field_name: str = "date", symbol_field_name: str = "symbol"):
        """normalize data

        Parameters
        ----------
        date_field_name: str
            date field name, default date
        symbol_field_name: str
            symbol field name, default symbol

        Examples
        ---------
            # normalize daily data
            $ python collector.py normalize_data --source_dir ~/.qlib/crypto_data/source/1d --normalize_dir ~/.qlib/crypto_data/source/1d_nor --interval 1d --date_field_name date
            
            # normalize 1min data
            $ python collector.py normalize_data --source_dir ~/.qlib/crypto_data/source/1min --normalize_dir ~/.qlib/crypto_data/source/1min_nor --interval 1min --date_field_name date
        """
        super(Run, self).normalize_data(date_field_name, symbol_field_name)


if __name__ == "__main__":
    fire.Fire(Run)
