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
        if self.interval == self.INTERVAL_1min:
            self.start_datetime = max(self.start_datetime, self.DEFAULT_START_DATETIME_1MIN)
        elif self.interval == self.INTERVAL_1d:
            pass
        else:
            raise ValueError(f"interval error: {self.interval}")

        self.start_datetime = self.convert_datetime(self.start_datetime, self._timezone)
        self.end_datetime = self.convert_datetime(self.end_datetime, self._timezone)

    @staticmethod
    def convert_datetime(dt: [pd.Timestamp, datetime.date, str], timezone):
        try:
            dt = pd.Timestamp(dt, tz=timezone).timestamp()
            dt = pd.Timestamp(dt, tz=tzlocal(), unit="s")
        except ValueError as e:
            pass
        return dt

    @property
    @abc.abstractmethod
    def _timezone(self):
        raise NotImplementedError("rewrite get_timezone")

    @staticmethod
    def get_data_from_remote(symbol, interval, start, end):
        error_msg = f"{symbol}-{interval}-{start}-{end}"
        try:
            client = Client()
            # 将时间转换为毫秒级时间戳
            start_ts = int(pd.Timestamp(start).timestamp() * 1000)
            end_ts = int(pd.Timestamp(end).timestamp() * 1000)
            
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
            
            # 转换时间戳为日期时间
            df['date'] = pd.to_datetime(df['open_time'], unit='ms')
            df['date'] = df['date'].dt.date
            
            # 转换数值列为浮点数
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)
            
            # 创建market_cap和total_volumes列模拟CoinGecko数据结构
            # 币安API不直接提供这些数据，所以我们使用一些替代计算
            df['market_cap'] = df['close'].astype(float) * df['volume'].astype(float)
            df['total_volumes'] = df['volume'].astype(float)
            
            # 选择所需的列
            result_df = df[['date', 'market_cap', 'total_volumes', 'close']]
            result_df.rename(columns={'close': 'prices'}, inplace=True)
            
            return result_df
            
        except Exception as e:
            logger.warning(f"{error_msg}:{e}")
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

        if interval == self.INTERVAL_1d:
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



class CryptoNormalize(BaseNormalize):
    DAILY_FORMAT = "%Y-%m-%d"

    @staticmethod
    def normalize_crypto(
        df: pd.DataFrame,
        calendar_list: list = None,
        date_field_name: str = "date",
        symbol_field_name: str = "symbol",
    ):
        if df.empty:
            return df
        df = df.copy()
        df.set_index(date_field_name, inplace=True)
        df.index = pd.to_datetime(df.index)
        df = df[~df.index.duplicated(keep="first")]
        if calendar_list is not None:
            df = df.reindex(
                pd.DataFrame(index=calendar_list)
                .loc[
                    pd.Timestamp(df.index.min()).date() : pd.Timestamp(df.index.max()).date()
                    + pd.Timedelta(hours=23, minutes=59)
                ]
                .index
            )
        df.sort_index(inplace=True)

        df.index.names = [date_field_name]
        return df.reset_index()

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.normalize_crypto(df, self._calendar_list, self._date_field_name, self._symbol_field_name)
        return df


class CryptoNormalize1d(CryptoNormalize):
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
            freq, value from [1min, 1d], default 1d, currently only supprot 1d
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
            $ python collector.py normalize_data --source_dir ~/.qlib/crypto_data/source/1d --normalize_dir ~/.qlib/crypto_data/source/1d_nor --interval 1d --date_field_name date
        """
        super(Run, self).normalize_data(date_field_name, symbol_field_name)


if __name__ == "__main__":
    fire.Fire(Run)
