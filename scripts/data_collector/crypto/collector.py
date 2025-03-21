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
            
            # 确保转换为Timestamp类型
            if not isinstance(dt, pd.Timestamp):
                dt = pd.Timestamp(dt)
            
            # 时区处理 - 安全地添加或转换时区
            try:
                # 如果已有时区信息
                if dt.tzinfo is not None:
                    # 转换到目标时区
                    dt = dt.tz_convert(timezone)
                else:
                    # 添加时区信息
                    dt = dt.tz_localize(timezone)
            except Exception as e:
                logger.warning(f"时区转换失败: {e}，将使用无时区的时间戳")
                # 如果时区转换失败，移除时区信息以避免比较问题
                if dt.tzinfo is not None:
                    dt = dt.tz_localize(None)
            
            # 特别注意：tzlocal()可能在某些环境中不稳定，这里使用更明确的做法
            try:
                # 转换为UTC时间戳然后转回带时区的Timestamp
                dt_timestamp = dt.timestamp()
                # 使用固定的时区而非tzlocal()以增强稳定性
                dt = pd.Timestamp(dt_timestamp, tz='UTC', unit="s")
            except Exception as e:
                logger.warning(f"时间戳转换失败: {e}，将保持原始时间格式")
            
            return dt
        except Exception as e:
            logger.error(f"日期转换错误: {e}, 输入类型: {type(dt)}, 值: {dt}")
            # 返回一个安全的默认值
            return pd.Timestamp.now(tz='UTC')

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
            
            # 创建market_cap和total_volumes列模拟CoinGecko数据结构
            try:
                # 添加复权因子列，统一设置为1
                df['factor'] = 1.0
                
                # 转换所有数值列为浮点型，确保类型一致
                numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'factor']
                for col in numeric_cols:
                    df[col] = df[col].astype(float)
                    
            except Exception as e:
                logger.warning(f"添加复权因子失败: {e}")
                df['factor'] = 1.0
            
            # 只选择所需的列：日期、开盘价、收盘价、最高价、最低价、成交量和复权因子
            result_df = df[['date', 'open', 'close', 'high', 'low', 'volume', 'factor']]
            
            # 检查实际数据的日期范围
            # 确保时区一致性 - 将输入的start/end时间转换为时区感知的时间戳
            try:
                # 统一转换为UTC时区的时间戳，避免时区比较问题
                if isinstance(start, str):
                    requested_start = pd.to_datetime(start)
                else:
                    requested_start = pd.Timestamp(start)
                
                if isinstance(end, str):
                    requested_end = pd.to_datetime(end)
                else:
                    requested_end = pd.Timestamp(end)
                
                # 确保时区一致 - 如果是带时区的，保留时区；如果没有时区，不添加时区
                # 这样在后续比较时会自动处理时区问题
            except Exception as e:
                logger.error(f"日期格式转换错误: {e}, start: {start}, end: {end}")
                # 使用安全的默认值
                requested_start = pd.Timestamp.now() - pd.Timedelta(days=30)
                requested_end = pd.Timestamp.now()
            
            # 检查实际数据与请求的数据范围是否有显著差异
            if not result_df.empty:
                # 确保'date'列没有时区信息，以便于比较
                actual_start = pd.to_datetime(result_df['date'].min()).tz_localize(None)
                actual_end = pd.to_datetime(result_df['date'].max()).tz_localize(None)
                
                # 移除请求时间的时区信息，以便于比较
                if requested_start.tzinfo is not None:
                    requested_start = requested_start.tz_localize(None)
                if requested_end.tzinfo is not None:
                    requested_end = requested_end.tz_localize(None)
                
                # 计算日期范围重叠率，使用更宽松的验证
                try:
                    requested_range = (requested_end - requested_start).total_seconds()
                    if requested_range > 0:
                        actual_range = (actual_end - actual_start).total_seconds()
                        overlap_start = max(actual_start, requested_start)
                        overlap_end = min(actual_end, requested_end)
                        
                        if overlap_end > overlap_start:
                            overlap_range = (overlap_end - overlap_start).total_seconds()
                            overlap_ratio = overlap_range / requested_range
                            
                            # 仅记录覆盖率信息，但不强制要求完全覆盖
                            logger.info(f"{symbol} 数据覆盖率: {overlap_ratio:.2%}，实际日期范围: {actual_start} 至 {actual_end}")
                        else:
                            # 没有重叠也接受，只是记录一个警告
                            logger.warning(f"{symbol} 数据范围与请求范围没有重叠，但仍保存数据")
                except Exception as e:
                    logger.error(f"计算日期重叠率错误: {e}")
                
                # 如果数据点太少，还是不保存
                min_data_points = 5  # 降低最小数据点数量要求
                if len(result_df) < min_data_points:
                    logger.warning(f"{symbol} 数据点太少 ({len(result_df)} < {min_data_points})，不保存")
                    return None
            
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
            
            # 如果获取到数据，验证数据的有效范围
            if _result is not None and not _result.empty:
                try:
                    # 确保'date'列没有时区信息，以便于比较
                    actual_start = pd.to_datetime(_result['date'].min()).tz_localize(None)
                    actual_end = pd.to_datetime(_result['date'].max()).tz_localize(None)
                    
                    # 移除请求时间的时区信息，以便于比较
                    start_dt = start_datetime.tz_localize(None) if start_datetime.tzinfo is not None else start_datetime
                    end_dt = end_datetime.tz_localize(None) if end_datetime.tzinfo is not None else end_datetime
                    
                    # 计算重叠率，但不强制要求完全覆盖
                    requested_range = (end_dt - start_dt).total_seconds()
                    if requested_range > 0:
                        overlap_start = max(actual_start, start_dt)
                        overlap_end = min(actual_end, end_dt)
                        
                        # 检查是否有重叠
                        if overlap_end > overlap_start:
                            overlap_range = (overlap_end - overlap_start).total_seconds()
                            coverage_ratio = overlap_range / requested_range
                            
                            # 只记录覆盖率信息，不作为筛选条件
                            logger.info(f"{symbol} 数据覆盖率: {coverage_ratio:.2%}，实际日期范围: {actual_start} 至 {actual_end}")
                        else:
                            # 没有重叠，只记录警告，但仍然保存数据
                            logger.warning(f"{symbol} 数据范围与请求范围没有重叠。请求范围: {start_dt} - {end_dt}, 实际范围: {actual_start} - {actual_end}")
                    
                    # 检查数据点数量是否足够
                    min_data_points = 10  # 设置合理的最小数据点数量
                    if len(_result) < min_data_points:
                        logger.warning(f"{symbol} 数据点太少 ({len(_result)} < {min_data_points})，不保存")
                        return None
                    
                    # 记录数据范围
                    logger.info(f"{symbol} 数据范围: {actual_start} 至 {actual_end}, 数据点数量: {len(_result)}")
                except Exception as e:
                    # 捕获所有处理中的异常，但仍然尝试保存数据
                    logger.error(f"{symbol} 数据验证过程中出错: {e}")
                    # 异常不影响数据保存
            
            return _result
        else:
            raise ValueError(f"cannot support {interval}")

    def save_instrument(self, symbol, df: pd.DataFrame):
        """save instrument data to file

        Parameters
        ----------
        symbol: str
            instrument code
        df : pd.DataFrame
            df.columns must contain "symbol" and "datetime"
        """
        if df is None or df.empty:
            logger.warning(f"{symbol} is empty")
            return

        symbol = self.normalize_symbol(symbol)
        # symbol = code_to_fname(symbol)
        instrument_path = self.save_dir.joinpath(f"{symbol}.csv")
        df["symbol"] = symbol
        
        # 确保数据包含必要的列
        required_columns = ['open', 'close', 'high', 'low', 'volume', 'factor']
        for col in required_columns:
            if col not in df.columns:
                if col == 'factor':
                    df[col] = 1.0  # 添加默认的复权因子
                else:
                    logger.error(f"{symbol} 数据缺少必要的列: {col}")
                    return
        
        if instrument_path.exists():
            try:
                _old_df = pd.read_csv(instrument_path)
                
                # 检查旧数据是否有更改列名的需要
                old_format = False
                if 'prices' in _old_df.columns and 'close' not in _old_df.columns:
                    _old_df.rename(columns={'prices': 'close'}, inplace=True)
                    old_format = True
                
                # 检查旧数据是否缺少factor列
                if 'factor' not in _old_df.columns:
                    _old_df['factor'] = 1.0
                    old_format = True
                
                # 移除不需要的列
                columns_to_keep = ['date', 'symbol', 'open', 'close', 'high', 'low', 'volume', 'factor']
                extra_columns = [col for col in _old_df.columns if col not in columns_to_keep]
                if extra_columns:
                    _old_df = _old_df[[col for col in _old_df.columns if col in columns_to_keep]]
                    old_format = True
                
                if old_format:
                    logger.info(f"更新 {symbol} 的数据格式")
                
                # 合并新旧数据
                df = pd.concat([_old_df, df], sort=False)
                
            except Exception as e:
                logger.error(f"读取或处理旧数据时出错: {e}")
        
        # 只保存需要的列
        columns_to_save = ['date', 'symbol', 'open', 'close', 'high', 'low', 'volume', 'factor']
        df = df[[col for col in columns_to_save if col in df.columns]]
        
        # 去重并排序
        df = df.drop_duplicates(subset=['date']).sort_values(by=['date'])
        
        # 保存
        df.to_csv(instrument_path, index=False)


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
            
            # 确保必要的列都存在
            required_columns = ['open', 'close', 'high', 'low', 'volume', 'factor']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"数据中缺少必要的列: {', '.join(missing_columns)}")
                # 如果缺少factor列，添加默认值1
                if 'factor' in missing_columns and len(missing_columns) == 1:
                    logger.warning("添加默认的复权因子列")
                    df['factor'] = 1.0
                else:
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
