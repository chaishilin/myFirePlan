# data_provider.py
import akshare as ak
import pandas as pd
from datetime import datetime
import logging
import concurrent.futures # 🔥 引入并发库用于处理超时

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataProvider:
    """
    外部财经数据源封装层 (带超时控制版)
    """
    
    # 设置超时时间 (秒)
    TIMEOUT_SECONDS = 8 

    @staticmethod
    def _run_with_timeout(func, *args):
        """
        私有辅助函数：在一个带超时的线程中运行函数
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(func, *args)
            try:
                return future.result(timeout=DataProvider.TIMEOUT_SECONDS)
            except concurrent.futures.TimeoutError:
                raise TimeoutError(f"请求超时 ({DataProvider.TIMEOUT_SECONDS}秒)")
            except Exception as e:
                raise e

    @staticmethod
    def _get_fund_nav_internal(code: str, end_date: str = None):
        """实际执行获取基金净值的逻辑"""
        # 默认先尝试作为开放式基金获取
        df = ak.fund_open_fund_info_em(symbol=code, indicator="单位净值走势")
        
        df['净值日期'] = pd.to_datetime(df['净值日期'])
        df.sort_values('净值日期', ascending=True, inplace=True)
        
        if end_date:
            target_date = pd.to_datetime(end_date)
            df = df[df['净值日期'] <= target_date]
            
        if df.empty:
            return None
            
        latest_nav = float(df.iloc[-1]['单位净值'])
        return latest_nav

    @staticmethod
    def get_fund_nav(code: str, start_date: str = None, end_date: str = None) -> float:
        """
        获取场外基金/ETF的最新单位净值 (带超时保护)
        """
        try:
            # 🔥 使用超时包装器调用
            val = DataProvider._run_with_timeout(DataProvider._get_fund_nav_internal, code, end_date)
            if val is not None:
                logger.info(f"✅ 基金 {code} 获取成功: {val}")
                return val
            else:
                logger.warning(f"⚠️ 基金 {code} 数据为空")
                return 1.0
        except TimeoutError:
            logger.error(f"⏳ 基金 {code} 请求超时")
            raise TimeoutError("网络超时") # 抛出异常供上层捕获
        except Exception as e:
            logger.error(f"❌ 获取基金 {code} 失败: {e}")
            return 1.0 # 其他错误降级返回 1.0

    @staticmethod
    def _get_stock_price_internal(code: str):
        """实际执行获取股票价格的逻辑"""
        df = ak.stock_zh_a_spot_em()
        row = df[df['代码'] == code]
        if row.empty:
            return None
        return float(row.iloc[0]['最新价'])

    @staticmethod
    def get_stock_price(code: str) -> float:
        """
        获取股票最新收盘价 (带超时保护)
        """
        try:
            val = DataProvider._run_with_timeout(DataProvider._get_stock_price_internal, code)
            if val is not None:
                return val
            return 1.0
        except TimeoutError:
            logger.error(f"⏳ 股票 {code} 请求超时")
            raise TimeoutError("网络超时")
        except Exception as e:
            logger.error(f"❌ 获取股票 {code} 失败: {e}")
            return 1.0

    @staticmethod
    def get_market_index_data(index_name, start_date_str, end_date_str):
        """
        获取指定区间内的指数数据，并统一格式为 [date, close]
        (纯净的数据获取逻辑，不包含 Streamlit 缓存)
        """
        try:
            df_index = pd.DataFrame()
            
            # 1. 沪深300 (sh000300)
            if index_name == "沪深300":
                # akshare 接口：stock_zh_index_daily
                df_index = ak.stock_zh_index_daily(symbol="sh000300")
                df_index = df_index[['date', 'close']]
                df_index['date'] = pd.to_datetime(df_index['date'])

            # 2. 纳斯达克100 (.NDX) - 使用新浪源
            elif index_name == "纳斯达克100":
                # akshare 接口：index_us_stock_sina
                df_index = ak.index_us_stock_sina(symbol=".NDX")
                df_index = df_index[['date', 'close']]
                df_index['date'] = pd.to_datetime(df_index['date'])
                
            # 3. 标普500 (.INX)
            elif index_name == "标普500":
                df_index = ak.index_us_stock_sina(symbol=".INX")
                df_index = df_index[['date', 'close']]
                df_index['date'] = pd.to_datetime(df_index['date'])

            # 4. 数据切片
            if not df_index.empty:
                # 转换输入日期格式以确保匹配
                s_date = pd.to_datetime(start_date_str)
                e_date = pd.to_datetime(end_date_str)
                
                mask = (df_index['date'] >= s_date) & (df_index['date'] <= e_date)
                return df_index.loc[mask].sort_values('date')
            
            return pd.DataFrame()
            
        except Exception as e:
            print(f"指数获取失败: {e}")
            return pd.DataFrame()
        
    @staticmethod
    def get_exchange_rate(currency_code, date_str):
        """
        获取指定日期、指定币种对人民币的汇率
        :param currency_code: USD, HKD, JPY, EUR, etc.
        :param date_str: YYYY-MM-DD
        :return: float rate or None
        """
        # 常见币种映射 (akshare currency_boc_sina 接口需要中文名称)
        # 根据你的资产情况，可以扩充这个字典
        name_map = {
            "USD": "美元",
            "HKD": "港币",
            "JPY": "日元",
            "EUR": "欧元",
            "GBP": "英镑",
            "AUD": "澳大利亚元",
            "CAD": "加拿大元",
            "SGD": "新加坡元",
        }
        
        cn_name = name_map.get(currency_code.upper())
        if not cn_name:
            print(f"⚠️ 未知的币种代码: {currency_code}，无法自动拉取汇率")
            return None

        try:
            # 转换日期格式: YYYY-MM-DD -> YYYYMMDD
            # 注意：中国银行牌价接口返回的是当天所有的时刻数据，我们通常取当天的平均价或收盘价，
            # 或者简单点，取第一条（通常是最新的）。
            # 为了稳健，我们查询当天的数据
            
            # 接口：currency_boc_sina (新浪财经-中国银行牌价)
            # symbol: 中文名称
            # start_date, end_date: YYYYMMDD
            date_str = date_str.replace("-","")
            df = ak.currency_boc_sina(symbol=cn_name, start_date=date_str, end_date=date_str)
            
            if df.empty:
                print(f"⚠️ {date_str} {cn_name} 无汇率数据 (可能是非交易日)")
                return None
            
            # df 列名通常包括：日期, 时间, 现汇买入价, 现钞买入价, 现汇卖出价, 现钞卖出价, 中行折算价
            # 我们优先取 "中行折算价" (中间价)，如果没有，取 "现汇买入价" (保守估值)
            
            # 确保按时间倒序，取最新的一条
            # 也就是当天的收盘价附近
            record = df.iloc[0] # akshare 返回通常是时间倒序吗？需要确认。通常是的，或者我们取平均。
            
            rate = None
            #print(record)
            if "中行汇买价" in record and record["中行汇买价"]:
                 rate = float(record["中行汇买价"])
            elif "中行钞卖价/汇卖价" in record and record["中行钞卖价/汇卖价"]:
                 rate = float(record["中行钞卖价/汇卖价"])
            elif "中行折算价" in record and record["中行折算价"]:
                 rate = float(record["中行折算价"])
            
            # 注意：日元汇率通常是每100日元，需要特殊判断吗？
            # akshare 的 currency_boc_sina 返回的日元通常是 100日元兑人民币
            # 比如 4.8 (代表 100 JPY = 4.8 CNY) -> 实际汇率 0.048
            # 但是大多数其他货币是 1 单位。
            # 这是一个坑。一般银行牌价 JPY 都是按 100 算的。
            if currency_code.upper() == "JPY" and rate > 1.0: 
                rate = rate / 100.0
                
            return rate / 100.0 if rate > 50 else rate # 二次兜底：如果算出来汇率比如 700 (100美元)，肯定不对，除以100？ 
            # 不，通常除了日元，其他都是1单位。上面的 JPY 判断应该够了。
            # 实际上中行折算价：美元~7.2，日元~4.8(100日元)。
            # 所以上面的 JPY / 100 是必须的。
            
            return rate

        except Exception as e:
            print(f"汇率获取失败 ({currency_code}): {e}")
            return None

if __name__ == "__main__":
    
    # 1. 生成 2025-12-24 至 2026-01-08 所有连续日期
    start_date = "2025-12-24"
    end_date = "2026-01-08"
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    # 2. 逐条循环请求每日USD汇率 + 打印结果
    for single_date in date_range:
        # 格式化日期为 你需要的 "YYYY-MM-DD" 格式
        date_str = single_date.strftime("%Y-%m-%d")
        try:
            # 核心：逐条调用汇率接口，和你的原代码语法完全一致
            sh = DataProvider.get_exchange_rate("USD", date_str)
            # 按你指定的格式打印结果
            print(f"{date_str} 汇率: {sh}")
        except Exception as e:
            # 异常捕获：兼容非交易日/接口报错/无数据的情况，不中断程序
            print(f"{date_str} 汇率: 获取失败，原因: {str(e)}")