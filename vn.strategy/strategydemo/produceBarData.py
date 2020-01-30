# encoding: UTF-8

from strategyEngine import *
from backtestingEngine import *
from stratetyProduceBar import StrategyProduceBar
import decimal

def main():
    """回测程序主函数"""
    # symbol = 'IF1506'
    symbol = 'a'

    # 创建回测引擎
    be = BacktestingEngine()

    # 创建策略引擎对象
    se = StrategyEngine(be.eventEngine, be, backtesting=True)
    be.setStrategyEngine(se)

    # 初始化回测引擎
    # be.connectMongo()
    be.connectMysql()
    # be.loadMongoDataHistory(symbol, datetime(2015,5,1), datetime.today())
    # be.loadMongoDataHistory(symbol, datetime(2012,1,9), datetime(2012,1,14))

    be.setDataHistory(symbol, datetime(2012,1,1), datetime(2012,1,31))

    # 创建策略对象
    setting = {}
    #setting['fastAlpha'] = 0.2
    #setting['slowAlpha'] =  0.05
    # setting['startDate'] = datetime(year=2015, month=5, day=20)
    setting['startDate'] = datetime(year=2012, month=1, day=1)

    se.createStrategy(u'生成M1M5数据策略', symbol, StrategyProduceBar, setting)

    # 启动所有策略
    se.startAll()

    # 开始回测
    be.startBacktesting()


# 回测脚本    
if __name__ == '__main__':
    main()


