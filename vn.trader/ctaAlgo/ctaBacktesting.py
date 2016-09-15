# encoding: UTF-8

'''
本文件中包含的是CTA模块的回测引擎，回测引擎的API和CTA引擎一致，
可以使用和实盘相同的代码进行回测。
'''

from datetime import datetime, timedelta
from collections import OrderedDict
from itertools import product
import pymongo

import MySQLdb
import json
import os
import cPickle

from ctaBase import *
from ctaSetting import *

from vtConstant import *
from vtGateway import VtOrderData, VtTradeData
from vtFunction import loadMongoSetting
import logging

########################################################################
class BacktestingEngine(object):
    """
    CTA回测引擎
    函数接口和策略引擎保持一样，
    从而实现同一套代码从回测到实盘。
    # modified by IncenseLee：
    1.增加Mysql数据库的支持；
    2.修改装载数据为批量式后加载模式。

    """
    
    TICK_MODE = 'tick'
    BAR_MODE = 'bar'

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        # 本地停止单编号计数
        self.stopOrderCount = 0
        # stopOrderID = STOPORDERPREFIX + str(stopOrderCount)
        
        # 本地停止单字典
        # key为stopOrderID，value为stopOrder对象
        self.stopOrderDict = {}             # 停止单撤销后不会从本字典中删除
        self.workingStopOrderDict = {}      # 停止单撤销后会从本字典中删除
        
        # 回测相关
        self.strategy = None        # 回测策略
        self.mode = self.BAR_MODE   # 回测模式，默认为K线
        
        self.slippage = 0           # 回测时假设的滑点
        self.rate = 0               # 回测时假设的佣金比例（适用于百分比佣金）
        self.size = 1               # 合约大小，默认为1        
        
        self.dbClient = None        # 数据库客户端
        self.dbCursor = None        # 数据库指针
        
        self.historyData = []       # 历史数据的列表，回测用
        self.initData = []          # 初始化用的数据
        self.backtestingData = []   # 回测用的数据
        
        self.dbName = ''            # 回测数据库名
        self.symbol = ''            # 回测集合名

        self.dataStartDate = None       # 回测数据开始日期，datetime对象
        self.dataEndDate = None         # 回测数据结束日期，datetime对象
        self.strategyStartDate = None   # 策略启动日期（即前面的数据用于初始化），datetime对象
        
        self.limitOrderDict = OrderedDict()         # 限价单字典
        self.workingLimitOrderDict = OrderedDict()  # 活动限价单字典，用于进行撮合用
        self.limitOrderCount = 0                    # 限价单编号
        
        self.tradeCount = 0             # 成交编号
        self.tradeDict = OrderedDict()  # 成交字典
        
        self.logList = []               # 日志记录
        
        # 当前最新数据，用于模拟成交用
        self.tick = None
        self.bar = None
        self.dt = None      # 最新的时间
        self.gatewayName = u'BackTest'
        
    #----------------------------------------------------------------------
    def setStartDate(self, startDate='20100416', initDays=10):
        """设置回测的启动日期"""
        self.dataStartDate = datetime.strptime(startDate, '%Y%m%d')

        # 初始化天数
        initTimeDelta = timedelta(initDays)

        self.strategyStartDate = self.dataStartDate + initTimeDelta
        
    #----------------------------------------------------------------------
    def setEndDate(self, endDate=''):
        """设置回测的结束日期"""
        if endDate:
            self.dataEndDate = datetime.strptime(endDate, '%Y%m%d')

        else:
            self.dataEndDate = datetime.now()

    def setMinDiff(self, minDiff):
        """设置回测品种的最小跳价，用于修正数据"""
        self.minDiff = minDiff

    #----------------------------------------------------------------------
    def setBacktestingMode(self, mode):
        """设置回测模式"""
        self.mode = mode

    #----------------------------------------------------------------------
    def setDatabase(self, dbName, symbol):
        """设置历史数据所用的数据库"""
        self.dbName = dbName
        self.symbol = symbol

    #----------------------------------------------------------------------
    def loadHistoryDataFromMongo(self):
        """载入历史数据"""
        host, port = loadMongoSetting()
        
        self.dbClient = pymongo.MongoClient(host, port)
        collection = self.dbClient[self.dbName][self.symbol]

        self.output(u'开始载入数据')
      
        # 首先根据回测模式，确认要使用的数据类
        if self.mode == self.BAR_MODE:
            dataClass = CtaBarData
            func = self.newBar
        else:
            dataClass = CtaTickData
            func = self.newTick

        # 载入初始化需要用的数据
        flt = {'datetime':{'$gte':self.dataStartDate,
                           '$lt':self.strategyStartDate}}        
        initCursor = collection.find(flt)
        
        # 将数据从查询指针中读取出，并生成列表
        for d in initCursor:
            data = dataClass()
            data.__dict__ = d
            self.initData.append(data)      
        
        # 载入回测数据
        if not self.dataEndDate:
            flt = {'datetime':{'$gte':self.strategyStartDate}}   # 数据过滤条件
        else:
            flt = {'datetime':{'$gte':self.strategyStartDate,
                               '$lte':self.dataEndDate}}  
        self.dbCursor = collection.find(flt)
        
        self.output(u'载入完成，数据量：%s' %(initCursor.count() + self.dbCursor.count()))

    #----------------------------------------------------------------------
    def connectMysql(self):
        """连接MysqlDB"""

        # 载入json文件
        fileName = 'mysql_connect.json'
        try:
            f = file(fileName)
        except IOError:
            self.writeCtaLog(u'回测引擎读取Mysql_connect.json失败')
            return

        # 解析json文件
        setting = json.load(f)
        try:
            mysql_host = str(setting['host'])
            mysql_port = int(setting['port'])
            mysql_user = str(setting['user'])
            mysql_passwd = str(setting['passwd'])
            mysql_db = str(setting['db'])


        except IOError:
            self.writeCtaLog(u'回测引擎读取Mysql_connect.json,连接配置缺少字段，请检查')
            return

        try:
            self.__mysqlConnection = MySQLdb.connect(host=mysql_host, user=mysql_user,
                                                     passwd=mysql_passwd, db=mysql_db, port=mysql_port)
            self.__mysqlConnected = True
            self.writeCtaLog(u'回测引擎连接MysqlDB成功')
        except ConnectionFailure:
            self.writeCtaLog(u'回测引擎连接MysqlDB失败')

     #----------------------------------------------------------------------
    def loadDataHistoryFromMysql(self, symbol, startDate, endDate):
        """载入历史TICK数据
        如果加载过多数据会导致加载失败,间隔不要超过半年
        """

        if not endDate:
            endDate = datetime.today()

        # 看本地缓存是否存在
        if self.__loadDataHistoryFromLocalCache(symbol, startDate, endDate):
            self.writeCtaLog(u'历史TICK数据从Cache载入')
            return

        # 每次获取日期周期
        intervalDays = 10

        for i in range (0,(endDate - startDate).days +1, intervalDays):
            d1 = startDate + timedelta(days = i )

            if (endDate - d1).days > 10:
                d2 = startDate + timedelta(days = i + intervalDays -1 )
            else:
                d2 = endDate

            # 从Mysql 提取数据
            self.__qryDataHistoryFromMysql(symbol, d1, d2)

        self.writeCtaLog(u'历史TICK数据共载入{0}条'.format(len(self.historyData)))

        # 保存本地cache文件
        self.__saveDataHistoryToLocalCache(symbol, startDate, endDate)


    def __loadDataHistoryFromLocalCache(self, symbol, startDate, endDate):
        """看本地缓存是否存在
        added by IncenseLee
        """

        # 运行路径下cache子目录
        cacheFolder = os.getcwd()+'/cache'

        # cache文件
        cacheFile = u'{0}/{1}_{2}_{3}.pickle'.\
                    format(cacheFolder, symbol, startDate.strftime('%Y-%m-%d'), endDate.strftime('%Y-%m-%d'))

        if not os.path.isfile(cacheFile):
            return False

        else:
            # 从cache文件加载
            cache = open(cacheFile,mode='r')
            self.historyData = cPickle.load(cache)
            cache.close()
            return True

    def __saveDataHistoryToLocalCache(self, symbol, startDate, endDate):
        """保存本地缓存
        added by IncenseLee
        """

        # 运行路径下cache子目录
        cacheFolder = os.getcwd()+'/cache'

        # 创建cache子目录
        if not os.path.isdir(cacheFolder):
            os.mkdir(cacheFolder)

        # cache 文件名
        cacheFile = u'{0}/{1}_{2}_{3}.pickle'.\
                    format(cacheFolder, symbol, startDate.strftime('%Y-%m-%d'), endDate.strftime('%Y-%m-%d'))

        # 重复存在 返回
        if os.path.isfile(cacheFile):
            return False

        else:
            # 写入cache文件
            cache = open(cacheFile, mode='w')
            cPickle.dump(self.historyData,cache)
            cache.close()
            return True

    #----------------------------------------------------------------------
    def __qryDataHistoryFromMysql(self, symbol, startDate, endDate):
        """从Mysql载入历史TICK数据
        added by IncenseLee
        """

        try:
            self.connectMysql()
            if self.__mysqlConnected:

                # 获取指针
                cur = self.__mysqlConnection.cursor(MySQLdb.cursors.DictCursor)

                if endDate:

                    # 开始日期 ~ 结束日期
                    sqlstring = ' select \'{0}\' as InstrumentID, str_to_date(concat(ndate,\' \', ntime),' \
                               '\'%Y-%m-%d %H:%i:%s\') as UpdateTime,price as LastPrice,vol as Volume,' \
                               'position_vol as OpenInterest,bid1_price as BidPrice1,bid1_vol as BidVolume1, ' \
                               'sell1_price as AskPrice1, sell1_vol as AskVolume1 from TB_{0}MI ' \
                               'where ndate between cast(\'{1}\' as date) and cast(\'{2}\' as date) order by UpdateTime'.\
                               format(symbol,  startDate, endDate)

                elif startDate:

                    # 开始日期 - 当前
                    sqlstring = ' select \'{0}\' as InstrumentID,str_to_date(concat(ndate,\' \', ntime),' \
                               '\'%Y-%m-%d %H:%i:%s\') as UpdateTime,price as LastPrice,vol as Volume,' \
                               'position_vol as OpenInterest,bid1_price as BidPrice1,bid1_vol as BidVolume1, ' \
                               'sell1_price as AskPrice1, sell1_vol as AskVolume1 from TB__{0}MI ' \
                               'where ndate > cast(\'{1}\' as date) order by UpdateTime'.\
                               format( symbol, startDate)

                else:

                    # 所有数据
                    sqlstring =' select \'{0}\' as InstrumentID,str_to_date(concat(ndate,\' \', ntime),' \
                              '\'%Y-%m-%d %H:%i:%s\') as UpdateTime,price as LastPrice,vol as Volume,' \
                              'position_vol as OpenInterest,bid1_price as BidPrice1,bid1_vol as BidVolume1, ' \
                              'sell1_price as AskPrice1, sell1_vol as AskVolume1 from TB__{0}MI order by UpdateTime'.\
                              format(symbol)

                self.writeCtaLog(sqlstring)

                # 执行查询
                count = cur.execute(sqlstring)
                self.writeCtaLog(u'历史TICK数据共{0}条'.format(count))


                # 分批次读取
                fetch_counts = 0
                fetch_size = 1000

                while True:
                    results = cur.fetchmany(fetch_size)

                    if not results:
                        break

                    fetch_counts = fetch_counts + len(results)

                    if not self.historyData:
                        self.historyData =results

                    else:
                        self.historyData = self.historyData + results

                    self.writeCtaLog(u'{1}~{2}历史TICK数据载入共{0}条'.format(fetch_counts,startDate,endDate))


            else:
                self.writeCtaLog(u'MysqlDB未连接，请检查')

        except MySQLdb.Error, e:

            self.writeCtaLog(u'MysqlDB载入数据失败，请检查.Error {0}'.format(e))

    def __dataToTick(self, data):
        """
        数据库查询返回的data结构，转换为tick对象
        added by IncenseLee
        """
        tick = CtaTickData()
        symbol = data['InstrumentID']
        tick.symbol = symbol

        # 创建TICK数据对象并更新数据
        tick.vtSymbol = symbol
        # tick.openPrice = data['OpenPrice']
        # tick.highPrice = data['HighestPrice']
        # tick.lowPrice = data['LowestPrice']
        tick.lastPrice = float(data['LastPrice'])

        tick.volume = data['Volume']
        tick.openInterest = data['OpenInterest']

        #  tick.upperLimit = data['UpperLimitPrice']
        #  tick.lowerLimit = data['LowerLimitPrice']

        tick.datetime = data['UpdateTime']
        tick.date = tick.datetime.strftime('%Y-%m-%d')
        tick.time = tick.datetime.strftime('%H:%M:%S')

        tick.bidPrice1 = float(data['BidPrice1'])
        # tick.bidPrice2 = data['BidPrice2']
        # tick.bidPrice3 = data['BidPrice3']
        # tick.bidPrice4 = data['BidPrice4']
        # tick.bidPrice5 = data['BidPrice5']

        tick.askPrice1 = float(data['AskPrice1'])
        # tick.askPrice2 = data['AskPrice2']
        # tick.askPrice3 = data['AskPrice3']
        # tick.askPrice4 = data['AskPrice4']
        # tick.askPrice5 = data['AskPrice5']

        tick.bidVolume1 = data['BidVolume1']
        # tick.bidVolume2 = data['BidVolume2']
        # tick.bidVolume3 = data['BidVolume3']
        # tick.bidVolume4 = data['BidVolume4']
        # tick.bidVolume5 = data['BidVolume5']

        tick.askVolume1 = data['AskVolume1']
        # tick.askVolume2 = data['AskVolume2']
        # tick.askVolume3 = data['AskVolume3']
        # tick.askVolume4 = data['AskVolume4']
        # tick.askVolume5 = data['AskVolume5']

        return tick

    #----------------------------------------------------------------------
    def getMysqlDeltaDate(self,symbol, startDate, decreaseDays):
        """从mysql库中获取交易日前若干天
        added by IncenseLee
        """
        try:
            if self.__mysqlConnected:

                # 获取mysql指针
                cur = self.__mysqlConnection.cursor()

                sqlstring='select distinct ndate from TB_{0}MI where ndate < ' \
                          'cast(\'{1}\' as date) order by ndate desc limit {2},1'.format(symbol, startDate, decreaseDays-1)

                # self.writeCtaLog(sqlstring)

                count = cur.execute(sqlstring)

                if count > 0:

                    # 提取第一条记录
                    result = cur.fetchone()

                    return result[0]

                else:
                    self.writeCtaLog(u'MysqlDB没有查询结果，请检查日期')

            else:
                self.writeCtaLog(u'MysqlDB未连接，请检查')

        except MySQLdb.Error, e:

            self.writeCtaLog(u'MysqlDB载入数据失败，请检查.Error {0}: {1}'.format(e.arg[0],e.arg[1]))

        # 出错后缺省返回
        return startDate-timedelta(days=3)

    #----------------------------------------------------------------------
    def runBacktestingWithMysql(self):
        """运行回测(使用Mysql数据）
        added by IncenseLee
        """

        if not self.dataStartDate:
            self.writeCtaLog(u'回测开始日期未设置。')
            return

        if not self.dataEndDate:
            self.dataEndDate = datetime.today()

        if len(self.symbol)<1:
            self.writeCtaLog(u'回测对象未设置。')
            return


        # 首先根据回测模式，确认要使用的数据类
        if self.mode == self.BAR_MODE:
            dataClass = CtaBarData
            func = self.newBar
        else:
            dataClass = CtaTickData
            func = self.newTick

        self.output(u'开始回测')

        #self.strategy.inited = True
        self.strategy.onInit()
        self.output(u'策略初始化完成')

        self.strategy.trading = True
        self.strategy.onStart()
        self.output(u'策略启动完成')

        self.output(u'开始回放数据')


        # 每次获取日期周期
        intervalDays = 10

        for i in range (0,(self.dataEndDate - self.dataStartDate).days +1, intervalDays):
            d1 = self.dataStartDate + timedelta(days = i )

            if (self.dataEndDate - d1).days > intervalDays:
                d2 = self.dataStartDate + timedelta(days = i + intervalDays -1 )
            else:
                d2 = self.dataEndDate

            # 提取历史数据
            self.loadDataHistoryFromMysql(self.symbol, d1, d2)

            self.output(u'数据日期:{0} => {1}'.format(d1,d2))
            # 将逐笔数据推送
            for data in self.historyData:

                # 记录最新的TICK数据
                self.tick = self.__dataToTick(data)
                self.dt = self.tick.datetime

                # 处理限价单
                self.crossLimitOrder()
                self.crossStopOrder()

                # 推送到策略引擎中
                self.strategy.onTick(self.tick)

            # 清空历史数据
            self.historyData = []

        self.output(u'数据回放结束')

    #----------------------------------------------------------------------
    def runBacktesting(self):
        """运行回测"""
        # 载入历史数据
        self.loadHistoryData()

        # 首先根据回测模式，确认要使用的数据类
        if self.mode == self.BAR_MODE:
            dataClass = CtaBarData
            func = self.newBar
        else:
            dataClass = CtaTickData
            func = self.newTick

        self.output(u'开始回测')
        
        self.strategy.inited = True
        self.strategy.onInit()
        self.output(u'策略初始化完成')
        
        self.strategy.trading = True
        self.strategy.onStart()
        self.output(u'策略启动完成')
        
        self.output(u'开始回放数据')

        for d in self.dbCursor:
            data = dataClass()
            data.__dict__ = d
            func(data)     
            
        self.output(u'数据回放结束')

    #----------------------------------------------------------------------
    def newBar(self, bar):
        """新的K线"""
        self.bar = bar
        self.dt = bar.datetime
        self.crossLimitOrder()      # 先撮合限价单
        self.crossStopOrder()       # 再撮合停止单
        self.strategy.onBar(bar)    # 推送K线到策略中
    
    #----------------------------------------------------------------------
    def newTick(self, tick):
        """新的Tick"""
        self.tick = tick
        self.dt = tick.datetime
        self.crossLimitOrder()
        self.crossStopOrder()
        self.strategy.onTick(tick)
        
    #----------------------------------------------------------------------
    def initStrategy(self, strategyClass, setting=None):
        """
        初始化策略
        setting是策略的参数设置，如果使用类中写好的默认设置则可以不传该参数
        """
        self.strategy = strategyClass(self, setting)
        self.strategy.name = self.strategy.className
        
    #----------------------------------------------------------------------
    def sendOrder(self, vtSymbol, orderType, price, volume, strategy):
        """发单"""

        self.writeCtaLog(u'{0},{1},{2}@{3}'.format(vtSymbol,orderType,price,volume))
        self.limitOrderCount += 1
        orderID = str(self.limitOrderCount)
        
        order = VtOrderData()
        order.vtSymbol = vtSymbol
        order.price = price
        order.totalVolume = volume
        order.status = STATUS_NOTTRADED     # 刚提交尚未成交
        order.orderID = orderID
        order.vtOrderID = orderID
        order.orderTime = str(self.dt)

        # added by IncenseLee
        order.gatewayName = self.gatewayName
        
        # CTA委托类型映射
        if orderType == CTAORDER_BUY:
            order.direction = DIRECTION_LONG
            order.offset = OFFSET_OPEN
        elif orderType == CTAORDER_SELL:
            order.direction = DIRECTION_SHORT
            order.offset = OFFSET_CLOSE
        elif orderType == CTAORDER_SHORT:
            order.direction = DIRECTION_SHORT
            order.offset = OFFSET_OPEN
        elif orderType == CTAORDER_COVER:
            order.direction = DIRECTION_LONG
            order.offset = OFFSET_CLOSE     

        # modified by IncenseLee
        key = u'{0}.{1}'.format(order.gatewayName, orderID)
        # 保存到限价单字典中
        self.workingLimitOrderDict[key] = order
        self.limitOrderDict[key] = order
        return key
    
    #----------------------------------------------------------------------
    def cancelOrder(self, vtOrderID):
        """撤单"""
        if vtOrderID in self.workingLimitOrderDict:
            order = self.workingLimitOrderDict[vtOrderID]
            order.status = STATUS_CANCELLED
            order.cancelTime = str(self.dt)
            del self.workingLimitOrderDict[vtOrderID]
        
    #----------------------------------------------------------------------
    def sendStopOrder(self, vtSymbol, orderType, price, volume, strategy):
        """发停止单（本地实现）"""

        self.stopOrderCount += 1
        stopOrderID = STOPORDERPREFIX + str(self.stopOrderCount)
        
        so = StopOrder()
        so.vtSymbol = vtSymbol
        so.price = price
        so.volume = volume
        so.strategy = strategy
        so.stopOrderID = stopOrderID
        so.status = STOPORDER_WAITING
        
        if orderType == CTAORDER_BUY:
            so.direction = DIRECTION_LONG
            so.offset = OFFSET_OPEN
        elif orderType == CTAORDER_SELL:
            so.direction = DIRECTION_SHORT
            so.offset = OFFSET_CLOSE
        elif orderType == CTAORDER_SHORT:
            so.direction = DIRECTION_SHORT
            so.offset = OFFSET_OPEN
        elif orderType == CTAORDER_COVER:
            so.direction = DIRECTION_LONG
            so.offset = OFFSET_CLOSE           
        
        # 保存stopOrder对象到字典中
        self.stopOrderDict[stopOrderID] = so
        self.workingStopOrderDict[stopOrderID] = so
        
        return stopOrderID
    
    #----------------------------------------------------------------------
    def cancelStopOrder(self, stopOrderID):
        """撤销停止单"""
        # 检查停止单是否存在
        if stopOrderID in self.workingStopOrderDict:
            so = self.workingStopOrderDict[stopOrderID]
            so.status = STOPORDER_CANCELLED
            del self.workingStopOrderDict[stopOrderID]
            
    #----------------------------------------------------------------------
    def crossLimitOrder(self):
        """基于最新数据撮合限价单"""
        # 先确定会撮合成交的价格
        if self.mode == self.BAR_MODE:
            buyCrossPrice = self.bar.low        # 若买入方向限价单价格高于该价格，则会成交
            sellCrossPrice = self.bar.high      # 若卖出方向限价单价格低于该价格，则会成交
            buyBestCrossPrice = self.bar.open   # 在当前时间点前发出的买入委托可能的最优成交价
            sellBestCrossPrice = self.bar.open  # 在当前时间点前发出的卖出委托可能的最优成交价
        else:
            buyCrossPrice = self.tick.askPrice1
            sellCrossPrice = self.tick.bidPrice1
            buyBestCrossPrice = self.tick.askPrice1
            sellBestCrossPrice = self.tick.bidPrice1
        
        # 遍历限价单字典中的所有限价单
        for orderID, order in self.workingLimitOrderDict.items():
            # 判断是否会成交
            buyCross = order.direction==DIRECTION_LONG and order.price>=buyCrossPrice
            sellCross = order.direction==DIRECTION_SHORT and order.price<=sellCrossPrice
            
            # 如果发生了成交
            if buyCross or sellCross:
                # 推送成交数据
                self.tradeCount += 1            # 成交编号自增1
                tradeID = str(self.tradeCount)
                trade = VtTradeData()
                trade.vtSymbol = order.vtSymbol
                trade.tradeID = tradeID
                trade.vtTradeID = tradeID
                trade.orderID = order.orderID
                trade.vtOrderID = order.orderID
                trade.direction = order.direction
                trade.offset = order.offset
                
                # 以买入为例：
                # 1. 假设当根K线的OHLC分别为：100, 125, 90, 110
                # 2. 假设在上一根K线结束(也是当前K线开始)的时刻，策略发出的委托为限价105
                # 3. 则在实际中的成交价会是100而不是105，因为委托发出时市场的最优价格是100
                if buyCross:
                    trade.price = min(order.price, buyBestCrossPrice)
                    self.strategy.pos += order.totalVolume
                else:
                    trade.price = max(order.price, sellBestCrossPrice)
                    self.strategy.pos -= order.totalVolume
                
                trade.volume = order.totalVolume
                trade.tradeTime = str(self.dt)
                trade.dt = self.dt
                self.strategy.onTrade(trade)
                
                self.tradeDict[tradeID] = trade
                
                # 推送委托数据
                order.tradedVolume = order.totalVolume
                order.status = STATUS_ALLTRADED
                self.strategy.onOrder(order)
                
                # 从字典中删除该限价单
                del self.workingLimitOrderDict[orderID]
                
    #----------------------------------------------------------------------
    def crossStopOrder(self):
        """基于最新数据撮合停止单"""
        # 先确定会撮合成交的价格，这里和限价单规则相反
        if self.mode == self.BAR_MODE:
            buyCrossPrice = self.bar.high    # 若买入方向停止单价格低于该价格，则会成交
            sellCrossPrice = self.bar.low    # 若卖出方向限价单价格高于该价格，则会成交
            bestCrossPrice = self.bar.open   # 最优成交价，买入停止单不能低于，卖出停止单不能高于
        else:
            buyCrossPrice = self.tick.lastPrice
            sellCrossPrice = self.tick.lastPrice
            bestCrossPrice = self.tick.lastPrice
        
        # 遍历停止单字典中的所有停止单
        for stopOrderID, so in self.workingStopOrderDict.items():
            # 判断是否会成交
            buyCross = so.direction==DIRECTION_LONG and so.price<=buyCrossPrice
            sellCross = so.direction==DIRECTION_SHORT and so.price>=sellCrossPrice
            
            # 如果发生了成交
            if buyCross or sellCross:
                # 推送成交数据
                self.tradeCount += 1            # 成交编号自增1
                tradeID = str(self.tradeCount)
                trade = VtTradeData()
                trade.vtSymbol = so.vtSymbol
                trade.tradeID = tradeID
                trade.vtTradeID = tradeID
                
                if buyCross:
                    self.strategy.pos += so.volume
                    trade.price = max(bestCrossPrice, so.price)
                else:
                    self.strategy.pos -= so.volume
                    trade.price = min(bestCrossPrice, so.price)                
                
                self.limitOrderCount += 1
                orderID = str(self.limitOrderCount)
                trade.orderID = orderID
                trade.vtOrderID = orderID
                
                trade.direction = so.direction
                trade.offset = so.offset
                trade.volume = so.volume
                trade.tradeTime = str(self.dt)
                trade.dt = self.dt
                self.strategy.onTrade(trade)
                
                self.tradeDict[tradeID] = trade
                
                # 推送委托数据
                so.status = STOPORDER_TRIGGERED
                
                order = VtOrderData()
                order.vtSymbol = so.vtSymbol
                order.symbol = so.vtSymbol
                order.orderID = orderID
                order.vtOrderID = orderID
                order.direction = so.direction
                order.offset = so.offset
                order.price = so.price
                order.totalVolume = so.volume
                order.tradedVolume = so.volume
                order.status = STATUS_ALLTRADED
                order.orderTime = trade.tradeTime
                self.strategy.onOrder(order)
                
                self.limitOrderDict[orderID] = order
                
                # 从字典中删除该限价单
                del self.workingStopOrderDict[stopOrderID]        

    #----------------------------------------------------------------------
    def insertData(self, dbName, collectionName, data):
        """考虑到回测中不允许向数据库插入数据，防止实盘交易中的一些代码出错"""
        pass
    
    #----------------------------------------------------------------------
    def loadBar(self, dbName, collectionName, startDate):
        """直接返回初始化数据列表中的Bar"""
        return self.initData
    
    #----------------------------------------------------------------------
    def loadTick(self, dbName, collectionName, startDate):
        """直接返回初始化数据列表中的Tick"""
        return self.initData
    
    #----------------------------------------------------------------------
    def writeCtaLog(self, content):
        """记录日志"""
        log = str(self.dt) + ' ' + content 
        self.logList.append(log)

        # 写入本地log日志
        logging.info(content)
        
    #----------------------------------------------------------------------
    def output(self, content):
        """输出内容"""
        print str(datetime.now()) + "\t" + content

    #----------------------------------------------------------------------
    def calculateBacktestingResult(self):
        """
        计算回测结果
        """
        self.output(u'计算回测结果')
        
        # 首先基于回测后的成交记录，计算每笔交易的盈亏
        resultDict = OrderedDict()  # 交易结果记录
        longTrade = []              # 未平仓的多头交易
        shortTrade = []             # 未平仓的空头交易

        for trade in self.tradeDict.values():
            # 多头交易
            if trade.direction == DIRECTION_LONG:
                # 如果尚无空头交易
                if not shortTrade:
                    longTrade.append(trade)
                # 当前多头交易为平空
                else:
                    entryTrade = shortTrade.pop(0)

                    result = TradingResult(entryTrade.price, trade.price, -trade.volume,
                                           self.rate, self.slippage, self.size)

                    resultDict[trade.dt] = result

                    self.writeCtaLog(u'{0},short:{1},{2},cover:{3},vol:{4},{5}'
                                .format(entryTrade.tradeTime, entryTrade.price,trade.tradeTime,trade.price, trade.volume,result.pnl))

            # 空头交易        
            else:
                # 如果尚无多头交易
                if not longTrade:
                    shortTrade.append(trade)
                # 当前空头交易为平多
                else:
                    entryTrade = longTrade.pop(0)

                    result = TradingResult(entryTrade.price, trade.price, trade.volume,
                                           self.rate, self.slippage, self.size)
                    resultDict[trade.dt] = result

                    self.writeCtaLog(u'{0},buy:{1},{2},sell:{3},vol:{4},{5}'
                                .format(entryTrade.tradeTime, entryTrade.price,trade.tradeTime,trade.price, trade.volume,result.pnl))

        # 检查是否有交易
        if not resultDict:
            self.output(u'无交易结果')
            return {}
        
        # 然后基于每笔交易的结果，我们可以计算具体的盈亏曲线和最大回撤等
        capital = 0             # 资金
        maxCapital = 0          # 资金最高净值
        drawdown = 0            # 回撤
        
        totalResult = 0         # 总成交数量
        totalTurnover = 0       # 总成交金额（合约面值）
        totalCommission = 0     # 总手续费
        totalSlippage = 0       # 总滑点
        
        timeList = []           # 时间序列
        pnlList = []            # 每笔盈亏序列
        capitalList = []        # 盈亏汇总的时间序列
        drawdownList = []       # 回撤的时间序列
        
        for time, result in resultDict.items():
            capital += result.pnl
            maxCapital = max(capital, maxCapital)
            drawdown = capital - maxCapital
            
            pnlList.append(result.pnl)
            timeList.append(time)
            capitalList.append(capital)
            drawdownList.append(drawdown)
            
            totalResult += 1
            totalTurnover += result.turnover
            totalCommission += result.commission
            totalSlippage += result.slippage

        # 返回回测结果
        d = {}
        d['capital'] = capital
        d['maxCapital'] = maxCapital
        d['drawdown'] = drawdown
        d['totalResult'] = totalResult
        d['totalTurnover'] = totalTurnover
        d['totalCommission'] = totalCommission
        d['totalSlippage'] = totalSlippage
        d['timeList'] = timeList
        d['pnlList'] = pnlList
        d['capitalList'] = capitalList
        d['drawdownList'] = drawdownList
        return d

    #----------------------------------------------------------------------
    def showBacktestingResult(self):
        """显示回测结果"""
        d = self.calculateBacktestingResult()

        if len(d)== 0:
            self.output(u'无交易结果')
            return
        # 输出
        self.output('-' * 30)
        self.output(u'第一笔交易：\t%s' % d['timeList'][0])
        self.output(u'最后一笔交易：\t%s' % d['timeList'][-1])

        self.output(u'总交易次数：\t%s' % formatNumber(d['totalResult']))
        self.output(u'总盈亏：\t%s' % formatNumber(d['capital']))
        self.output(u'最大回撤: \t%s' % formatNumber(min(d['drawdownList'])))

        self.output(u'平均每笔盈利：\t%s' %formatNumber(d['capital']/d['totalResult']))
        self.output(u'平均每笔滑点：\t%s' %formatNumber(d['totalSlippage']/d['totalResult']))
        self.output(u'平均每笔佣金：\t%s' %formatNumber(d['totalCommission']/d['totalResult']))
            
        # 绘图
        #import matplotlib.pyplot as plt
        
        #pCapital = plt.subplot(3, 1, 1)
        #pCapital.set_ylabel("capital")
        #pCapital.plot(d['capitalList'])
        
        #pDD = plt.subplot(3, 1, 2)
        #pDD.set_ylabel("DD")
        #pDD.bar(range(len(d['drawdownList'])), d['drawdownList'])
        
        #pPnl = plt.subplot(3, 1, 3)
        #pPnl.set_ylabel("pnl")
        #pPnl.hist(d['pnlList'], bins=50)
        
        #plt.show()
    
    #----------------------------------------------------------------------
    def putStrategyEvent(self, name):
        """发送策略更新事件，回测中忽略"""
        pass

    #----------------------------------------------------------------------
    def setSlippage(self, slippage):
        """设置滑点点数"""
        self.slippage = slippage
        
    #----------------------------------------------------------------------
    def setSize(self, size):
        """设置合约大小"""
        self.size = size
        
    #----------------------------------------------------------------------
    def setRate(self, rate):
        """设置佣金比例"""
        self.rate = rate

    #----------------------------------------------------------------------
    def runOptimization(self, strategyClass, optimizationSetting):
        """优化参数"""
        # 获取优化设置
        settingList = optimizationSetting.generateSetting()
        targetName = optimizationSetting.optimizeTarget

        # 检查参数设置问题
        if not settingList or not targetName:
            self.output(u'优化设置有问题，请检查')

        # 遍历优化
        resultList = []
        for setting in settingList:
            self.clearBacktestingResult()
            self.output('-' * 30)
            self.output('setting: %s' %str(setting))
            self.initStrategy(strategyClass, setting)
            self.runBacktesting()
            d = self.calculateBacktestingResult()
            try:
                targetValue = d[targetName]
            except KeyError:
                targetValue = 0
            resultList.append(([str(setting)], targetValue))

        # 显示结果
        resultList.sort(reverse=True, key=lambda result:result[1])
        self.output('-' * 30)
        self.output(u'优化结果：')
        for result in resultList:
            self.output(u'%s: %s' %(result[0], result[1]))

    #----------------------------------------------------------------------
    def clearBacktestingResult(self):
        """清空之前回测的结果"""
        # 清空限价单相关
        self.limitOrderCount = 0
        self.limitOrderDict.clear()
        self.workingLimitOrderDict.clear()

        # 清空停止单相关
        self.stopOrderCount = 0
        self.stopOrderDict.clear()
        self.workingStopOrderDict.clear()

        # 清空成交相关
        self.tradeCount = 0
        self.tradeDict.clear()


########################################################################
class TradingResult(object):
    """每笔交易的结果"""

    #----------------------------------------------------------------------
    def __init__(self, entry, exit, volume, rate, slippage, size):
        """Constructor"""
        self.entry = entry      # 开仓价格
        self.exit = exit        # 平仓价格
        self.volume = volume    # 交易数量（+/-代表方向）

        self.turnover = (self.entry+self.exit)*size         # 成交金额
        self.commission = self.turnover*rate                # 手续费成本
        self.slippage = slippage*2*size                     # 滑点成本
        self.pnl = ((self.exit - self.entry) * volume * size
                    - self.commission - self.slippage)      # 净盈亏


########################################################################
class OptimizationSetting(object):
    """优化设置"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.paramDict = OrderedDict()

        self.optimizeTarget = ''        # 优化目标字段

    #----------------------------------------------------------------------
    def addParameter(self, name, start, end, step):
        """增加优化参数"""
        if end <= start:
            print u'参数起始点必须小于终止点'
            return

        if step <= 0:
            print u'参数布进必须大于0'
            return

        l = []
        param = start

        while param <= end:
            l.append(param)
            param += step

        self.paramDict[name] = l

    #----------------------------------------------------------------------
    def generateSetting(self):
        """生成优化参数组合"""
        # 参数名的列表
        nameList = self.paramDict.keys()
        paramList = self.paramDict.values()

        # 使用迭代工具生产参数对组合
        productList = list(product(*paramList))

        # 把参数对组合打包到一个个字典组成的列表中
        settingList = []
        for p in productList:
            d = dict(zip(nameList, p))
            settingList.append(d)

        return settingList

    #----------------------------------------------------------------------
    def setOptimizeTarget(self, target):
        """设置优化目标字段"""
        self.optimizeTarget = target


#----------------------------------------------------------------------
def formatNumber(n):
    """格式化数字到字符串"""
    n = round(n, 2)         # 保留两位小数
    return format(n, ',')   # 加上千分符



if __name__ == '__main__':
    # 以下内容是一段回测脚本的演示，用户可以根据自己的需求修改
    # 建议使用ipython notebook或者spyder来做回测
    # 同样可以在命令模式下进行回测（一行一行输入运行）
    from ctaDemo import *
    
    # 创建回测引擎
    engine = BacktestingEngine()
    
    # 设置引擎的回测模式为K线
    engine.setBacktestingMode(engine.BAR_MODE)

    # 设置回测用的数据起始日期
    engine.setStartDate('20110101')
    
    # 载入历史数据到引擎中
    engine.setDatabase(MINUTE_DB_NAME, 'IF0000')
    
    # 设置产品相关参数
    engine.setSlippage(0.2)     # 股指1跳
    engine.setRate(0.3/10000)   # 万0.3
    engine.setSize(300)         # 股指合约大小    
    
    # 在引擎中创建策略对象
    engine.initStrategy(DoubleEmaDemo, {})
    
    # 开始跑回测
    engine.runBacktesting()
    
    # 显示回测结果
    # spyder或者ipython notebook中运行时，会弹出盈亏曲线图
    # 直接在cmd中回测则只会打印一些回测数值
    engine.showBacktestingResult()
    
    