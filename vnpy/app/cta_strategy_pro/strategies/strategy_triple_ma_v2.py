# encoding: UTF-8

# 首先写系统内置模块
import os
import traceback

from datetime import datetime, timedelta
from collections import OrderedDict

# 其次，导入vnpy的基础模块
from vnpy.app.cta_strategy_pro import (
    CtaTemplate,
    StopOrder,
    Direction,
    Offset,
    Status,
    TickData,
    BarData,
    TradeData,
    OrderData,
    CtaPolicy,
    CtaGrid,
    CtaMinuteBar
)

from vnpy.trader.utility import get_underlying_symbol, round_to, append_data, extract_vt_symbol
from vnpy.trader.util_wechat import send_wx_msg


class TripleMa_Policy(CtaPolicy):
    """
    v1 重构海龟策略执行例子
    v2 增加多单持仓/空单持仓数量；增加限制正向加仓次数
    """

    def __init__(self, strategy):
        super(TripleMa_Policy, self).__init__(strategy)

        # 多/空
        self.tns_direction = ''

        # 增加观测信号
        self.sub_tns = {}

        # 事务开启后，最高价/最低价
        self.tns_high_price = 0
        self.tns_low_price = 0

        # 事务首次开仓价
        self.tns_open_price = 0
        # 最后一次顺势加仓价格
        self.last_open_price = 0
        # 最后一次逆势加仓价格
        self.last_under_open_price = 0

        # 事务止损价
        self.tns_stop_price = 0

        # 高位回落或低位回升x跳,离场
        self.tns_rtn_pips = 0

        # 允许加仓
        self.allow_add_pos = False
        # 顺势可加仓次数
        self.add_pos_count_above_first_price = 0
        # 逆势可加仓次数
        self.add_pos_count_under_first_price = 0

    def to_json(self):
        j = super(TripleMa_Policy, self).to_json()

        j['tns_direction'] = self.tns_direction
        j['sub_tns'] = self.sub_tns
        j['tns_high_price'] = self.tns_high_price
        j['tns_low_price'] = self.tns_low_price
        j['tns_open_price'] = self.tns_open_price
        j['last_open_price'] = self.last_open_price
        j['last_under_open_price'] = self.last_under_open_price

        j['tns_stop_price'] = self.tns_stop_price
        j['tns_rtn_pips'] = self.tns_rtn_pips

        j['allow_add_pos'] = self.allow_add_pos
        j['add_pos_count_above_first_price'] = self.add_pos_count_above_first_price
        j['add_pos_count_under_first_price'] = self.add_pos_count_under_first_price

        return j

    def from_json(self, json_data):
        super(TripleMa_Policy, self).from_json(json_data)

        self.tns_direction = json_data.get('tns_direction', '')
        self.sub_tns = json_data.get('sub_tns', {})
        self.tns_high_price = json_data.get('tns_low_price', 0)
        self.tns_low_price = json_data.get('tns_low_price', 0)
        self.tns_open_price = json_data.get('tns_open_price', 0)
        self.last_open_price = json_data.get('last_open_price', 0)
        self.last_under_open_price = json_data.get('last_under_open_price', 0)
        self.tns_stop_price = json_data.get('tns_stop_price', 0)
        self.tns_rtn_pips = json_data.get('tns_rtn_pips', 0)

        self.allow_add_pos = json_data.get('allow_add_pos', False)
        self.add_pos_count_above_first_price = json_data.get('add_pos_count_above_first_price', 0)
        self.add_pos_count_under_first_price = json_data.get('add_pos_count_under_first_price', 0)

    def clean(self):
        self.sub_tns = {}
        self.tns_high_price = 0
        self.tns_low_price = 0
        self.tns_open_price = 0
        self.last_open_price = 0
        self.last_under_open_price = 0
        self.tns_stop_price = 0
        self.tns_rtn_pips = 0

        self.allow_add_pos = False
        self.add_pos_count_above_first_price = 0
        self.add_pos_count_under_first_price = 0


class Strategy_TripleMa_v2(CtaTemplate):
    """螺纹钢、5分钟级别、三均线策略
    策略：
    10，20，120均线，120均线做多空过滤
    MA120之上
        MA10 上穿 MA20，金叉，做多
        MA10 下穿 MA20，死叉，平多
    MA120之下
        MA10 下穿 MA20，死叉，做空
        MA10 上穿 MA20，金叉，平空

    # 回测要求：
    使用1分钟数据回测
    # 实盘要求：
    使用tick行情

    V2：
    使用增强版策略模板
    使用指数行情，主力合约交易
    使用网格保存持仓

    """
    author = u'李来佳'

    max_invest_pos = 10  # 投资volume总数, 设置为0，则不限制
    max_invest_margin = 0  # 最大投资保证金，设置为0， 则不限制
    max_invest_percent = 0  # 最大投资仓位%， 0~100，
    single_lost_percent = 1  # 单次投入冒得风险比率,例如1%， 就是资金1%得亏损风险
    single_invest_pos = 1  # 单次固定开仓手数

    x_minute = 15  # K线分钟数
    x_atr_len = 20  # 平均波动周期 ATR Length
    x_ma1_len = 10
    x_ma2_len = 20
    x_ma3_len = 120

    atr_value = 0  # K线得ATR均值

    # 外部参数设置清单
    parameters = ["max_invest_pos", "max_invest_margin", "max_invest_percent", "single_lost_percent",
                  "add_pos_under_price_count", "add_pos_above_price_count",
                  "x_atr_len", "x_minute",
                  "x_ma1_len", "x_ma2_len", "x_ma3_len",
                  "backtesting"]

    # 显示在界面上得变量
    variables = ["atr_value"]

    # ----------------------------------------------------------------------
    def __init__(self, cta_engine, strategy_name, vt_symbol, setting=None):
        """Constructor"""
        super(Strategy_TripleMa_v2, self).__init__(
            cta_engine, strategy_name, vt_symbol, setting
        )

        self.kline_x = None  # x分钟K线

        # 创建一个策略规则
        self.policy = TripleMa_Policy(strategy=self)

        self.add_pos_under_price_count = 0  # 逆势加仓次数
        self.add_pos_above_price_count = 0  # 正向加仓次数

        if setting:
            # 根据配置文件更新参数
            self.update_setting(setting)

            # 创建的M5 K线(使用分钟bar）
            kline_setting = {}
            kline_setting['name'] = u'M{}'.format(self.x_minute)  # k线名称
            kline_setting['bar_interval'] = self.x_minute  # K线的Bar时长
            kline_setting['para_ma1_len'] = self.x_ma1_len  # 第1条均线
            kline_setting['para_ma2_len'] = self.x_ma2_len  # 第2条均线
            kline_setting['para_ma3_len'] = self.x_ma3_len  # 第3条均线
            kline_setting['para_atr1_len'] = self.x_atr_len  # ATR
            kline_setting['para_pre_len'] = 30  # 前高/前低
            kline_setting['price_tick'] = self.price_tick
            kline_setting['underly_symbol'] = get_underlying_symbol(vt_symbol).upper()
            self.kline_x = CtaMinuteBar(self, self.on_bar_x, kline_setting)
            self.klines.update({self.kline_x.name: self.kline_x})

    # ----------------------------------------------------------------------
    def on_init(self):
        """初始化 """
        self.write_log(u'策略初始化')
        if self.inited:
            self.write_log(u'已经初始化过，不再执行')
            return

        self.pos = 0  # 初始化持仓
        self.entrust = 0  # 初始化委托状态
        if not self.backtesting:

            # 这里是使用通达信的历史数据
            if self.init_data_from_tdx():
                self.inited = True
            else:
                self.write_error(u'从pytdx初始数据失败')
                return

            # 从本地持久化json文件中，恢复policy的记录数据
            self.policy.load()
            if self.add_pos_under_price_count > 0 or self.add_pos_above_price_count > 0:
                self.policy.allow_add_pos = True

            msg = u'{}初始化,{} 多{}手,空:{}手'.format(self.strategy_name, self.vt_symbol, self.position.long_pos,
                                                self.position.short_pos)
            send_wx_msg(msg)
        else:
            self.inited = True

        self.put_event()
        self.write_log(u'策略初始化完成')

    def init_data_from_tdx(self):
        """从通达信初始化数据"""
        try:
            from vnpy.data.tdx.tdx_future_data import TdxFutureData

            # 优先从本地缓存文件，获取缓存
            last_bar_dt = self.load_klines_from_cache()

            # 创建接口
            tdx = TdxFutureData(self)

            # 开始时间
            if last_bar_dt:
                start_dt = last_bar_dt - timedelta(days=2)
            else:
                start_dt = datetime.now() - timedelta(days=30)

            # 通达信返回得bar，datetime属性是bar的结束时间，所以不能使用callback函数自动推送Bar
            # 这里可以直接取5分钟，也可以取一分钟数据
            result, min1_bars = tdx.get_bars(symbol=self.idx_symbol, period='1min', callback=None, bar_freq=1,
                                             start_dt=start_dt)

            if not result:
                self.write_error(u'未能取回数据')
                return False

            for bar in min1_bars:
                if last_bar_dt and bar.datetime < last_bar_dt:
                    continue
                self.cur_datetime = bar.datetime
                bar.datetime = bar.datetime - timedelta(minutes=1)
                bar.time = bar.datetime.strftime('%H:%M:%S')
                self.cur_99_price = bar.close_price
                self.kline_x.add_bar(bar, bar_freq=1)

            return True

        except Exception as ex:
            self.write_error(u'init_data_from_tdx Exception:{},{}'.format(str(ex), traceback.format_exc()))
            return False

    def sync_data(self):
        """同步更新数据"""
        if not self.backtesting:
            self.write_log(u'保存k线缓存数据')
            self.save_klines_to_cache()

        if self.inited and self.trading:
            self.write_log(u'保存policy数据')
            self.policy.save()

    def on_start(self):
        """启动策略（必须由用户继承实现）"""
        self.write_log(u'启动')
        self.trading = True
        self.put_event()

    # ----------------------------------------------------------------------
    def on_stop(self):
        """停止策略（必须由用户继承实现）"""
        self.active_orders.clear()
        self.pos = 0
        self.entrust = 0

        self.write_log(u'停止')
        self.put_event()

    # ----------------------------------------------------------------------
    def on_trade(self, trade: TradeData):
        """交易更新"""
        self.write_log(u'{},OnTrade(),当前持仓：{} '.format(self.cur_datetime, self.position.pos))

        dist_record = OrderedDict()
        if self.backtesting:
            dist_record['datetime'] = trade.datetime
        else:
            dist_record['datetime'] = self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S')

        dist_record['source_vol'] = trade.volume
        dist_record['price'] = trade.price
        dist_record['symbol'] = trade.vt_symbol

        if trade.direction == Direction.LONG and trade.offset == Offset.OPEN:
            dist_record['operation'] = 'buy'
            self.position.open_pos(trade.direction, volume=trade.volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos

        if trade.direction == Direction.SHORT and trade.offset == Offset.OPEN:
            dist_record['operation'] = 'short'
            self.position.open_pos(trade.direction, volume=trade.volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos

        if trade.direction == Direction.LONG and trade.offset != Offset.OPEN:
            dist_record['operation'] = 'cover'
            self.position.close_pos(trade.direction, volume=trade.volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos

        if trade.direction == Direction.SHORT and trade.offset != Offset.OPEN:
            dist_record['operation'] = 'sell'
            self.position.close_pos(trade.direction, volume=trade.volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos

        self.save_dist(dist_record)
        self.pos = self.position.pos

    # ----------------------------------------------------------------------
    def on_order(self, order: OrderData):
        """报单更新"""
        self.write_log(
            u'OnOrder()报单更新:{}'.format(order.__dict__))

        if order.vt_orderid in self.active_orders:
            # 全部成交
            if order.status == Status.ALLTRADED:
                self.on_order_all_traded(order)

            # 撤单(含部分成交后拒单）/拒单
            elif order.status in [Status.CANCELLED, Status.REJECTED]:
                if order.status == Status.REJECTED:
                    self.send_wechat(f'委托单被拒:{order.__dict__}')

                if order.offset == Offset.OPEN:
                    self.on_order_open_canceled(order)
                else:
                    self.on_order_close_canceled(order)
        else:
            self.write_error(f'委托单{order.vt_orderid}不在本策略的活动订单列表中')

        if len(self.active_orders) == 0:
            self.entrust = 0

        self.put_event()  # 更新监控事件

    def on_order_all_traded(self, order: OrderData):
        """委托单全部成交"""
        order_info = self.active_orders.get(order.vt_orderid)
        grid = order_info.get('grid', None)
        if grid:
            # 移除grid的委托单中order_id
            if order.vt_orderid in grid.order_ids:
                grid.order_ids.remove(order.vt_orderid)

            # 网格的所有委托单已经执行完毕
            if len(grid.order_ids) == 0:
                grid.order_status = False
                grid.traded_volume = 0

                # 平仓完毕（cover， sell）
                if order.offset != Offset.OPEN:
                    grid.open_status = False
                    grid.close_status = True

                    self.write_log(f'{grid.direction.value}单已平仓完毕,order_price:{order.price}' +
                                   f',volume:{order.volume}')

                    self.write_log(f'移除网格:{grid.to_json()}')
                    self.gt.remove_grids_by_ids(direction=grid.direction, ids=[grid.id])

                # 开仓完毕( buy, short)
                else:
                    grid.open_status = True
                    self.write_log(f'{grid.direction.value}单已开仓完毕,order_price:{order.price}' +
                                   f',volume:{order.volume}')

            # 网格的所有委托单部分执行完毕
            else:
                old_traded_volume = grid.traded_volume
                grid.traded_volume += order.volume

                self.write_log(f'{grid.direction.value}单部分{order.offset}仓，' +
                               f'网格volume:{grid.volume}, traded_volume:{old_traded_volume}=>{grid.traded_volume}')

                self.write_log(f'剩余委托单号:{grid.order_ids}')

        # 在策略得活动订单中，移除
        self.active_orders.pop(order.vt_orderid, None)

    def on_order_open_canceled(self, order: OrderData):
        """开仓委托单撤单/部分成交/拒单"""
        self.write_log(f'委托单{order.status.value}')

        order_info = self.active_orders.get(order.vt_orderid)
        grid = order_info.get('grid', None)
        if grid:
            # 移除grid的委托单中order_id
            if order.vt_orderid in grid.order_ids:
                self.write_log(f'网格移除开仓委托单号{order.vt_orderid}')
                grid.order_ids.remove(order.vt_orderid)

            # 网格的所有委托单已经执行完毕
            if len(grid.order_ids) == 0:
                grid.order_status = False
            else:
                self.write_log(f'网格剩余开仓委托单号:{grid.order_ids}')

            # 撤单得部分成交
            if order.traded > 0:
                self.write_log(f'网格{grid.direction.value}单，' +
                               f'计划开仓{grid.volume}' +
                               f'已开仓:{grid.traded_volume} =》{grid.traded_volume + order.traded}')
                grid.traded_volume += order.traded

            if len(grid.order_ids) == 0 and grid.order_status is False and grid.traded_volume == 0:
                self.gt.remove_grids_by_ids(direction=grid.direction, ids=[grid.id])

        # 在策略得活动订单中，移除
        self.active_orders.pop(order.vt_orderid, None)

    def on_order_close_canceled(self, order: OrderData):
        """"平委托单撤单/部分成交/拒单"""
        self.write_log(f'委托单{order.status.value}')

        order_info = self.active_orders.get(order.vt_orderid)
        grid = order_info.get('grid', None)
        if grid:
            # 移除grid的委托单中order_id
            if order.vt_orderid in grid.order_ids:
                self.write_log(f'网格移除平仓委托单号{order.vt_orderid}')
                grid.order_ids.remove(order.vt_orderid)

            # 网格的所有委托单已经执行完毕
            if len(grid.order_ids) == 0:
                grid.order_status = False
            else:
                self.write_log(f'网格剩余平仓委托单号:{grid.order_ids}')

            # 撤单得部分成交
            if order.traded > 0:
                self.write_log(f'网格{grid.direction.value}单，' +
                               f'计划平仓{grid.volume}' +
                               f'已平仓:{grid.traded_volume} =》{grid.traded_volume + order.traded}')
                grid.traded_volume += order.traded

        # 在策略得活动订单中，移除
        self.active_orders.pop(order.vt_orderid, None)

    # ----------------------------------------------------------------------
    def on_stop_order(self, stop_order: StopOrder):
        """停止单更新"""
        self.write_log(u'{},停止单触发，{}'.format(self.cur_datetime, stop_order.__dict__))
        pass

    # ----------------------------------------------------------------------
    def on_tick(self, tick: TickData):
        """行情更新（实盘运行，从tick导入）
        :type tick: object
        """
        # 首先检查是否是实盘运行还是数据预处理阶段
        if not (self.inited):
            return

        # 更新tick 到dict
        self.tick_dict.update({tick.vt_symbol: tick})

        if tick.vt_symbol == self.vt_symbol:
            # 设置为当前主力tick
            self.cur_mi_tick = tick
            self.cur_mi_price = tick.last_price

        if tick.vt_symbol == self.idx_symbol:
            self.cur_99_tick = tick
            # 更新最新价
            self.cur_99_price = tick.last_price

            if self.cur_mi_tick is None:
                self.write_log(f'主力tick未到达，丢弃当前指数tick:{tick.vt_symbol},价格:{tick.last_price}')
                return
        else:
            # 所有非指数的tick，都直接返回
            return

        if (tick.datetime.hour >= 3 and tick.datetime.hour <= 8) or (
                tick.datetime.hour >= 16 and tick.datetime.hour <= 20):
            self.write_log(u'休市/集合竞价排名时数据不处理')
            return

        # 更新策略执行的时间
        self.cur_datetime = tick.datetime

        # 推送Tick到lineM5
        self.kline_x.on_tick(tick)

        self.tns_update_price()

        if self.position.pos != 0:
            self.tns_check_stop()
            self.tns_add_logic()
        else:
            self.tns_open_logic()

    # ----------------------------------------------------------------------
    def on_bar(self, bar: BarData):
        """分钟K线数据更新（仅用于回测时，从策略外部调用)"""

        # 更新策略执行的时间（用于回测时记录发生的时间）
        # 回测数据传送的bar.datetime，为bar的开始时间，所以，到达策略时，当前时间为bar的结束时间
        # 本策略采用1分钟bar回测
        self.cur_datetime = bar.datetime + timedelta(minutes=1)
        self.cur_99_price = bar.close_price
        self.cur_mi_price = bar.close_price
        # 推送bar到x分钟K线
        self.kline_x.add_bar(bar)

        # 4、交易逻辑
        # 首先检查是否是实盘运行还是数据预处理阶段
        if not self.inited or not self.trading:
            return

        # 执行撤单逻辑
        self.tns_cancel_logic(dt=self.cur_datetime)

        # 执行事务逻辑判断
        self.tns_update_price()

        if self.position.pos != 0:
            self.tns_check_stop()
            self.tns_add_logic()
        else:
            self.tns_open_logic()

    def on_bar_x(self, bar: BarData):
        """  分钟K线数据更新，实盘时，由self.kline_x的回调"""

        # 调用kline_x的显示bar内容
        self.write_log(self.kline_x.get_last_bar_str())

        # 未初始化完成
        if not self.inited:
            return

        # 更新sub tns的金叉死叉
        sub_tns_count = self.policy.sub_tns.get('count', 0)
        if self.kline_x.ma12_count >= 1 and sub_tns_count <= 0:
            self.write_log(u'{} 死叉 {} => 金叉 :{}'.format(self.cur_datetime, sub_tns_count, self.kline_x.ma12_count))
            self.policy.sub_tns = {'count': self.kline_x.ma12_count, 'price': self.cur_99_price}
        elif self.kline_x.ma12_count <= -1 and sub_tns_count >= 0:
            self.write_log(u'{} 金叉 {} => 死叉 :{}'.format(self.cur_datetime, sub_tns_count, self.kline_x.ma12_count))
            self.policy.sub_tns = {'count': self.kline_x.ma12_count, 'price': self.cur_99_price}

        # 多空事务处理
        self.tns_logic()

    def tns_update_price(self):
        """更新事务的一些跟踪价格"""

        # 持有多仓/空仓时，更新最高价和最低价
        if self.position.pos > 0:
            self.policy.tns_high_price = max(self.cur_99_price, self.kline_x.line_bar[-1].high_price,
                                             self.policy.tns_high_price)
        if self.position.pos < 0:
            if self.policy.tns_low_price == 0:
                self.policy.tns_low_price = self.cur_99_price
            else:
                self.policy.tns_low_price = min(self.cur_99_price, self.kline_x.line_bar[-1].low_price,
                                                self.policy.tns_low_price)

        if self.position.pos == 0:
            self.policy.tns_high_price = 0
            self.policy.tns_low_price = 0

        # 更新ATR
        if len(self.kline_x.line_atr1) > 1 and self.kline_x.line_atr1[-1] > 2 * self.price_tick:
            self.atr_value = max(self.kline_x.line_atr1[-1], 5 * self.price_tick)

            if self.position.pos != 0 and self.policy.allow_add_pos:
                # 2倍的ATR作为跟随止损
                self.policy.tns_rtn_pips = int((self.atr_value * 2) / self.price_tick) + 1

    def tns_logic(self):
        """
        趋势逻辑
        长均线向上，价格在长均线上方时，空趋势/无趋势-》多趋势
        长均线向下，价格在长均线下方时，多趋势/无趋势-》空趋势
        """

        if len(self.kline_x.line_ma3) < 2:
            return

        if self.kline_x.line_ma3[-1] > self.kline_x.line_ma3[-2] and self.cur_99_price > self.kline_x.line_ma3[-1]:
            if self.policy.tns_direction != Direction.LONG:
                self.write_log(u'开启做多趋势事务')
                self.policy.tns_direction = Direction.LONG
                self.policy.tns_count = 0
                self.policy.tns_high_price = self.kline_x.line_pre_high[-1]
                self.policy.tns_low_price = self.kline_x.line_pre_low[-1]
                if self.add_pos_above_price_count > 0 or self.add_pos_under_price_count > 0:
                    self.policy.allow_add_pos = True

                h = OrderedDict()
                h['datetime'] = self.cur_datetime
                h['price'] = self.cur_99_price
                h['direction'] = 'long'
                self.save_tns(h)
            return

        if self.kline_x.line_ma3[-1] < self.kline_x.line_ma3[-2] and self.cur_99_price < self.kline_x.line_ma3[-1]:
            if self.policy.tns_direction != Direction.SHORT:
                self.write_log(u'开启做空趋势事务')
                self.policy.tns_direction = Direction.SHORT
                self.policy.tns_count = 0
                self.policy.tns_high_price = self.kline_x.line_pre_high[-1]
                self.policy.tns_low_price = self.kline_x.line_pre_low[-1]
                if self.add_pos_above_price_count > 0 or self.add_pos_under_price_count > 0:
                    self.policy.allow_add_pos = True
                h = OrderedDict()
                h['datetime'] = self.cur_datetime
                h['price'] = self.cur_99_price
                h['direction'] = 'short'
                self.save_tns(h)

            return

    def tns_open_logic(self):
        """开仓逻辑判断"""

        # 已经开仓，不再判断
        if self.position.pos != 0:
            return

        if self.entrust != 0 or not self.trading:
            return

        # MA10 上穿MA20，
        if self.policy.tns_direction == Direction.LONG \
                and self.kline_x.ma12_count > 0 \
                and self.position.pos == 0:

            if self.tns_buy():
                # 更新开仓价格
                self.policy.tns_open_price = self.cur_99_price
                self.policy.last_open_price = self.cur_99_price
                self.policy.last_under_open_price = self.cur_99_price
                # 更新事务的最高价
                self.policy.high_price_in_long = self.cur_99_price
                # 设置前低为止损价
                self.policy.tns_stop_price = self.kline_x.line_pre_low[-1]
                # 允许顺势加仓/逆势加仓的次数
                self.policy.add_pos_count_under_first_price = self.add_pos_under_price_count
                self.policy.add_pos_count_above_first_price = self.add_pos_above_price_count
            return

        # MA10 下穿MA20，
        if self.policy.tns_direction == Direction.SHORT \
                and self.kline_x.ma12_count < 0 \
                and self.position.pos == 0:

            if self.tns_short():
                # 更新开仓价格
                self.policy.tns_open_price = self.cur_99_price
                self.policy.last_open_price = self.cur_99_price
                self.policy.last_under_open_price = self.cur_99_price
                # 更新最低价
                self.policy.low_price_in_short = self.cur_99_price
                # 设置前高为止损价
                self.policy.tns_stop_price = self.kline_x.line_pre_high[-1]
                # 允许顺势加仓/逆势加仓的次数
                self.policy.add_pos_count_under_first_price = self.add_pos_under_price_count
                self.policy.add_pos_count_above_first_price = self.add_pos_above_price_count

            return

    def tns_get_volume(self, stop_price: float = 0, invest_percent: float = None):
        """获取事务开仓volume
        :param stop_price:存在止损价时,按照最大亏损比例,计算可开仓手数
        :param invest_percent: 当次投资资金比例
        """

        if stop_price == 0 and invest_percent is None:
            return self.single_invest_pos

        volume = 0

        # 从策略引擎获取当前净值，可用资金，当前保证金比例，账号使用资金上限
        balance, avaliable, percent, percent_limit = self.cta_engine.get_account()

        if invest_percent is None:
            invest_percent = self.max_invest_percent

        if invest_percent > self.max_invest_percent:
            invest_percent = self.max_invest_percent

        # 计算当前策略实例，可使用的资金
        invest_money = float(balance * invest_percent / 100)
        invest_money = min(invest_money, avaliable)

        self.write_log(u'账号净值:{},可用:{},仓位:{},上限:{}%,策略投入仓位:{}%'
                       .format(balance, avaliable, percent, percent_limit, invest_percent))

        symbol_size = self.cta_engine.get_size(self.vt_symbol)
        symbol_margin_rate = self.cta_engine.get_margin_rate(self.vt_symbol)
        # 投资资金总额允许的开仓数量
        max_unit = max(1, int(invest_money / (self.cur_mi_price * symbol_size * symbol_margin_rate)))
        self.write_log(u'投资资金总额{}允许的开仓数量：{},当前已经开仓手数:{}'
                       .format(invest_money, max_unit,
                               self.position.long_pos + abs(self.position.short_pos)))
        volume = max_unit

        if stop_price > 0 and stop_price != self.cur_99_price:
            eval_lost_money = balance * self.single_lost_percent / 100
            eval_lost_per_volume = abs(self.cur_99_price - stop_price) * symbol_size
            eval_lost_volume = max(int(eval_lost_money / eval_lost_per_volume), 1)
            new_volume = min(volume, eval_lost_volume)
            if volume != new_volume:
                self.write_log(
                    u'止损 {}% 限制金额:{},最多可使用{}手合约'.format(self.single_lost_percent, eval_lost_money, new_volume))
                volume = new_volume

        return volume

    def tns_add_logic(self):
        """
        加仓逻辑
        # 海龟加仓
        """

        if not self.policy.allow_add_pos:
            return

        if self.entrust != 0 or not self.trading:
            return

        # 加仓策略使用特定pip间隔（例如海龟的N）
        # 根据 ATR更新N
        self.policy.add_pos_on_pips = int(self.atr_value / (2 * self.price_tick))

        # 加多仓
        if self.position.long_pos > 0:
            # 还有允许加多单的额度,价格超过指最后的加仓价格+加仓价格幅度
            if self.policy.add_pos_count_above_first_price > 0 and \
                    self.cur_99_price >= (self.policy.last_open_price + self.policy.add_pos_on_pips * self.price_tick):

                # 这里可以根据风险，来评估你加仓数量，到达止损后，亏损多少
                # 设置新开仓价-2ATR为止损价
                new_stop_price = max(self.policy.tns_stop_price, self.policy.last_open_price - 2 * self.atr_value)

                if self.tns_buy():
                    # 更新开仓价格
                    self.policy.last_open_price = self.cur_99_price
                    self.policy.add_pos_count_above_first_price -= 1

                    self.write_log(u'更新止损价:{}->{}'.format(self.policy.tns_stop_price, new_stop_price))
                    self.policy.tns_stop_price = new_stop_price
                    self.policy.save()
                    self.display_tns()
                return

            # 还有允许逆势加多单的额度,价格低于过指最后的逆势加仓价格- 加仓价格幅度，并且不低于止损价
            if self.policy.add_pos_count_under_first_price > 0 \
                    and self.cur_99_price <= (
                    self.policy.last_under_open_price - self.policy.add_pos_on_pips * self.price_tick) \
                    and self.cur_99_price > self.policy.tns_stop_price:

                if self.tns_buy():
                    # 更新开仓价格
                    self.policy.last_under_open_price = self.cur_99_price
                    self.policy.add_pos_count_under_first_price -= 1
                    self.policy.save()
                    self.display_tns()
                return

        if self.position.short_pos < 0:
            # 还有允许加空单的额度,价格低于指最后的加仓价格 - 加仓价格幅度
            #
            if self.policy.add_pos_count_above_first_price and \
                    self.cur_99_price <= (self.policy.last_open_price - self.policy.add_pos_on_pips * self.price_tick):
                # 设置新开仓价-2ATR为止损价
                new_stop_price = max(self.policy.tns_stop_price, self.policy.last_open_price + 2 * self.atr_value)

                if self.tns_short():
                    # 更新开仓价格
                    self.policy.last_open_price = self.cur_99_price
                    self.write_log(u'更新止损价:{}->{}'.format(self.policy.tns_stop_price, new_stop_price))
                    self.policy.tns_stop_price = new_stop_price
                    self.policy.save()
                    self.display_tns()
                return

            # 还有允许逆势加空单的额度,价格高于过指最后的逆势加仓价格 + 加仓价格幅度，并且不低于止损价
            if self.policy.add_pos_count_under_first_price > 0 \
                    and self.cur_99_price >= (
                    self.policy.last_under_open_price + self.policy.add_pos_on_pips * self.price_tick) \
                    and self.cur_99_price < self.policy.tns_stop_price:

                if self.tns_short():
                    # 更新开仓价格
                    self.policy.last_under_open_price = self.cur_99_price
                    self.policy.add_pos_count_under_first_price -= 1
                    self.policy.save()
                    self.display_tns()
                return

    def tns_check_stop(self):
        """检查持仓止损或"""

        if self.entrust != 0 or not self.trading:
            return

        if self.position.long_pos == 0 and self.position.short_pos == 0:
            return

        if self.position.long_pos > 0:
            # MA10下穿MA20，Ma20拐头，多单离场
            if self.kline_x.ma12_count < 0 and self.kline_x.line_ma2[-1] < self.kline_x.line_ma2[-2]:
                self.write_log(u'{},平仓多单{}手,价格:{}'.format(self.cur_datetime, abs(self.position.pos), self.cur_99_price))
                self.tns_sell()
                return

            # 转空事务
            if self.policy.tns_direction != Direction.LONG:
                self.write_log(
                    u'{},事务与持仓不一致，平仓多单{}手,价格:{}'.format(self.cur_datetime, abs(self.position.pos), self.cur_99_price))
                self.tns_sell()
                return

            # policy 跟随止损
            follow_stop_price = self.policy.tns_high_price - self.policy.tns_rtn_pips * self.price_tick
            if self.policy.tns_rtn_pips > 0 \
                    and self.cur_99_price < follow_stop_price <= self.policy.tns_stop_price:
                self.write_log(
                    u'{},跟随止损，平仓多单{}手,价格:{}'.format(self.cur_datetime, abs(self.position.long_pos), self.cur_99_price))
                self.tns_sell()
                return

            # 固定止损
            if self.policy.tns_stop_price > self.cur_99_price:
                self.write_log(
                    u'{},固定止损，平仓多单{}手,价格:{}'.format(self.cur_datetime, abs(self.position.long_pos), self.cur_99_price))
                self.tns_sell()
                return

        if abs(self.position.short_pos) > 0:
            # MA10上穿MA20，MA20拐头，空单离场
            if self.kline_x.ma12_count > 0 and self.kline_x.line_ma2[-1] > self.kline_x.line_ma2[-2]:
                self.write_log(
                    u'{},平仓空单{}手,价格:{}'.format(self.cur_datetime, abs(self.position.short_pos), self.cur_99_price))
                self.tns_cover()
                return

            # 转多事务
            if self.policy.tns_direction != Direction.SHORT:
                self.write_log(
                    u'{},事务与持仓不一致，平仓空单{}手,价格:{}'.format(self.cur_datetime, abs(self.position.short_pos),
                                                        self.cur_99_price))
                self.tns_cover()
                return

            # 跟随止损
            follow_stop_price = self.policy.tns_low_price + self.policy.tns_rtn_pips * self.price_tick
            if self.policy.tns_rtn_pips > 0 \
                    and self.cur_99_price > follow_stop_price > self.policy.tns_stop_price:
                self.write_log(
                    u'{},跟随止损，平仓空单{}手,价格:{}'.format(self.cur_datetime, abs(self.position.pos), self.cur_99_price))
                self.tns_cover()

                return

            # 固定止损
            if self.cur_99_price > self.policy.tns_stop_price > 0:
                self.write_log(
                    u'{},固定止损，平仓空单{}手,价格:{}'.format(self.cur_datetime, abs(self.position.pos), self.cur_99_price))
                self.tns_cover()
                return

    def tns_buy(self):
        """事务开多"""
        if not self.inited or not self.trading:
            return False

        if self.entrust != 0:
            return False

        # 计算开仓数量
        total_open_count = self.add_pos_under_price_count + self.add_pos_under_price_count + 1
        first_open_volume = self.tns_get_volume(stop_price=self.kline_x.line_pre_low[-1],
                                                invest_percent=self.max_invest_percent / (total_open_count))

        self.write_log(u'{},开仓多单{}手,指数价格:{}，主力价格:{}'
                       .format(self.cur_datetime, first_open_volume, self.cur_99_price, self.cur_mi_price))

        # 创建一个持仓网格， 价格数据以主力合约为准
        grid = CtaGrid(
            direction=Direction.LONG,
            open_price=self.cur_99_price,
            stop_price=self.kline_x.line_pre_low[-1],
            close_price=self.cur_99_price * 2,
            volume=first_open_volume
        )
        # 更新网格的切片，登记当前主力合约数据和开仓数据
        grid.snapshot.update({'mi_symbol': self.vt_symbol,
                              'open_price': self.cur_mi_price,
                              })

        # 发出委托
        order_ids = self.buy(price=self.cur_mi_price,
                             volume=first_open_volume,
                             order_time=self.cur_datetime,
                             vt_symbol=self.vt_symbol,
                             grid=grid)
        if len(order_ids) > 0:
            # 委托成功后，添加至做多队列
            self.gt.dn_grids.append(grid)
            return True

        return False

    def tns_sell(self):
        """事务平多仓"""
        if not self.inited or not self.trading:
            return False

        if self.entrust != 0:
            return False

        for grid in self.gt.get_opened_grids(direction=Direction.LONG):
            # 检查1，检查是否为已委托状态
            if grid.order_status:
                continue

            sell_symbol = grid.snapshot.get('mi_symbol', self.vt_symbol)
            sell_price = self.cta_engine.get_price(sell_symbol) - self.price_tick
            sell_volume = grid.volume - grid.traded_volume

            # 修正持仓
            if sell_volume != grid.volume:
                self.write_log(f'网格多单持仓:{grid.volume},已成交:{grid.traded_volume}, 修正为:{sell_volume}')
                grid.volume = sell_volume
                grid.traded_volume = 0

            # 进一步检查
            if grid.volume == 0:
                grid.open_status = False
                continue

            order_ids = self.sell(price=sell_price,
                                  volume=grid.volume,
                                  vt_symbol=sell_symbol,
                                  order_time=self.cur_datetime, grid=grid)
            if len(order_ids) == 0:
                self.write_error(f'sell失败:{grid.__dict__}')

        return True

    def tns_short(self):
        """事务开空"""
        if not self.inited or not self.trading:
            return False

        if self.entrust != 0:
            return False

        # 计算开仓数量( 总次数：逆势加仓数+ 顺势加仓数 + 首笔）
        total_open_count = self.add_pos_under_price_count + self.add_pos_under_price_count + 1
        first_open_volume = self.tns_get_volume(stop_price=self.kline_x.line_pre_high[-1],
                                                invest_percent=self.max_invest_percent / (total_open_count))

        self.write_log(u'{},开仓空单{}手,指数价格:{}，主力价格:{}'
                       .format(self.cur_datetime, first_open_volume, self.cur_99_price, self.cur_mi_price))

        # 创建一个持仓网格， 价格数据以主力合约为准
        grid = CtaGrid(
            direction=Direction.SHORT,
            open_price=self.cur_99_price,
            stop_price=self.kline_x.line_pre_high[-1],
            close_price=0,
            volume=first_open_volume
        )
        # 更新网格的切片，登记当前主力合约数据和开仓数据
        grid.snapshot.update({'mi_symbol': self.vt_symbol,
                              'open_price': self.cur_mi_price,
                              })

        # 发出委托
        order_ids = self.short(price=self.cur_mi_price,
                               volume=first_open_volume,
                               order_time=self.cur_datetime,
                               vt_symbol=self.vt_symbol,
                               grid=grid)
        if len(order_ids) > 0:
            # 委托成功后，添加至做多队列
            self.gt.up_grids.append(grid)
            return True

        return False

    def tns_cover(self):
        """事务平空仓"""
        if not self.inited or not self.trading:
            return False

        if self.entrust != 0:
            return False

        for grid in self.gt.get_opened_grids(direction=Direction.SHORT):
            # 检查1，检查是否为已委托状态
            if grid.order_status:
                continue

            cover_symbol = grid.snapshot.get('mi_symbol', self.vt_symbol)
            cover_price = self.cta_engine.get_price(cover_symbol) + self.price_tick
            cover_volume = grid.volume - grid.traded_volume

            # 修正持仓
            if cover_volume != grid.volume:
                self.write_log(f'网格空单持仓:{grid.volume},已成交:{grid.traded_volume}, 修正为:{cover_volume}')
                grid.volume = cover_volume
                grid.traded_volume = 0

            # 进一步检查
            if grid.volume == 0:
                grid.open_status = False
                continue

            order_ids = self.cover(price=cover_price,
                                   volume=grid.volume,
                                   vt_symbol=cover_symbol,
                                   order_time=self.cur_datetime, grid=grid)
            if len(order_ids) == 0:
                self.write_error(f'cover失败:{grid.__dict__}')

        return True
