import multiprocessing
from time import sleep
from datetime import datetime, time
from logging import INFO

from vnpy.event import EventEngine
from vnpy.trader.setting import SETTINGS
from vnpy.trader.engine import MainEngine
from vnpy.gateway.bitfinex import BitfinexGateway

# from vnpy.gateway.ctp import CtpGateway
from vnpy.app.cta_strategy import CtaStrategyApp
from vnpy.app.cta_strategy.base import EVENT_CTA_LOG

SETTINGS["log.active"] = True
SETTINGS["log.level"] = INFO
SETTINGS["log.console"] = True

bitfinex_erikgqp8645_setting = {
        "key": "zXhnDgjZL7gUw9LjkksrmLjoCetyzyh7X6WxeajmfxJ",
        "secret": "cWc8g9ougKU2NQUUzgEoovtyPHScLR1eJXM0D0pse0v",
        "session": 3,
        "proxy_host": "",
        "proxy_port": 1080,
        "margin": "True"
    }


def run_child():
    """
    Running in the child process.
    """
    SETTINGS["log.file"] = True

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(BitfinexGateway)
    cta_engine = main_engine.add_app(CtaStrategyApp)
    main_engine.write_log("主引擎创建成功")

    log_engine = main_engine.get_engine("log")
    event_engine.register(EVENT_CTA_LOG, log_engine.process_log_event)
    main_engine.write_log("注册日志事件监听")

    main_engine.connect(bitfinex_erikgqp8645_setting, "BitfinexGateway")
    main_engine.write_log("连接BFX接口")

    sleep(10)

    cta_engine.init_engine()
    main_engine.write_log("CTA策略初始化完成")

    cta_engine.init_all_strategies()
    sleep(60)  # Leave enough time to complete strategy initialization
    main_engine.write_log("CTA策略全部初始化")

    cta_engine.start_all_strategies()
    main_engine.write_log("CTA策略全部启动")

    while True:
        sleep(1)


# def run_parent():
#     """
#     Running in the parent process.
#     """
#     print("启动BFX策略守护父进程")
#
#     # Chinese futures market trading period (day/night)
#     # 夜盘和日盘的开盘时间，数字货币不需要
#     # DAY_START = time(8, 45)
#     # DAY_END = time(15, 30)
#     #
#     # NIGHT_START = time(20, 45)
#     # NIGHT_END = time(2, 45)
#
#     child_process = None
#
#     while True:
#         current_time = datetime.now().time()
#         trading = False
#
#         # Check whether in trading period
#         # if (
#         #         (current_time >= DAY_START and current_time <= DAY_END)
#         #         or (current_time >= NIGHT_START)
#         #         or (current_time <= NIGHT_END)
#         # ):
#             # trading = True
#
#         # Start child process in trading period
#         if trading and child_process is None:
#             print("启动子进程")
#             child_process = multiprocessing.Process(target=run_child)
#             child_process.start()
#             print("子进程启动成功")
#
#         # 非记录时间则退出子进程
#         # if not trading and child_process is not None:
#         #     print("关闭子进程")
#         #     child_process.terminate()
#         #     child_process.join()
#         #     child_process = None
#         #     print("子进程关闭成功")
#
#         sleep(5)


if __name__ == "__main__":
    # run_parent() # 不需要守护进程，用进程守护的程序运行
    run_child()
