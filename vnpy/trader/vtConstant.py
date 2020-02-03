# encoding: UTF-8

# 默认空值
EMPTY_STRING = ''
EMPTY_UNICODE = u''
EMPTY_INT = 0
EMPTY_FLOAT = 0.0

# k线颜色
COLOR_RED = u'Red'      # 上升K线
COLOR_BLUE = u'Blue'    # 下降K线
COLOR_EQUAL = u'Equal'  # 平K线

# 方向常量
DIRECTION_NONE = u'无方向'
DIRECTION_LONG = u'多'
DIRECTION_SHORT = u'空'
DIRECTION_UNKNOWN = u'未知'
DIRECTION_NET = u'净'
DIRECTION_SELL = u'卖出'      # IB接口

# 开平常量
OFFSET_NONE = u'无开平'
OFFSET_OPEN = u'开仓'
OFFSET_CLOSE = u'平仓'
OFFSET_FORCECLOSE = u'强平'
OFFSET_CLOSETODAY = u'平今'
OFFSET_CLOSESYESTERDAY = u'平昨'
OFFSET_FORCECOFF = u'强减'
OFFSET_LOCALFORCECLOSE = u'本地强平'
OFFSET_UNKNOWN = u'未知'

# 状态常量
STATUS_NOTTRADED = u'未成交'
STATUS_PARTTRADED = u'部分成交'
STATUS_ALLTRADED = u'全部成交'
STATUS_CANCELLED = u'已撤销'
STATUS_UNKNOWN = u'未知'

# 合约类型常量
PRODUCT_EQUITY = u'股票'
PRODUCT_FUTURES = u'期货'
PRODUCT_OPTION = u'期权'
PRODUCT_INDEX = u'指数'
PRODUCT_COMBINATION = u'组合'
PRODUCT_FOREX = u'外汇'
PRODUCT_UNKNOWN = u'未知'

# 价格类型常量
PRICETYPE_LIMITPRICE = u'限价'
PRICETYPE_MARKETPRICE = u'市价'
PRICETYPE_FAK = u'FAK'
PRICETYPE_FOK = u'FOK'

# 期权类型
OPTION_CALL = u'看涨期权'
OPTION_PUT = u'看跌期权'

# 交易所类型
EXCHANGE_SSE = u'SSE'       # 上交所
EXCHANGE_SZSE = u'SZSE'     # 深交所
EXCHANGE_CFFEX = u'CFFEX'   # 中金所
EXCHANGE_SHFE = u'SHFE'     # 上期所
EXCHANGE_CZCE = u'CZCE'     # 郑商所
EXCHANGE_DCE = u'DCE'       # 大商所
EXCHANGE_UNKNOWN = 'UNKNOWN'# 未知交易所
EXCHANGE_NONE = ''          # 空交易所

EXCHANGE_UNKNOWN = 'UNKNOWN'# 未知交易所
EXCHANGE_NONE = ''          # 空交易所

EXCHANGE_SMART = u'SMART'       # IB智能路由（股票、期权）
EXCHANGE_GLOBEX = u'GLOBEX'     # CME电子交易平台
EXCHANGE_IDEALPRO = u'IDEALPRO' # IB外汇ECN

# 货币类型
CURRENCY_USD = 'USD'            # 美元
CURRENCY_CNY = 'CNY'            # 人民币
CURRENCY_UNKNOWN = 'UNKNOWN'    # 未知货币
