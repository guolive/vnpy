from datetime import datetime


from vnpy.event import Event, EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import QtCore, QtWidgets
from vnpy.trader.event import EVENT_CONTRACT

from ..engine import (
    APP_NAME,
    EVENT_RECORDER_LOG,
    EVENT_RECORDER_UPDATE
)


class RecorderManager(QtWidgets.QWidget):
    """"""

    signal_log = QtCore.pyqtSignal(Event)
    signal_update = QtCore.pyqtSignal(Event)
    signal_contract = QtCore.pyqtSignal(Event)

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        super().__init__()

        self.main_engine = main_engine
        self.event_engine = event_engine
        self.recorder_engine = main_engine.get_engine(APP_NAME)

        self.init_ui()
        self.register_event()
        self.recorder_engine.put_event()

    def init_ui(self):
        """"""
        self.setWindowTitle("Tick行情记录")
        self.resize(1000, 600)

        # Create widgets
        self.symbol_line = QtWidgets.QLineEdit()
        self.symbol_line.setFixedHeight(
            self.symbol_line.sizeHint().height() * 2)

        contracts = self.main_engine.get_all_contracts()
        self.vt_symbols = [contract.vt_symbol for contract in contracts]

        self.symbol_completer = QtWidgets.QCompleter(self.vt_symbols)
        self.symbol_completer.setFilterMode(QtCore.Qt.MatchContains)
        self.symbol_completer.setCompletionMode(
            self.symbol_completer.PopupCompletion)
        self.symbol_line.setCompleter(self.symbol_completer)

        add_tick_button = QtWidgets.QPushButton("添加")
        add_tick_button.clicked.connect(self.add_tick_recording)

        remove_tick_button = QtWidgets.QPushButton("移除")
        remove_tick_button.clicked.connect(self.remove_tick_recording)

        self.tick_recording_edit = QtWidgets.QTextEdit()
        self.tick_recording_edit.setReadOnly(True)

        self.log_edit = QtWidgets.QTextEdit()
        self.log_edit.setReadOnly(True)

        # Set layout
        grid = QtWidgets.QGridLayout()
        grid.addWidget(QtWidgets.QLabel("Tick记录"), 0, 0)
        grid.addWidget(add_tick_button, 0, 1)
        grid.addWidget(remove_tick_button, 0, 2)

        hbox = QtWidgets.QHBoxLayout()
        hbox.addWidget(QtWidgets.QLabel("本地代码"))
        hbox.addWidget(self.symbol_line)
        hbox.addWidget(QtWidgets.QLabel("     "))
        hbox.addLayout(grid)
        hbox.addStretch()

        grid2 = QtWidgets.QGridLayout()
        grid2.addWidget(QtWidgets.QLabel("Tick记录列表"), 0, 0)
        grid2.addWidget(self.tick_recording_edit, 0, 1)

        vbox = QtWidgets.QVBoxLayout()
        vbox.addLayout(hbox)
        vbox.addLayout(grid2)
        self.setLayout(vbox)

    def register_event(self):
        """"""
        self.signal_log.connect(self.process_log_event)
        self.signal_contract.connect(self.process_contract_event)
        self.signal_update.connect(self.process_update_event)

        self.event_engine.register(EVENT_CONTRACT, self.signal_contract.emit)
        self.event_engine.register(
            EVENT_RECORDER_LOG, self.signal_log.emit)
        self.event_engine.register(
            EVENT_RECORDER_UPDATE, self.signal_update.emit)

    def process_log_event(self, event: Event):
        """"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        msg = f"{timestamp}\t{event.data}"
        self.log_edit.append(msg)

    def process_update_event(self, event: Event):
        """"""
        data = event.data

        self.tick_recording_edit.clear()
        tick_text = "\n".join(data["tick"])
        self.tick_recording_edit.setText(tick_text)

    def process_contract_event(self, event: Event):
        """"""
        contract = event.data
        self.vt_symbols.append(contract.vt_symbol)

        model = self.symbol_completer.model()
        model.setStringList(self.vt_symbols)

    def add_tick_recording(self):
        """"""
        vt_symbol = self.symbol_line.text()
        self.recorder_engine.add_tick_recording(vt_symbol)

    def remove_tick_recording(self):
        """"""
        vt_symbol = self.symbol_line.text()
        self.recorder_engine.remove_tick_recording(vt_symbol)
