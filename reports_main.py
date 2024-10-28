from PyQt5.QtWidgets import QApplication, QTableWidget, QTableWidgetItem, QMessageBox, QLabel, QSplitter, QTableView, QComboBox, QMainWindow, QWidget, QListWidget, QCalendarWidget, QPushButton, QHBoxLayout, QVBoxLayout, QDialog
from Server_DB import Database
from PyQt5.QtGui import QIcon, QStandardItemModel, QStandardItem
from datetime import datetime
from PyQt5.QtCore import QThread, pyqtSignal, QDate, QModelIndex, Qt
import pandas as pd
from PyQt5.QtGui import QKeyEvent
import os
import subprocess

database_instance = Database()


class WorkerThread(QThread):
    finished = pyqtSignal(str)

    def __init__(self, model, data, headers):
        super().__init__()
        self.model = model
        self.data = data
        self.headers = headers

    def run(self):
        for row in range(len(self.data)):
            for col in range(len(self.headers)):
                item = QStandardItem(str(self.data.iloc[row, col]))
                self.model.setItem(row, col, item)
        self.finished.emit("Reports")
        print("Table Updated")


class FromCalendarDialog(QDialog):
    date_selected = pyqtSignal(QDate)

    def __init__(self, parent=None):
        super().__init__(parent=parent)

        self.calendar = QCalendarWidget(self)
        self.calendar.setGridVisible(True)
        self.calendar.clicked.connect(self.handle_date_selection)

        layout = QVBoxLayout(self)
        layout.addWidget(self.calendar)
        self.setWindowTitle("From date")

    def handle_date_selection(self, date):
        self.date_selected.emit(date)
        self.accept()


class ToCalendarDialog(QDialog):
    date_selected = pyqtSignal(QDate)

    def __init__(self, parent=None):
        super().__init__(parent=parent)

        self.calendar = QCalendarWidget(self)
        self.calendar.setGridVisible(True)
        self.calendar.clicked.connect(self.handle_date_selection)
        layout = QVBoxLayout(self)
        layout.addWidget(self.calendar)
        self.setWindowTitle("To date")

    def handle_date_selection(self, date):
        self.date_selected.emit(date)
        self.accept()


class ExcelWriterThread(QThread):
    def __init__(self, df, filepath):
        super().__init__()
        self.df = df
        self.filepath = filepath

    def run(self):
        self.df.to_excel(self.filepath, index=False)


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        # Set the window title and size
        # self.data = None
        self.thread = None
        self.setWindowTitle('Reports')
        self.resize(800, 600)
        # Create a central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        self.setWindowIcon(QIcon("D:\\HULK\\builds\\Reports\\report_.ico"))

        self.setGeometry(100, 100, 800, 600)
        # Create a vertical layout for the central widget
        layout = QVBoxLayout()
        central_widget.setLayout(layout)

        # Create a horizontal layout for the first row
        row_1_layout = QHBoxLayout()
        layout.addLayout(row_1_layout)

        # Add a list widget to the first row
        self.combo = QComboBox()
        self.tester_list()
        self.combo.currentIndexChanged.connect(self.on_item_clicked)
        row_1_layout.addWidget(self.combo)
        self.cats_list = QListWidget()
        self.cats_list.setVisible(False)

        self.cats_list.setSelectionMode(QListWidget.MultiSelection)
        self.cats_list.clicked.connect(self.on_cat_clicked)


        # Add a from-date calendar button to the first row
        self.from_date_button = QPushButton('From', self)
        self.from_date_title = QLabel('Date: ')
        row_1_layout.addWidget(self.from_date_title)
        row_1_layout.addWidget(self.from_date_button)

        icon = QIcon('schedule.png')
        self.from_date_button.setIcon(icon)
        self.from_date_button.clicked.connect(self.show_from_calendar)

        # Add a to-date calendar button to the first row
        self.to_date_button = QPushButton('To', self)
        self.to_date_title = QLabel('Date: ')
        row_1_layout.addWidget(self.to_date_title)
        row_1_layout.addWidget(self.to_date_button)

        self.to_date_button.setIcon(icon)
        self.to_date_button.clicked.connect(self.show_to_calendar)

        row_2_layout = QHBoxLayout()
        layout.addLayout(row_2_layout)
        self.table = QTableView()
        self.table.setSortingEnabled(True)
        self.table.doubleClicked.connect(self.on_double_click)
        self.model = QStandardItemModel()

        # Add a search button to the second row
        self.save_button = QPushButton('Save Results', self)
        self.save_button.clicked.connect(self.save_to_file)
        self.save_button.setVisible(False)
        search_button = QPushButton('Search', self)
        search_button.clicked.connect(self.search_function)
        row_2_layout.addWidget(search_button)
        row_2_layout.addWidget(self.save_button)

        splitter = QSplitter()
        splitter.addWidget(self.table)
        splitter.addWidget(self.cats_list)
        splitter.setSizes([int(self.width() * 0.8), int(self.width() * 0.2)])

        row_3_layout = QHBoxLayout()
        row_3_layout.addWidget(splitter)
        layout.addWidget(splitter)

        self.from_calendar_dialog = None
        self.to_calendar_dialog = None

    def show_from_calendar(self):
        if self.from_calendar_dialog is None:
            self.from_calendar_dialog = FromCalendarDialog(self)
        self.from_calendar_dialog.date_selected.connect(self.handle_from_date_selection)
        self.from_calendar_dialog.show()

    def show_to_calendar(self):
        if self.to_calendar_dialog is None:
            self.to_calendar_dialog = ToCalendarDialog(self)
        self.to_calendar_dialog.date_selected.connect(self.handle_to_date_selection)
        self.to_calendar_dialog.show()

    def handle_from_date_selection(self, date):
        self.from_date_selected = date.toString('dd/MM/yyyy')
        date_obj = datetime.strptime(self.from_date_selected, "%d/%m/%Y")
        self.from_date_selected = date_obj.strftime("%d/%m/%Y")
        self.from_date_title.setText(self.from_date_selected)
        print(f'From date selected:{self.from_date_selected}')

    def handle_to_date_selection(self, date):
        self.to_date_selected = date.toString('dd/MM/yyyy')
        date_obj = datetime.strptime(self.to_date_selected, "%d/%m/%Y")
        self.to_date_title.setText(self.to_date_selected)
        self.to_date_selected = date_obj.strftime("%d/%m/%Y")
        print('To date selected:', self.to_date_selected)

    def tester_list(self):
        self.testers_list = database_instance.get_Testers()
        for item in self.testers_list:
            tester = item['Name']
            self.combo.addItem(tester)

    def cats_list_by_tester(self):
        self.cats = database_instance.get_cats_No(self.selected_tester['ID'], self.from_date_selected, self.to_date_selected)
        print(self.cats)
        try:
            self.cats_list.setVisible(True)
            self.cats_list.clear()
            for cat in self.cats:
                if cat != " ":
                    self.cats_list.addItem(cat)
        except Exception as e:
            print(f"There are no cats at this Tester: {e}")

    def on_item_clicked(self):
        self.cats_list.clear()
        self.cats_list.setVisible(False)
        self.selected_cats = None
        self.selected_tester = {}
        self.selected_tester['Name'] = self.combo.currentText()
        for item in self.testers_list:
            if item['Name'] == self.selected_tester['Name']:
                self.selected_tester['ID'] = item['ID']

    def create_table(self, cat=""):
        if cat:
            cat_query = self.get_cats_query(self.selected_cats)
        else:
            cat_query = ""
            # cat_query = self.get_first_cat_query(self.cats[0])
        self.data, headers = database_instance.get_data_search(self.selected_tester['ID'], self.from_date_selected, self.to_date_selected, cat_query)
        if not self.data.empty:
            headers = list(map(str, headers))
            self.model.setHorizontalHeaderLabels(headers)
            self.model.setColumnCount(len(headers))
            self.model.setRowCount(len(self.data))
            self.start_task(self.data, headers)
            self.table.setModel(self.model)
            self.table.setSelectionBehavior(QTableView.SelectRows)
            self.table.setAlternatingRowColors(True)

    def get_first_cat_query(self, cat):
        cat_query = f"AND Cat_No='{cat}'"
        return cat_query

    def get_cats_query(self, cats):
        if cats:
            cat_query = f"AND "
            if len(cats) > 1:
                cat_query = f"AND ("
                for cat in cats[:-1]:
                    cat_query = cat_query + f"Cat_No='{cat}' OR "
                cat_query = cat_query + f"Cat_No='{cats[-1]}')"
            elif len(cats) == 1:
                cat_query = cat_query + f"Cat_No='{cats[0]}'"
        else:
            cat_query = ""
        return cat_query

    def on_cat_clicked(self):
        self.selected_cats = [item.text() for item in self.cats_list.selectedItems()]
        self.create_table(self.selected_cats)

    def search_function(self):
        # self.create_table()
        self.cats_list_by_tester()
        self.create_table()

    # Thread  Function
    def start_task(self, data, headers):
        self.thread = WorkerThread(self.model, data, headers)
        self.thread.finished.connect(self.update_progress)
        self.thread.start()
        self.setWindowTitle('Loading...')

    # Thread  Function
    def update_progress(self, results):
        # self.thread.join()
        self.setWindowTitle(results)
        self.save_button.setVisible(True)

    def closeEvent(self, event):
        # Action when the user closes the window
        database_instance.close_connection()
        print("Window closed")
        event.accept()

    def on_double_click(self, index: QModelIndex):
        # Retrieve the serial number from the clicked cell
        serial_number = self.model.item(index.row(), 4).text()
        print(f"Double-clicked on serial number: {serial_number}")
        file_path = r"D:\HULK\builds\Serial History\Serial_History_main.exe"
        if not os.path.exists(file_path):
            print(f"File does not exist: {file_path}")
        else:
            try:
                subprocess.Popen([r"D:\HULK\builds\Serial History\Serial_History_main.exe", serial_number])
            except Exception as e:
                print(f"Failed to open external application: {e}")

    def save_to_file(self):
        df = pd.DataFrame(self.data)
        filepath = "Results.xlsx"
        try:
            with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False)
            print(f"File saved successfully at {filepath}")
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Information)
            msg.setWindowTitle("Message")
            msg.setText("File Created Successfully!")
            msg.setStandardButtons(QMessageBox.Ok)
            msg.exec_()

        except Exception as e:
            print(f"Error writing to file {filepath}: {e}")
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Critical)
            msg.setWindowTitle("ERROR")
            msg.setText(f"Error Creating File {e}")
            msg.setStandardButtons(QMessageBox.Ok)
            msg.exec_()


if __name__ == '__main__':
    app = QApplication([])
    app.setStyle('Fusion')
    window = MainWindow()
    window.show()
    app.exec_()
