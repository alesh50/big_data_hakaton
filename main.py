import datetime
import os
from os import path
import sqlite3
import xlrd
import time
import datetime as dt
import traceback
import sys
import pandas as pd


def open_data_set():
    data_set = xlrd.open_workbook(r'data_set/02-2020.xls', formatting_info=True)
    data_set_sheet = data_set.sheet_by_index(0)
    data_set_n_row_values = data_set_sheet.row_values(3)
    print(data_set_n_row_values)
    date_data_set = data_set_n_row_values[0]
    data_set_year_old = data_set_n_row_values[16]


def data_set_date_base(file_name):
    date_base = []
    years_old = []
    time_call = []
    time_arrival = []
    id_call = []
    addres = []
    delivered = []
    substation = []
    type_call = []
    type_of_disease = []
    data_set = xlrd.open_workbook_xls(fr'data_set/{file_name}', formatting_info=True)
    data_set_sheet = data_set.sheet_by_index(0)
    max_rows = data_set_sheet.nrows
    # df = pd.read_excel('data_set/02-2020.xls')
    # print(data_set_sheet.row_values(7))
    for i in range(max_rows + 1):
        # check_addres = df.iloc[[i]]
        # print(check_addres)
        # if check_addres == 'Адрес:':
        #     addres.append(df.iloc[i, 1])
        data_set_row_values = data_set_sheet.row_values(i - 1)
        if (data_set_row_values[14] == 'Возраст:') and (type(data_set_row_values[0]) == float) and (data_set_sheet.row_values(i + 4)[0] == 'Принят:'):
            date_base.append(data_set_row_values[0])
            x_year = data_set_row_values[16].strip().split(' ')
            try:
                if x_year[1].lower().startswith('лет'):
                    years_old.append(x_year[0])
                elif x_year[1].lower().startswith('мес'):
                    years_old.append(int(x_year[0]) / 12)
                elif x_year[1].lower().startswith('нед'):
                    years_old.append(int(x_year[0]) / 52)
                elif x_year[1].lower().startswith('дн'):
                    years_old.append(int(x_year[0]) / 365)
                else:
                    print(f"ошибка на строке {i}")
            except:
                print(f"ощибка в строке {i}")
                years_old.append(x_year[0])
            id_call.append(data_set_row_values[3])
        elif (data_set_row_values[0] == 'Принят:') and (type(data_set_sheet.row_values(i - 6)[0]) == float) and (data_set_sheet.row_values(i - 6)[14] == 'Возраст:'):
            time_call.append(data_set_row_values[8])
            time_arrival.append(data_set_row_values[11])
        elif (data_set_row_values[0] == 'Адрес:') and (type(data_set_sheet.row_values(i - 2)[0]) == float) and (data_set_sheet.row_values(i - 2)[14] == 'Возраст:'):
            addres.append(data_set_row_values[1])
        elif (data_set_row_values[9] == 'Вызов:') and (type(data_set_sheet.row_values(i - 3)[0]) == float) and (data_set_sheet.row_values(i - 3)[14] == 'Возраст:'):
            type_call.append(data_set_row_values[11])
            type_of_disease.append(data_set_row_values[18])
        elif (data_set_row_values[0] == 'Доставлен:') and (type(data_set_sheet.row_values(i - 5)[0]) == float) and (data_set_sheet.row_values(i - 5)[14] == 'Возраст:'):
            delivered.append(data_set_row_values[1])
            substation.append(data_set_row_values[17])
    date_datetime = []
    for item in date_base:
        date_datetime.append((xlrd.xldate.xldate_as_datetime(int(item), data_set.datemode)).date())
    if id(time_call[0]) == id(time_call[len(time_call)-1]):
        time_call.pop(0)
        time_arrival.pop(0)
    print(len(date_base))
    print(len(years_old))
    print(len(id_call))
    print(len(time_call))
    print(len(time_arrival))
    print(len(addres))
    print(len(type_call))
    print(len(type_of_disease))
    print(len(delivered))
    print(len(substation))

    print(date_base)
    print(years_old)
    print(id_call)
    print(time_call)
    print(time_arrival)
    print(addres)
    print(type_call)
    print(type_of_disease)
    print(delivered)
    print(substation)
    return date_base, years_old, id_call, time_call, time_arrival, addres, type_call, type_of_disease, delivered, substation


def sqlite_data_base():
    sqlite_connection = sqlite3.connect('sqlite_python.db')
    cursor = sqlite_connection.cursor()
    print("База данных создана и успешно подключена к SQLite")
    sqlite_select_query = "select sqlite_version();"
    cursor.execute(sqlite_select_query)
    record = cursor.fetchall()
    print("Версия базы данных SQLite: ", record)
    cursor.close()
    if (sqlite_connection):
        sqlite_connection.close()
        print("Соединение с SQLite закрыто")


def data_base_sqlite3_create_table():
    try:
        sqlite_connection = sqlite3.connect('sqlite_python.db')
        sqlite_create_table_query = '''CREATE TABLE data_set_base1 (
                                    id INTEGER PRIMARY KEY,
                                    id_call VARCHAR(50),
                                    date_xls INTEGER,
                                    date TEXT,
                                    year_old VARCHAR(20),
                                    time_call VARCHAR(50),
                                    time_arrival VARCHAR(50),
                                    addres VARCHAR(250),
                                    type_call VARCHAR(100), 
                                    type_of_disease VARCHAR(100), 
                                    delivered VARCHAR(250), 
                                    substation VARCHAR(150)
                                    );'''

        cursor = sqlite_connection.cursor()
        print("База данных подключена к SQLite")
        cursor.execute(sqlite_create_table_query)
        sqlite_connection.commit()
        print("Таблица SQLite создана")

        cursor.close()

    except sqlite3.Error as error:
        print("Ошибка при подключении к sqlite", error)
    finally:
        if (sqlite_connection):
            sqlite_connection.close()
            print("Соединение с SQLite закрыто")


def func_insert_sql(vals):
    try:
        sqlite_connection = sqlite3.connect('sqlite_python.db')
        cursor = sqlite_connection.cursor()
        print("База данных подключена к SQLite")
        vals_f = str(vals)
        sqlite_insert_query = f"INSERT INTO data_set_base1 (date_xls, year_old, id_call, time_call, time_arrival, addres, type_call, type_of_disease, delivered, substation)  VALUES {vals_f[1:len(vals_f) - 1]}"
        print(vals_f[1:len(vals_f) - 1])

        count = cursor.execute(sqlite_insert_query)
        sqlite_connection.commit()
        print("Запись успешно вставлена таблицу ", cursor.rowcount)
        cursor.close()

    except sqlite3.Error as error:
        print("Не удалось вставить данные в таблицу sqlite")
        print("Класс исключения: ", error.__class__)
        print("Исключение", error.args)
        print("Печать подробноcтей исключения SQLite: ")
        exc_type, exc_value, exc_tb = sys.exc_info()
        print(traceback.format_exception(exc_type, exc_value, exc_tb))
    finally:
        if (sqlite_connection):
            sqlite_connection.close()
            print("Соединение с SQLite закрыто")


def tuple_generator(file_name):
    date_base_xls, years_old_base, id_call_base, time_call_base, time_arrival_base, addres_base, type_call_base, type_of_disease_base, delivered_base, substation_base = data_set_date_base(file_name)
    base_set = []
    for i in range(len(date_base_xls)):
        tup = (date_base_xls[i], years_old_base[i], id_call_base[i], time_call_base[i], time_arrival_base[i], addres_base[i], type_call_base[i], type_of_disease_base[i], delivered_base[i], substation_base[i])
        base_set.append(tup)
    return base_set


def file_iteration():
    folder_name = 'data_set/'
    file_list_dir = os.listdir(folder_name)
    # print(len(file_list_dir))
    # print(file_list_dir)
    return file_list_dir



if __name__ == '__main__':
    data_base_sqlite3_create_table()
    # data_set_date_base()
    # base_values = tuple_generator()
    # print(base_values)
    # func_insert_sql(base_values)
    files_names = file_iteration()
    for name in files_names:
        base_values = tuple_generator(name)
        func_insert_sql(base_values)

