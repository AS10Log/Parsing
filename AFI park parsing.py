#!/usr/bin/env python
# coding: utf-8

# # Парсинг сайта недвижимости

# Данные проект подготовлен как пример реализации парсинга данных с сайта, обработки этих данных и последущего экеспорта в google sheet и базу данных PostgreSQL.

# In[16]:


# импортируем необходимые библиотеки, при необходимости устанавливаем 
import requests
import pandas as pd
import gspread
import psycopg2
from fake_useragent import UserAgent
import random
import pprint
from datetime import datetime

pd.set_option('display.max_columns', None)


# ## Определяем классы и методы для работы 

# In[37]:


# Класс позволит работаь с google sheets
class GoogleSheets:
    
    """ КЛАСС ПОЗВОЛЯЮЩИЙ РАБОТАТЬ С GOOGLE SHEETS """

    def __init__(self, service_account, url):

        """ АУТЕНТИФИКАЦИЯ С ПОМОЩЬЮ СЕРВИСНОГО АККАУНТА GOOGLE  И ПОДКЛЮЧЕНИЕ К ТАБЛИЦЕ GOOGLE SHEET """
        
        gc = gspread.service_account(filename=service_account)
        self.sh = gc.open_by_url(url)
        
            
    def export(self, data, name):

        """ ЭКСПОРТ ДАННЫХ В ТАБЛИЦУ """
        
        sheet = self.sh.worksheet(name)
        sheet.clear()
        data.fillna(0, inplace=True)
        all_processed_data_columns = data.columns.to_list()
        all_processed_data_list = data.to_numpy().tolist()
        all_processed_data_all = [all_processed_data_columns] + all_processed_data_list
        sheet.update(all_processed_data_all)
        
        print(f"Данные записаны в лист {name} ", "_" * 100, sep='\n')

    def get(self, name):

        """ ПОЛУЧЕНИЕ ДАННЫХ ИЗ ГУГЛ ТАБЛИЦЫ """

        print(f"Получение данных из листа {name}")
        worksheet = self.sh.worksheet(name)
        data = worksheet.get_all_records()
        data = pd.DataFrame(data)
        print("_" * 100)
        return data


# In[25]:


# Класс позволит спарсить данные и преобразовать их к формату DataFrame
class Parsing:
    
    columns_name = {'b':'Номер корпуса', 's':'s', 'f':'Этаж', 'n':'n', 'rc':'Виду комнатности', 'sq':'Площадь', 'st':'st',
                'tc':'Цена до скидки', 'tcd':"Цена со скидкой", 'cpm':'cpm', 'cpmd':'cpmd', 'tn':'Номер квартиры',
                'views':'Вид', 't':'Тип помещения', 'fn':'Наличие отделки', 'fn_t':'Вид отделки', 'ds':'Скидка %', 'uid':'ID'}
    
    def get_data(self):
        
        """ ПОЛУЧЕНИЕ ДАННЫХ С САЙТА """
        
        url = "https://afi-v-park.ru/hydra/json/data.json?"
        ua = UserAgent().random
        headers = {"User-Agent":ua}
        response = requests.get(url = url, headers = headers)
        print(response.status_code)
        try:
            data = pd.DataFrame(response.json()['apartments'].values())
            data = self.data_to_pandas(data)
            
            return data
        
        except Exception as ex:
            print(f"[INFO]: {ex}")
       

    def data_to_pandas(self,data):
        
        """ ОБРАБОТКА ДАННЫЕ И ПРЕОБРАЗОВАНИЕ К DATAFRAME """
        
        data = data.rename(columns = self.columns_name)
        date = str(datetime.now())[:10]
        data['Дата сбора'] = date 
        data['Виду комнатности'] = data['Виду комнатности'].apply(lambda x: 'Студия' if x == 0 else f'{x}-комнатная квартира')
        data["Вид отделки"] = data["Вид отделки"].apply(lambda x: 'Без отделки' if x == '' else x)
        data = data[['Дата сбора','Номер корпуса','Номер квартиры','Виду комнатности',
                  'Этаж',"Вид отделки",'Площадь','Цена до скидки',"Цена со скидкой"]]
        return data


# In[53]:


# Класс для работы с базой данных
class Query:

    def __init__(self,config):
        
        """ ИНИЦИАЛИЗАЦИЯ ДАННЫХ ДЛЯ ПОДКЛЮЧЕННИЯ К БД POSTGRE SQL """
        
        self.host = config["host"]
        self.user = config["host"]
        self.password = config["password"]
        self.db_name = config["db_name"]
        self.port = config["port"]

    def export_data(self, query, data):
        
        """ ЭКСПОРТИРТ ПЕРЕДАННЫХ ДАННЫЕ В СООТВЕТСВИИ С ЗАПРОСОМ В БД """
        
        connection = None
        data = data.to_numpy().tolist()
        try:
            connection = psycopg2.connect(host=self.host,
                                          user=self.user,
                                          password=self.password,
                                          database=self.db_name)
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.executemany(query, data)
                print(cursor.fetchone())
        except Exception as _ex:
            if _ex == 'no results to fetch':
                print("No errors")
            else:
                print(f"[INFO]: Error while working with PostgreSQL - {_ex}")
        finally:
            if connection:
                connection.close()
                print("[INFO]: PostgreSQL connection close")
                

    def query(self, query):
        
        """ СОЗДНИЕ ПЕРЕДАННОГО ЗАПРОСА К БД, ПРИ НАЛИЧИИ ВОЗВРАЩАЕТ ДАННЫЕ """
        
        connection = None
        try:
            connection = psycopg2.connect(host=self.host,
                                          user=self.user,
                                          password=self.password,
                                          database=self.db_name)
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()
                
            
        except Exception as _ex:
            if _ex == 'no results to fetch':
                print("No errors")
            else:
                print(f"[INFO]: Error while working with PostgreSQL - {_ex}")
        finally:
            if connection:
                connection.close()
                print("[INFO]: PostgreSQL connection close")


# ## Выполнение кода 

# In[56]:


# Указывваем данные конфигурации для работы с БД
config = { "host":"127.0.0.1",
           "host":"postgres",
           "password":'passwodr',
           "db_name" : 'database',
           "port" : 5432}

# Указываем ссылку на файл с даннымы сервисного аккаунта Google
SA_path = "/Users/service_account_google.json"
db = Query(config)


# In[28]:


# Получаем данные
data = Parsing().get_data()
data


# In[39]:


# Инициализирум таблицу и выгружаем туда полученные данные
url = "https://docs.google.com/spreadsheets/d/1AFlbZP1xArZhTC1y1-miuD5f5BfhzoU_iOlT69KEl8k/edit?usp=sharing"
table = GoogleSheets(service_account = SA_path ,url = url)
table.export(data = data, name = 'data')


# In[79]:


# Создадим таблицу в postgesql  
q = """ 
    CREATE TABLE IF NOT EXISTS apartments
        (date_get_data DATE,
         korpus VARCHAR,
         apartment_numbers VARCHAR,
         aparments_type VARCHAR,
         floor INT,
         apartment_finishing_type VARCHAR,
         square FLOAT,
         price_befor_discount REAL,
         final_price REAL); """


db.query(q)
         

# Экспортируем данные в БД
q = """ INSERT INTO apartments VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
db.export_data(q, data)             


# In[80]:


# Сделаем запрос к таблице apartments 
q = "SELECT * FROM apartments "
for i in db.query(q):
    print(*i)

