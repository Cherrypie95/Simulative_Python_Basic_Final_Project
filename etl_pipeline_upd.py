import requests
import psycopg2
import logging
from datetime import datetime, timedelta
from collections import defaultdict
import json
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import smtplib
import ssl
from email.message import EmailMessage
from credentials import client, client_key, user_db, password_db, host_db, port_db, sender_gmail, smtp_server_gmail, smtp_port_gmail, password_gmail, google_sheet_name, google_worksheet_name


# Параметры для подключения к API
api_url = "https://b2b.itresume.ru/api/statistics"
params = {
        "client":client,
        "client_key":client_key,
        "start": datetime.strftime(datetime.now()- timedelta(days=1), "%Y-%m-%d") + ' 00:00:00.000000',
        "end": datetime.strftime(datetime.now(), "%Y-%m-%d") + ' 00:00:00.000000'
        }

# Шаблон для генерации названия файла логов
logs_file_name = f'{datetime.now().date()} - logs.txt'
logs_file_dir = 'logs'
file_path = os.path.join(logs_file_dir, logs_file_name)
os.makedirs(logs_file_dir, exist_ok=True)

# Шаблон для генерации логов в консоли
logger = logging.getLogger()
handler = logging.FileHandler(file_path, mode='w')
formatter = logging.Formatter('%(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# logging.basicConfig(
#     format='%(name)s %(levelname)s: %(message)s',
#     level=logging.INFO)


# Класс для подкючения к базе данных PostgreSQL
class PGConnection:
    @staticmethod
    def get_pg_connection():
        conn = psycopg2.connect(
                        dbname = user_db,
                        user = user_db,
                        password = password_db,
                        host = host_db,
                        port = port_db
                        )
        return conn
    

# Класс для подключения к API и выгрузки данных
class ApiResponseReader:
    
    # Создаем экземпляр класса с атрибутами url и params
    def __init__(self, url, params):
        self.url = url
        self.params = params
    
    def get_response(self):
        """Функция для выгрузки данных по API

        Args:
            url (string): api url для выгрузки статистики по студентам
            params (dict): параметры для доступа к API

        Returns:
            list: Функция возвращает ответ сервера в виде списка словарей
        """
        try:
            logger.info(f'Процесс: Получение данных по API\nДата/время запуска процесса: {datetime.now()}\nСтатус: Старт загрузки данных по API')
            logger.info('=================================')
            response = requests.get(self.url, self.params)
            response.raise_for_status()
            logger.info(f'Процесс: Получение данных по API\nДата/время запуска процесса: {datetime.now()}\nСтатус: Данные успешно получены')
            logger.info('=================================')
            response_json = response.json()
            if not response_json:
                logger.info(f'Процесс: Получение данных по API\nДата/время запуска процесса: {datetime.now()}\nСтатус: API вернул пустой список')
                logger.info('=================================')
            if not isinstance(response_json, list):
                logger.info(f'Процесс: Получение данных по API\nДата/время загрузки: {datetime.now()}\nСтатус: Ошибка\nОписание ошибки: API вернул невалидный тип данных')
                logger.info('=================================')
                raise ValueError('Ожидался список, получен ' + type(response_json))
            logger.info(f'Процесс: Получение данных по API\nДата/время запуска процесса: {datetime.now()}\nСтатус: Данные успешно преобразованы в JSON\nПолучено {len(response_json)} записей')
            logger.info('=================================')            
            return response_json
        except requests.exceptions.HTTPError as http_err:            
            logger.info(f'Процесс: Получение данных по API\nДата/время загрузки: {datetime.now()}\nСтатус: Ошибка\nКод ошибки: {response.status_code}\nОписание ошибки: {http_err}')
            logger.info('=================================')                    
        except Exception as err:              
            logger.info(f'Процесс: Получение данных по API\nДата/время загрузки: {datetime.now()}\nСтатус: Ошибка\nОписание ошибки: {err}')
            logger.info('=================================') 


# Класс для обработки данных от API
class ApiResponseProcessing:

    # Создаем экземпляр класса с атрибутами data и tasks_stat
    # В data будем записывать обработанные данные от API
    # В task_stat будет записывать статистику решения задач 
    def __init__(self):
        self.data = []
        self.tasks_stat = []    
    
    def pars_response(self, response_json):
        """Функция для обработки ответа сервера и сохранения данных

        Returns:
            List: Функция возвращает список словарей с необходимыми параметрами
        """

        # Создаем словари для сохранения отчета по количеству успешных/неуспешных попыток решения задач
        success_tasks = defaultdict(int)
        fail_taks = defaultdict(int)

        logger.info(f'Процесс: Обработка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Старт обработки данных ответа API')
        logger.info('=================================')  
        for i, r in enumerate(response_json):
            params_str = r.get('passback_params', '')
            if not params_str or not params_str.strip():
                logger.info(f'Процесс: Обработка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Ошибка\nПропуск записи: passback_params пуст')
                logger.info('=================================')
                continue
            try:
                user_id = r.get('lti_user_id', None)
                if user_id:                    
                    passback_params_pars_json = json.loads(params_str.replace("'",'"'))
                    oauth_consumer_key = None if passback_params_pars_json.get('oauth_consumer_key', None) == '' else passback_params_pars_json.get('oauth_consumer_key', None)
                    lis_result_sourcedid = passback_params_pars_json.get('lis_result_sourcedid', None)
                    lis_outcome_service_url = passback_params_pars_json.get('lis_outcome_service_url', None)
                    is_correct = r.get('is_correct', None)
                    attempt_type = r.get('attempt_type', None)
                    created_at = r.get('created_at', None)

                    # Собираем статистику по решению задач                        
                    if is_correct == 1:
                        success_tasks['created_at'] = created_at.split(" ")[0]
                        success_tasks['is_correct'] = '1'
                        success_tasks['cnt_task'] += 1
                    if is_correct == 0:
                        fail_taks['created_at'] = created_at.split(" ")[0]
                        fail_taks['is_correct'] = '0'
                        fail_taks['cnt_task'] += 1                        
                    self.data.append((user_id, oauth_consumer_key, lis_result_sourcedid, lis_outcome_service_url, is_correct, attempt_type, created_at))
            except json.JSONDecodeError as err:
                logger.info(f'Процесс: Обработка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Ошибка парсинга JSON\nИнформация по ошбике: {err}')
                logger.info('=================================')
                continue
            except Exception as err:
                logger.info(f'Процесс: Обработка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Ошибка\nИнформация по ошбике: {err}')
                logger.info('=================================')
        logger.info(f'Процесс: Обработка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Данные успешно обработаны')
        logger.info('=================================')
        # Сохраняем статистику решения задач
        self.tasks_stat.append(list(success_tasks.values()))
        self.tasks_stat.append(list(fail_taks.values()))
        return self.data


# Класс для загрузки данных в базу данных PostgreSQL
class ImportData:

    @staticmethod
    def import_data(students_data):
        """Функция для загрузки данных в БД

        Args:
            students_data (List): Список словарей с необходимыми параметрами
        """
        try:
            logger.info(f'Процесс: Загрузка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Попытка подключения к базе данных')
            logger.info('=================================')
            conn = PGConnection.get_pg_connection()
            logger.info(f'Процесс: Загрузка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Подключение к базе данных успешно установлено')
            logger.info('=================================')
        except Exception as err:
            logger.info(f'Процесс: Загрузка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Ошибка\nКод ошибки: {err.pgcode}\nИнформация по ошибке {err}')
            logger.info('=================================')
        else:
            try:
                logger.info(f'Процесс: Загрузка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Попытка загрузки данных в базу данных')
                logger.info('=================================')
                cur = conn.cursor()
                cur.execute("""create table if not exists students_activity (
                    user_id varchar,
                    oauth_consumer_key text,
                    lis_result_sourcedid text,
                    lis_outcome_service_url text,
                    is_correct varchar,
                    attempt_type varchar,
                    created_at timestamp,
                    constraint unique_rows unique (user_id, created_at)
                )
                """)
                cur.executemany(
                """INSERT INTO students_activity (user_id, oauth_consumer_key, lis_result_sourcedid, lis_outcome_service_url, is_correct, attempt_type, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, created_at) DO NOTHING""",
                students_data)
                conn.commit()
                logger.info(f'Процесс: Загрузка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Данные успешно загружены в базу данных')
                logger.info('=================================')
            except psycopg2.Error as err:
                pgcode = err.pgcode if hasattr(err, 'pgcode') else 'Unknown'
                logger.info(f'Процесс: Загрузка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Ошибка\nКод ошибки: {pgcode}\nИнформация по ошибке {err}')
                logger.info('=================================')
            except Exception as err:
                logger.info(f'Процесс: Загрузка данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Ошибка\nИнформация по ошибке {err}')
            finally:
                cur.close()
                conn.close()


# Класс для удаления старых файлов логов
class LogFilesCleaner:
    
    @staticmethod
    def remove_old_log_files(n=3):
        """Функция для очистки старых файлов логов и сохранения файлов логов за последние 3 дня

        Args:
            n (int, optional): Количество дней за которые нужно сохранить файлы логов. По умолчанию 3.
        """
        
        logs_file_list = os.listdir(logs_file_dir)
        
        # Отбираем файлы, которые нужно удалить
        logs_file_for_remove = sorted([datetime.strptime(x.replace(' - logs.txt',''), '%Y-%m-%d') for x in logs_file_list])[:-n]

        for f in logs_file_for_remove:
            if os.path.exists(logs_file_dir):
                try:
                    logger.info(f'Процесс: Удаление старых данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Попытка удаления файлов логов')
                    logger.info('=================================')
                    os.remove(os.path.join(logs_file_dir, f'{datetime.strftime(f, "%Y-%m-%d")} - logs.txt'))
                    logger.info(f'Процесс: Удаление старых данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Старые данные успешно удалены')
                    logger.info('=================================')
                except Exception as err:
                    logger.info(f'Процесс: Удаление старых данных\nДата/время запуска процесса: {datetime.now()}\nСтатус: Ошибка\nИнформация по ошибке: {err}')
                    logger.info('=================================')


# Класс для загрузки отчета в Google Sheets
class GoogleSheetImportReport:

    @staticmethod
    def import_data_google_sheet(task_stat):
        scope = ['https://www.googleapis.com/auth/spreadsheets.readonly', 'https://www.googleapis.com/auth/drive']

        try:
            logger.info(f'Процесс: Загрузка данных в Google Sheets\nДата/время запуска процесса: {datetime.now()}\nСтатус: Попытка загрузки данных в Google Sheets')
            logger.info('=================================')

            # Загружаем ключи аутентификации из файла json
            creds_file = 'creds.json'
            if not os.path.exists(creds_file):
                logger.info(f'Процесс: Загрузка данных в Google Sheets\nДата/время запуска процесса: {datetime.now()}\nСтатус: Ошибка\nФайл {creds_file} не найден')
                logger.info('=================================')
                raise FileNotFoundError(f'Файл {creds_file} должен быть в корне проекта')
            creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)

            # Авторизуемся в Google Sheets API
            client_gs = gspread.authorize(creds)

            sheet = client_gs.open(google_sheet_name)
            sheet1 = sheet.worksheet(google_worksheet_name)
        
            res_list = [[r[0], r[1], r[2]] for r in task_stat]

            headers = ['activity_date','is_correct','cnt_tasks']

            if sheet1.get_values()[0] != headers:
                sheet1.insert_row(headers, index = 1)
                sheet1.append_rows(res_list)
            else:
                sheet1.append_rows(res_list)
            
            logger.info(f'Процесс: Загрузка данных в Google Sheets\nДата/время запуска процесса: {datetime.now()}\nСтатус: Данные успешно загружены в Google Sheets')
            logger.info('=================================')
        except Exception as err:
            logger.info(f'Процесс: Загрузка данных в Google Sheets\nДата/время запуска процесса: {datetime.now()}\nСтатус: Ошибка\nИнформация по ошибке: {err}')
            logger.info('=================================')
        finally:
            sheet.client.session.close()


# Класс для отправки отчета на электронную почту
class SendEmailReport:
    
    @staticmethod
    def send_email(task_stat):

        # Находим количество успешных попыток решения задач за предыдущий день. 
        # Если данных за T-1 нет, то для расчета берем последнюю дату предшествующую дате текущей загрузки
        conn = PGConnection.get_pg_connection()
        cur = conn.cursor()

        cur.execute("""
                select 
                    created_at::date as activity_date, 
                    is_correct,
                    count(*) as cnt_tasks
                from students_activity sa
                where is_correct is not null and created_at::date = (select distinct created_at::date from students_activity order by created_at::date desc limit 1 offset 1)
                group by 1,2
                order by 1,2
            """)

        yesterday_data = cur.fetchall()

        if task_stat:
            if not yesterday_data or yesterday_data[1][2] == 0:
                delta_success_tasks = 0
            else:
                delta_success_tasks = round(abs((task_stat[0][2]-yesterday_data[1][2])*1.0 / task_stat[0][2]*100), 2)
                
            if not yesterday_data or yesterday_data[0][2] == 0:
                delta_fail_tasks = 0
            else:
                delta_fail_tasks = round(abs((task_stat[1][2]-yesterday_data[0][2])*1.0 / task_stat[1][2]*100), 2)
        else:
            logger.info(f'Процесс: Отправка отчета на Email\nДата/время запуска процесса: {datetime.now()}\nСтатус: Ошибка\nИнформация по ошибке: Не получил данные из Google таблицы')
            logger.info('=================================')

        # Собираем отчет и отправляем письмо
        context = ssl.create_default_context()

        msg = EmailMessage()

        subject = f"Статистика решения задач за {datetime.strftime(datetime.now()- timedelta(days=1), "%Y-%m-%d")}"
        message = f"""Здравствуйте!
                      Успешных попыток решения задач: {task_stat[0][2]}\n
                      Это на {delta_success_tasks}% {'больше' if delta_success_tasks > 0 else 'меньше'} чем за предыдущий день                      
                      Неуспешных попыток решения задач: {task_stat[1][2]}
                      Это на {delta_fail_tasks}% {'больше' if delta_fail_tasks > 0 else 'меньше'} чем за предыдущий день
                    """
        
        msg.set_content(message)

        msg.add_alternative(f"""
            <html>
            <body>
                <h3>Здравствуйте!</h3>
                <p>Успешных попыток решения задач: <b>{task_stat[0][2]}</b></p>
                <p>Это на <b>{delta_success_tasks}% {'больше' if delta_success_tasks > 0 else 'меньше'}</b> чем за предыдущий день</p>
                <p>Неуспешных попыток решения задач: <b>{task_stat[1][2]}</b></p>
                <p>Это на <b>{delta_fail_tasks}% {'больше' if delta_fail_tasks > 0 else 'меньше'}</b> чем за предыдущий день<p>
            </body>
            </html>
            """, subtype="html")


        msg['Subject'] = subject
        msg['From'] = sender_gmail
        msg['To'] = sender_gmail
        
        try:
            logger.info(f'Процесс: Отправка отчета на Email\nДата/время запуска процесса: {datetime.now()}\nСтатус: Попытка отправки отчета на Email')
            logger.info('=================================')
            server = smtplib.SMTP_SSL(smtp_server_gmail, smtp_port_gmail, context=context)
            server.login(sender_gmail, password_gmail)
            server.send_message(msg=msg)
            logger.info(f'Процесс: Отправка отчета на Email\nДата/время запуска процесса: {datetime.now()}\nСтатус: Отчет успешно отправлен')
            logger.info('=================================')
        except Exception as err:
            logger.info(f'Процесс: Отправка отчета на Email\nДата/время запуска процесса: {datetime.now()}\nСтатус: Ошибка\nИнформация по ошибке: {err}')
            logger.info('=================================')
        finally:
            server.quit()


# Создаем экземляр класса ApiResponseReader и выгружаем данные
t = ApiResponseReader(api_url, params)
response = t.get_response()

# Создаем экземляр класса ApiResponseProcessing и приводим данные к нужной форме
r = ApiResponseProcessing()
r_pars = r.pars_response(response)

# Загружаем данные в базу данных
ImportData.import_data(r_pars)

# Очищаем старые файлы логов
LogFilesCleaner.remove_old_log_files()

# Загружаем отчет в Google Sheets
tasks_stat_for_report = r.tasks_stat
GoogleSheetImportReport.import_data_google_sheet(tasks_stat_for_report)

# Отправляем отчет на Email
SendEmailReport.send_email(tasks_stat_for_report)
