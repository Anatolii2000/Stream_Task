import psycopg2
from datetime import datetime, timedelta
from calendar import monthrange
import csv


weekdays = [0, 1, 2, 3, 4]
str_huder = 'ID,DATA,T1,T2,T3,Task,FIO,Section,Departament\n'

def get_day_early_month(day):
    month = day.month
    early_month = 0
    if month == 1:
        early_month = 12
    else:
        early_month = month - 1
    
    days = monthrange(day.year, early_month)


    early_day = datetime(day.year, early_month, days[1])
    return early_day



def get_date_yesterday():
    #date_now = datetime(2022, 5, 1)
    date_now = datetime.now()
    if date_now.day == 1:
        date_now = get_day_early_month(day=date_now)

    result_date = date_now - timedelta(days=1)
    if result_date.weekday() not in weekdays:
        while True:
            result_date = result_date - timedelta(days=1)
            if result_date.weekday() in weekdays:
                break

    print(result_date)
    result_date = result_date.strftime("%Y-%m-%d")
    return result_date


def get_result_from_db():

    date_yesterday = get_date_yesterday()

    query = f"SELECT Worklog.id as worklog_id, Worklog.start_date, Worklog.comment, Worklog.time_spent_seconds, Task.title as task_title, Task.key as task_key, Worker.fullname, Project.title as project_title, Project.key as project_key FROM Worklog INNER JOIN Task ON Task.id = Worklog.task_id INNER JOIN Worker ON Worker.id = Worklog.worker_id INNER JOIN Project ON Project.id = Task.project_id WHERE Worklog.start_date = '{date_yesterday}';"

    host = '172.23.193.2'
    user = 'postgres'
    password = 'postgres'
    database = 'monitor'

    try:
        connection = psycopg2.connect(
                host=host,
                user=user,
                password=password,
                database=database
        )
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(
                    query
                )
            
            return cursor.fetchall()
    except Exception as e:
        print('[INFO] Error while working with PostgreSQL', e)

if __name__ == '__main__':
    data = get_result_from_db()
    print(data)
    fieldnames="ID DATA T1 T2 T3 Task FIO Section Departament".split()
    with open('jira.csv', 'w') as csvfile:
        w = csv.DictWriter(csvfile, fieldnames=fieldnames)
        w.writeheader()
        for l in data:
            w.writerow({fn: d for fn, d in zip(fieldnames, l)})


    #print(data)