import psycopg2
import csv
import time

input_csv_2019 = "./data/Odata2019File.csv"
input_csv_2020 = "./data/Odata2020File.csv"
output_csv = "./data/ZnoMathResults.csv"
time_record = "./data/TimeRecord.txt"

table_name = "zno_results"
table_log_name = "transactions"

main_table_query = (
    f"""
    CREATE TABLE IF NOT EXISTS {table_name}(
        OutID varchar,
        Birth int,
        SexTypeName varchar,
        RegName varchar,
        AreaName varchar,
        TerName varchar,
        RegTypeName varchar,
        TerTypeName varchar,
        ClassProfileName varchar,
        ClassLangName varchar,
        EOName varchar,
        EOTypeName varchar,
        EORegName varchar,
        EOAreaName varchar,
        EOTerName varchar,
        EOParent varchar,
        UkrTest varchar,
        UkrTestStatus varchar,
        UkrBall100 decimal(4, 1),
        UkrBall12 int,
        UkrBall int,
        UkrAdaptScale int,
        UkrPTName varchar,
        UkrPTRegName varchar,
        UkrPTAreaName varchar,
        UkrPTTerName varchar,
        histTest varchar,
        HistLang varchar,
        histTestStatus varchar,
        histBall100 decimal(4, 1),
        histBall12 int,
        histBall int,
        histPTName varchar,
        histPTRegName varchar,
        histPTAreaName varchar,
        histPTTerName varchar,
        mathTest varchar,
        mathLang varchar,
        mathTestStatus varchar,
        mathBall100 decimal(4, 1),
        mathBall12 int,
        mathBall int,
        mathPTName varchar,
        mathPTRegName varchar,
        mathPTAreaName varchar,
        mathPTTerName varchar,
        physTest varchar,
        physLang varchar,
        physTestStatus varchar,
        physBall100 decimal(4, 1),
        physBall12 int,
        physBall int,
        physPTName varchar,
        physPTRegName varchar,
        physPTAreaName varchar,
        physPTTerName varchar,
        chemTest varchar,
        chemLang varchar,
        chemTestStatus varchar,
        chemBall100 decimal(4, 1),
        chemBall12 int,
        chemBall int,
        chemPTName varchar,
        chemPTRegName varchar,
        chemPTAreaName varchar,
        chemPTTerName varchar,
        bioTest varchar,
        bioLang varchar,
        bioTestStatus varchar,
        bioBall100 decimal(4, 1),
        bioBall12 int,
        bioBall int,
        bioPTName varchar,
        bioPTRegName varchar,
        bioPTAreaName varchar,
        bioPTTerName varchar,
        geoTest varchar,
        geoLang varchar,
        geoTestStatus varchar,
        geoBall100 decimal(4, 1),
        geoBall12 int,
        geoBall int,
        geoPTName varchar,
        geoPTRegName varchar,
        geoPTAreaName varchar,
        geoPTTerName varchar,
        engTest varchar,
        engTestStatus varchar,
        engBall100 decimal(4, 1),
        engBall12 int,
        engDPALevel varchar,
        engBall int,
        engPTName varchar,
        engPTRegName varchar,
        engPTAreaName varchar,
        engPTTerName varchar,
        fraTest varchar,
        fraTestStatus varchar,
        fraBall100 decimal(4, 1),
        fraBall12 int,
        fraDPALevel varchar,
        fraBall int,
        fraPTName varchar,
        fraPTRegName varchar,
        fraPTAreaName varchar,
        fraPTTerName varchar,
        deuTest varchar,
        deuTestStatus varchar,
        deuBall100 decimal(4, 1),
        deuBall12 int,
        deuDPALevel varchar,
        deuBall int,
        deuPTName varchar,
        deuPTRegName varchar,
        deuPTAreaName varchar,
        deuPTTerName varchar,
        spaTest varchar,
        spaTestStatus varchar,
        spaBall100 decimal(4, 1),
        spaBall12 int,
        spaDPALevel varchar,
        spaBall int,
        spaPTName varchar,
        spaPTRegName varchar,
        spaPTAreaName varchar,
        spaPTTerName varchar,
        year int
    );"""
)

log_table_query = (
    f"""
    CREATE TABLE IF NOT EXISTS {table_log_name}(
        id int,
        row_count int,
        year int
    );"""
)

clear_query = f"DELETE FROM {table_name}"
clear_query2 = f"DELETE FROM {table_log_name}"


def last_transaction(conn):
    cursor = conn.cursor()
    cursor.execute(f'''
            select * from {table_log_name} order by id desc limit 1;
            ''')
    row = cursor.fetchone()
    cursor.close()
    return row


def formatter(line, year):
    new_line = []
    for val in line:
        if ',' in val:
            if len(val.split(",")) == 2:
                if val.split(",")[0].isdigit() and val.split(",")[1].isdigit():
                    new_line.append(val.replace(",", "."))
                    continue
        elif val == 'null':
            new_line.append(None)
            continue
        new_line.append(val)
    new_line.append(year)
    return tuple(new_line)


def writer_table(conn):
    try:
        if last_transaction(conn) is None:
            transaction_idx = 0
            row_count, start_count = 0, 0
        else:
            transaction_idx = last_transaction(conn)[0]
            row_count, start_count = last_transaction(conn)[1], last_transaction(conn)[1]
        cnt_row_2019 = sum(1 for line in open(input_csv_2019, 'r', encoding='cp1251'))-1
        cnt_row_2020 = sum(1 for line in open(input_csv_2020, 'r', encoding='cp1251'))-1
        cur = conn.cursor()
        with open(input_csv_2019, 'r', encoding="cp1251") as f:
            csv_reader = csv.reader(f, delimiter=';')
            insert_query = f"INSERT INTO {table_name} VALUES ({','.join(['%s'] * (len(next(csv_reader)) + 1))})"
            log_query = f"INSERT INTO {table_log_name} VALUES ({','.join(['%s'] * 3)})"
            for idx, row in enumerate(csv_reader):
                if idx < start_count:
                    pass
                elif start_count <= idx:
                    cur.execute(insert_query, formatter(row, 2019))
                    if (idx + 1) % 1000 == 0:
                        row_count += 1000
                        transaction_idx += 1
                        print(row_count)
                        cur.execute(log_query, (transaction_idx, row_count, 2019))
                        conn.commit()
                    elif (idx + 1) % 1000 != 0 and (idx + 1) == cnt_row_2019:
                        row_count += cnt_row_2019 - last_transaction(conn)[1]
                        transaction_idx += 1
                        print(row_count)
                        cur.execute(log_query, (transaction_idx, row_count, 2019))
                        conn.commit()
                else:
                    break
            cur.execute(f"select count(*) from {table_name} where year = 2019")
            numb_of_2019 = cur.fetchone()[0]
            start_count = row_count-numb_of_2019
        with open(input_csv_2020, 'r', encoding="cp1251") as f:
            csv_reader = csv.reader(f, delimiter=';')
            insert_query = f"INSERT INTO {table_name} VALUES ({','.join(['%s'] * (len(next(csv_reader)) + 1))})"
            log_query = f"INSERT INTO {table_log_name} VALUES ({','.join(['%s'] * 3)})"
            for idx, row in enumerate(csv_reader):
                if idx < start_count:
                    pass
                elif start_count <= idx:
                    cur.execute(insert_query, formatter(row, 2020))
                    if (idx + 1) % 1000 == 0:
                        row_count += 1000
                        transaction_idx += 1
                        print(row_count)
                        cur.execute(log_query, (transaction_idx, row_count, 2020))
                        conn.commit()
                    elif (idx + 1) % 1000 != 0 and (idx + 1) == cnt_row_2020:
                        row_count += cnt_row_2020 + cnt_row_2019 - last_transaction(conn)[1]
                        transaction_idx += 1
                        print(row_count)
                        cur.execute(log_query, (transaction_idx, row_count, 2019))
                        conn.commit()
                else:
                    break
        all_count_select = f"select count(*) from {table_name}"
        cur.execute(all_count_select)
        print(f"Кількість рядків у таблиці: {cur.fetchone()[0]}")
        cur.close()
    except (Exception, psycopg2.Error):
        conn.rollback()


main_task_query = (f'''
with temp_func as(
select RegName, round(avg(mathBall100), 2) as "2019" 
from {table_name} where (mathTestStatus = 'Зараховано') and (year = 2019)
group by RegName
), temp_func2 as(
select RegName, round(avg(mathBall100), 2) as "2020" 
from {table_name} where (mathTestStatus = 'Зараховано') and (year = 2020) 
group by RegName
) select temp_func.RegName, "2019", "2020" from temp_func, temp_func2 where temp_func.RegName = temp_func2.RegName
''')


def output_writer(conn):
    cur = conn.cursor()
    cur.execute(main_task_query)
    columns_name = ["Регіон", "2019 рік", "2020 рік"]
    with open(output_csv, 'w', encoding="cp1251", newline='') as wr:
        writer = csv.writer(wr, delimiter=';')
        writer.writerow(columns_name)
        for row in cur:
            writer.writerow([str(i) for i in row])
    cur.close()


def main():
    while True:
        try:
            conn = psycopg2.connect(dbname="lab1", user="Valentin", password="qwerty123", host="db")
            cursor = conn.cursor()

            cursor.execute(main_table_query)
            cursor.execute(log_table_query)

            if last_transaction(conn) is None:
                start = time.time()

            writer_table(conn)

            sec = time.time() - start
            end = time.strftime("%H:%M:%S", time.gmtime(sec))
            time_file = open(time_record, "w")
            time_file.write(end)
            time_file.close()

            output_writer(conn)

            #for test only--------------
            cursor.execute(clear_query)
            cursor.execute(clear_query2)
            #---------------------------

            conn.commit()
            conn.close()
        except psycopg2.Error as error:
            print(error)
            print("Trying to reconnect...")
            time.sleep(5)
            continue
        except Exception as error:
            print("Unexpected error: ", error)
            print("Shutting down...")
            exit()
        break


if __name__ == '__main__':
    main()