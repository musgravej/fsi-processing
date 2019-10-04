import os
import openpyxl
import sqlite3
import datetime
import csv


class Global:
    def __init__(self):
        self.excel_import_path = ("\\\\jtsrv4\\data\\"
                                  "Customer Files\\In Progress\\Media Logic"
                                  "\\Return Processing\\LG 6_FSI\\")

        self.database = 'fsi_processing.db'

        self.to_cass_header = ['filename', 'source_recno', 'import_date', 'process_date',
                               'mid', 'first_name', 'middle_name', 'last_name',
                               'address_1', 'address_2', 'city', 'state',
                               'zip', 'telephone', 'email', 'other', 'county',
                               'cass_processed']

        self.from_cass_header = ['filename', 'recno', 'import_date', 'process_date',
                                 'mid', 'first_name', 'middle_name', 'last_name',
                                 'address_1', 'address_2', 'city', 'state',
                                 'zip', 'telephone', 'email', 'other', 'county',
                                 'cass_processed']


def import_file(fle):
    file_path = os.path.join(g.excel_import_path, fle)
    wb = openpyxl.load_workbook(filename=file_path)
    ws = wb.active

    conn = sqlite3.connect(database=g.database)
    cursor = conn.cursor()

    for n, row in enumerate(ws.iter_rows()):

        row_data = [cell.value for cell in row]

        sql = ("INSERT INTO `records` VALUES ("
               "?,?,DATETIME('now', 'localtime'),?,?,?,?,?,?"
               ",?,?,?,?,?,?,?,?,?,?,?,?,?,?);")

        if n != 0:
            cursor.execute(sql, (fle, n, datetime.datetime.strftime(row_data[0], "%Y-%m-%d"), row_data[1], row_data[2],
                                 row_data[3], row_data[4], row_data[5], row_data[6], row_data[7], row_data[8],
                                 row_data[9], row_data[10], row_data[11], row_data[12], None, None,
                                 None, None, None, None, None))
            conn.commit()

    conn.close()


def init_db():

    if not os.path.exists(os.path.join(g.excel_import_path, 'cass_files')):
        os.mkdir(os.path.join(g.excel_import_path, 'cass_files'))

    conn = sqlite3.connect(database=g.database)
    cursor = conn.cursor()

    sql = ("CREATE table `records` ("
           "`filename` VARCHAR(100) NULL DEFAULT NULL,"
           "`recno` INT(10) NULL DEFAULT NULL,"
           "`import_date` DATETIME NULL DEFAULT NULL,"
           "`process_date` DATE NULL DEFAULT NULL,"
           "`mid` VARCHAR(20) NULL DEFAULT NULL,"
           "`first_name` VARCHAR(100) NULL DEFAULT NULL,"
           "`middle_name` VARCHAR(100) NULL DEFAULT NULL,"
           "`last_name` VARCHAR(100) NULL DEFAULT NULL, "
           "`address_1` VARCHAR(100) NULL DEFAULT NULL, "
           "`address_2` VARCHAR(100) NULL DEFAULT NULL, "
           "`city` VARCHAR(100) NULL DEFAULT NULL, "
           "`state` VARCHAR(100) NULL DEFAULT NULL, "
           "`zip` VARCHAR(20) NULL DEFAULT NULL,"
           "`telephone` VARCHAR(100) NULL DEFAULT NULL, "
           "`email` VARCHAR(100) NULL DEFAULT NULL, "
           "`other` VARCHAR(100) NULL DEFAULT NULL,"
           "`county` VARCHAR(50) NULL DEFAULT NULL,"
           "`cass_processed` DATE NULL DEFAULT NULL,"
           "`cass_address_1` VARCHAR(100) NULL DEFAULT NULL, "
           "`cass_address_2` VARCHAR(100) NULL DEFAULT NULL, "
           "`cass_city` VARCHAR(100) NULL DEFAULT NULL, "
           "`cass_state` VARCHAR(100) NULL DEFAULT NULL, "
           "`cass_zip` VARCHAR(20) NULL DEFAULT NULL"
           ");")

    cursor.execute("DROP TABLE IF EXISTS `records`;")
    cursor.execute("VACUUM;")
    cursor.execute(sql)

    conn.commit()

    conn.close()


def export_for_cass(fle):
    conn = sqlite3.connect(database=g.database)
    cursor = conn.cursor()

    sql = "SELECT * FROM `records` WHERE `cass_processed` IS NULL;"
    cursor.execute(sql)
    results = cursor.fetchall()

    # datetime_string = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d_%H-%M-%S")
    cass_file_name = "{}.txt".format(fle[:-5])

    with open(os.path.join(
            g.excel_import_path, 'cass_files', cass_file_name), 'w+', newline='') as s:

        csvw = csv.writer(s, delimiter='\t')
        csvw.writerow(g.to_cass_header)

        for rec in results:
            csvw.writerow([rec[0], rec[1], rec[2], rec[3], rec[4], rec[5], rec[6],
                           rec[7], rec[8], rec[9], rec[10], rec[11], rec[12], rec[13], rec[14],
                           rec[15], rec[16], rec[17]])
    conn.close()


def import_from_cass(fle):
    file_path = os.path.join(g.excel_import_path, fle)

    conn = sqlite3.connect(database=g.database)
    cursor = conn.cursor()

    with open(os.path.join(g.excel_import_path, 'cass_files', fle), 'r') as f:
        csvr = csv.reader(f, delimiter='\t')
        next(csvr)

        for row in csvr:
            # print(row)

            sql1 = ("UPDATE `records` SET `cass_processed` = DATE('now', 'localtime') "
                    "WHERE (`filename`||`recno`) = (?||?);")

            sql2 = ("UPDATE `records` SET `cass_address_1` = ? "
                    "WHERE (`filename`||`recno`) = (?||?);")

            sql3 = ("UPDATE `records` SET `cass_address_2` = ? "
                    "WHERE (`filename`||`recno`) = (?||?);")

            sql4 = ("UPDATE `records` SET `cass_city` = ? "
                    "WHERE (`filename`||`recno`) = (?||?);")

            sql5 = ("UPDATE `records` SET `cass_state` = ? "
                    "WHERE (`filename`||`recno`) = (?||?);")

            sql6 = ("UPDATE `records` SET `cass_zip` = ? "
                    "WHERE (`filename`||`recno`) = (?||?);")

            sql7 = ("UPDATE `records` SET `county` = ? "
                    "WHERE (`filename`||`recno`) = (?||?);")

            cursor.execute(sql1, (row[0], row[1]))
            cursor.execute(sql2, (row[2], row[0], row[1]))
            cursor.execute(sql3, (row[3], row[0], row[1]))
            cursor.execute(sql4, (row[4], row[0], row[1]))
            cursor.execute(sql5, (row[5], row[0], row[1]))
            cursor.execute(sql6, (row[6], row[0], row[1]))
            cursor.execute(sql7, (row[7], row[0], row[1]))

            conn.commit()

    conn.close()


def start_processing(process_file):

    # import_file(process_file)
    # export_for_cass(process_file)
    import_from_cass('medica fsi brc data entry_20191003-cass.txt')


def main():
    global g
    g = Global()
    # init_db()

    process_file = 'Medica FSI BRC Data Entry_20191003.xlsx'
    start_processing(process_file)


if __name__ == '__main__':
    main()
