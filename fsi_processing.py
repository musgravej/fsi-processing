import os
import openpyxl
import sqlite3
import datetime
import csv
import configparser

# TODO export letter merge by version order


class Global:
    def __init__(self):
        self.in_service = ('MN_ANOKA', 'MN_ANOKA', 'MN_BLUE EARTH', 'MN_BROWN', 'MN_BLUE EARTH',
                           'MN_BROWN', 'NE_BUTLER', 'MN_CARVER', 'MN_CHISAGO', 'MN_CARVER',
                           'NE_CASS', 'MN_CHISAGO', 'MN_DAKOTA', 'MN_DODGE', 'MN_DAKOTA',
                           'NE_DODGE', 'NE_DOUGLAS', 'MN_FARIBAULT', 'MN_FILLMORE', 'MN_FREEBORN',
                           'MN_FARIBAULT', 'MN_FILLMORE', 'MN_FREEBORN', 'MN_HENNEPIN', 'MN_HOUSTON',
                           'IA_HARRISON', 'MN_HENNEPIN', 'MN_HOUSTON', 'MN_ISANTI', 'MN_ISANTI', 'MN_KANDIYOHI',
                           'MN_KANDIYOHI', 'NE_LANCASTER', 'MN_MARTIN', 'MN_MOWER', 'MN_MARTIN', 'IA_MILLS',
                           'MN_MOWER', 'MN_NICOLLET', 'MN_NICOLLET', 'MN_OLMSTED', 'MN_OLMSTED', 'IA_POTTAWATTAMIE',
                           'MN_RAMSEY', 'MN_RAMSEY', 'MN_SCOTT', 'MN_SHERBURNE', 'MN_STEARNS', 'MN_STEELE',
                           'NE_SARPY', 'NE_SAUNDERS', 'MN_SCOTT', 'MN_SHERBURNE', 'MN_STEARNS',
                           'MN_STEELE', 'MN_WABASHA', 'MN_WASECA', 'MN_WASHINGTON', 'MN_WATONWAN',
                           'MN_WINONA', 'MN_WRIGHT', 'MN_WABASHA', 'MN_WASECA', 'NE_WASHINGTON',
                           'MN_WATONWAN', 'MN_WINONA', 'MN_WRIGHT')

        self.tracking_codes = {'ML6': '2020 ML Guide', 'FSI20 ML2': '2020 ML Guide',
                               'FSI20 ML7': '2020 ML Guide', 'FSI20 ML5': '2020 ML Guide',
                               'FSI20 ML3': '2010 Adv Sol CHI', 'FSI20 ML4': '2010 Adv Sol CHI'}

        self.mn_counties = {'ANOKA': 'TC-TCM', 'CARVER': 'TC-TCM', 'DAKOTA': 'TC-TCM', 'HENNEPIN': 'TC-TCM',
                            'RAMSEY': 'TC-TCM', 'SCOTT': 'TC-TCM', 'WASHINGTON': 'TC-TCM',
                            'CHISAGO': 'TC-GTCM', 'ISANTI': 'TC-GTCM', 'STEARNS': 'TC-GTCM',
                            'KANDIYOHI': 'TC-GTCM', 'WRIGHT': 'TC-GTCM', 'SHERBURNE': 'TC-GTCM',
                            'BLUE EARTH': 'TC-SEMN', 'BROWN': 'TC-SEMN', 'DODGE': 'TC-SEMN',
                            'FARIBAULT': 'TC-SEMN', 'FILLMORE': 'TC-SEMN', 'FREEBORN': 'TC-SEMN',
                            'HOUSTON': 'TC-SEMN', 'MARTIN': 'TC-SEMN', 'MOWER': 'TC-SEMN',
                            'NICOLLET': 'TC-SEMN', 'OLMSTED': 'TC-SEMN', 'STEELE': 'TC-SEMN',
                            'WABASHA': 'TC-SEMN', 'WASECA': 'TC-SEMN', 'WATONWAN': 'TC-SEMN',
                            'WINONA': 'TC-SEMN'}

        self.merge_letter_header = ['Campaign', 'Individual_First_Name_1', 'Individual_Last_Name_1',
                                    'Individual_First_Name_2', 'Individual_Last_Name_2', 'Address_1',
                                    'Address_2', 'City', 'State', 'Zip', 'County', 'Unique_ID',
                                    'mid', 'art_code', 'kit'
                                    ]
        
        self.excel_import_path = ""
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

        self.header_web_lead = ['line number', 'Transaction Type', 'Transaction Date', 'Person ID', 'Title',
                                'Last Name', 'First Name', 'Middle Name', 'Suffix', 'Birth Date', 'Gender',
                                'Address Line 1', 'Address Line 2', 'City', 'County', 'State Code', 'Zipcode',
                                'Phone Number', 'Email Address', 'Response Type Code', 'Tracking Code',
                                'Fulfillment Package Code', 'Call Permission', 'Email Permission']

        self.header_outside_area = ['process_date', 'mid', 'first_name', 'last_name', 'address_1',
                                    'address_2', 'city', 'state', 'zip', 'telephone', 'email',
                                    'other', 'county', 'proc_notes']

    def initialize_config(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.excel_import_path = config['PATHS']['EXCEL_IMPORT_PATH']


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


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
               ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);")

        if n != 0:
            cursor.execute(sql, (fle, n, datetime.datetime.strftime(row_data[0], "%Y-%m-%d"), row_data[1], row_data[2],
                                 row_data[3], row_data[4], row_data[5], row_data[6], row_data[7], row_data[8],
                                 row_data[9], row_data[10], row_data[11], row_data[12], None, None,
                                 None, None, None, None, None, None, None, None))
            conn.commit()

    conn.close()


def init_db():

    if not os.path.exists(os.path.join(g.excel_import_path, 'cass_files')):
        os.mkdir(os.path.join(g.excel_import_path, 'cass_files'))

    if not os.path.exists(os.path.join(g.excel_import_path, 'ftp_transfer')):
        os.mkdir(os.path.join(g.excel_import_path, 'ftp_transfer'))
        os.makedirs(os.path.join(g.excel_import_path, 'ftp_transfer', 'complete'))

    conn = sqlite3.connect(database=g.database)
    cursor = conn.cursor()

    sql1 = ("CREATE table `records` ("
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
            "`cass_zip` VARCHAR(20) NULL DEFAULT NULL, "
            "`proc_notes` VARCHAR(100) NULL DEFAULT NULL,"
            "`kit_code` VARCHAR(25) NULL DEFAULT NULL,"
            "`export_for_ftp` DATETIME NULL DEFAULT NULL);")

    sql2 = ("CREATE table `in_service` ("
            "`state` VARCHAR(2) NULL DEFAULT NULL,"
            "`county` VARCHAR(50) NULL DEFAULT NULL);")

    cursor.execute("DROP TABLE IF EXISTS `records`;")
    cursor.execute("DROP TABLE IF EXISTS `in_service`;")
    cursor.execute("VACUUM;")
    cursor.execute(sql1)
    cursor.execute(sql2)

    conn.commit()

    for rec in g.in_service:
        st, county = rec.split("_")[0], rec.split("_")[1]
        cursor.execute("INSERT INTO `in_service` VALUES (?,?);", (st, county,))

    conn.commit()
    conn.close()


def update_cass_results():
    conn = sqlite3.connect(database=g.database)
    cursor = conn.cursor()

    sql1 = ("UPDATE `records` SET `proc_notes` = 'out of area' WHERE "
            "UPPER(cass_state||county) NOT IN "
            "(SELECT UPPER(b.state||b.county) FROM `in_service` b) "
            "AND `export_for_ftp` IS NULL;")

    sql2 = ("UPDATE `records` SET `proc_notes` = 'in area' WHERE "
            "UPPER(cass_state||county) IN "
            "(SELECT UPPER(b.state||b.county) FROM `in_service` b) "
            "AND `export_for_ftp` IS NULL;")

    cursor.execute(sql1)
    cursor.execute(sql2)
    conn.commit()
    conn.close()


def update_kit_code():
    conn = sqlite3.connect(database=g.database)
    cursor = conn.cursor()

    sql1 = "SELECT * FROM `records` WHERE `proc_notes` = 'in area' AND `export_for_ftp` is NULL;"
    cursor.execute(sql1)
    results = cursor.fetchall()

    for line in results:
        rec_state = line[21]

        if rec_state == 'IA' or rec_state == 'NE':
            sql1 = ("UPDATE `records` SET `kit_code` = 'OMA' WHERE "
                    "UPPER(filename||recno) = UPPER(?||?);")
            cursor.execute(sql1, (line[0], line[1],))

        if rec_state == 'MN':
            kit_code = g.mn_counties[str(line[16]).upper()]
            sql1 = ("UPDATE `records` SET `kit_code` = ? WHERE "
                    "UPPER(filename||recno) = UPPER(?||?);")
            cursor.execute(sql1, (kit_code, line[0], line[1],))

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
    print(f"Updating table with {file_path}")

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


def write_web_lead_file(process_file):
    conn = sqlite3.connect(database=g.database)
    conn.row_factory = dict_factory
    cursor = conn.cursor()

    sql1 = ("SELECT `process_date`, `mid`, TRIM(`first_name`) 'first_name', TRIM(`last_name`) 'last_name', "
            "TRIM(`address_1`) 'address_1', TRIM(`address_2`) 'address_2', TRIM(`city`) 'city', "
            "`state`, `zip`, `telephone`, TRIM(`email`) 'email',"
            "`other`, `county`, TRIM(`proc_notes`) 'proc_notes', `filename`, `recno` FROM `records` "
            "WHERE `proc_notes` = 'out of area' AND `export_for_ftp` IS NULL;")

    cursor.execute(sql1)
    out_of_area = cursor.fetchall()

    sql2 = "SELECT * FROM `records` WHERE `proc_notes` = 'in area' AND `export_for_ftp` IS NULL;"
    cursor.execute(sql2)
    in_area = cursor.fetchall()

    dt = datetime.datetime.now()
    trans_date = datetime.datetime.strftime(dt, "%m/%d/%Y %H:%M")
    file_date = datetime.datetime.strftime(dt, "%Y%m%d%H%M%S")

    outside_area_file = f"Outside Area_{file_date}.csv"
    web_lead_file = f"medica_web_{file_date}.txt"
    letter_merge_file = f"fsi_letter_merge_{file_date}.txt"

    with open(os.path.join(g.excel_import_path, 'ftp_transfer', outside_area_file), 'w+', newline="") as s:
        csvw = csv.DictWriter(s, g.header_outside_area, delimiter=",", quoting=csv.QUOTE_ALL)
        csvw.writeheader()
        for rec in out_of_area:

            w = {'process_date': rec['process_date'],
                 'mid': rec['mid'],
                 'first_name': rec['first_name'],
                 'last_name': rec['last_name'],
                 'address_1': rec['address_1'],
                 'address_2': rec['address_2'],
                 'city': rec['city'],
                 'state': rec['state'],
                 'zip': rec['zip'],
                 'telephone': rec['telephone'],
                 'email': rec['email'],
                 'other': rec['other'],
                 'county': rec['county'],
                 'proc_notes': rec['proc_notes']}

            csvw.writerow(w)
            sql = ("UPDATE `records` SET `export_for_ftp` = DATETIME('now', 'localtime') "
                   "WHERE `filename` = ? AND `recno` = ?;")

            cursor.execute(sql, (process_file, rec['recno'],))

    with open(os.path.join(g.excel_import_path, 'ftp_transfer', letter_merge_file), 'w+', newline="") as s:
        csvw = csv.DictWriter(s, g.merge_letter_header, delimiter="\t")
        csvw.writeheader()
        for rec in in_area:
            w = {'Campaign': rec['mid'],
                 'Individual_First_Name_1': str(rec['first_name']).strip(),
                 'Individual_Last_Name_1': str(rec['last_name']).strip(),
                 'Individual_First_Name_2': '',
                 'Individual_Last_Name_2': '',
                 'Address_1': rec['cass_address_1'],
                 'Address_2': rec['cass_address_2'],
                 'City': rec['cass_city'],
                 'State': rec['cass_state'],
                 'Zip': rec['cass_zip'],
                 'County': rec['county'],
                 'Unique_ID': '',
                 'mid': rec['mid'],
                 'art_code': '',
                 'kit': rec['kit_code']}

            csvw.writerow(w)

    with open(os.path.join(g.excel_import_path, 'ftp_transfer', web_lead_file), 'w+', newline="") as s:
        csvw = csv.DictWriter(s, g.header_web_lead, delimiter="|")
        csvw.writeheader()
        for rec in in_area:
            phone = "".join(filter(lambda x: x.isdigit(), '' if rec['telephone'] is None else rec['telephone']))
            package_code = g.tracking_codes[rec['mid']]

            w = {'line number': '1',
                 'Transaction Type': 'C',
                 'Transaction Date': trans_date,
                 'Person ID': '',
                 'Title': '',
                 'Last Name': str(rec['last_name']).strip(),
                 'First Name': str(rec['first_name']).strip(),
                 'Middle Name': '',
                 'Suffix': '',
                 'Birth Date': '',
                 'Gender': '',
                 'Address Line 1': rec['cass_address_1'],
                 'Address Line 2': rec['cass_address_2'],
                 'City': rec['cass_city'],
                 'County': rec['county'],
                 'State Code': rec['cass_state'],
                 'Zipcode': rec['cass_zip'][0:5],
                 'Phone Number': phone,
                 'Email Address': rec['email'],
                 'Response Type Code': 'E',
                 'Tracking Code': rec['mid'],
                 'Fulfillment Package Code': package_code,
                 'Call Permission': '0' if phone == '' else '1',
                 'Email Permission': '0' if rec['email'] is None else '1'
                 }

            csvw.writerow(w)

            sql = ("UPDATE `records` SET `export_for_ftp` = DATETIME('now', 'localtime') "
                   "WHERE `filename` = ? AND `recno` = ?;")

            cursor.execute(sql, (process_file, rec['recno'],))

    conn.commit()
    conn.close()


def pre_cass_processing(process_file):
    import_file(process_file)
    export_for_cass(process_file)


def post_cass_processing(process_file):
    # import_from_cass('medica fsi brc data entry_20191003-cass.txt')
    # update_cass_results()
    update_kit_code()
    write_web_lead_file(process_file)


def main():
    global g
    g = Global()
    g.initialize_config()
    # init_db()

    process_file = 'Medica FSI BRC Data Entry - Preheat_20191015.xlsx'
    # process_file = 'Medica FSI BRC Data Entry - FSI 1_20191015.xlsx'
    pre_cass_processing(process_file)
    # post_cass_processing(process_file)


if __name__ == '__main__':
    main()
