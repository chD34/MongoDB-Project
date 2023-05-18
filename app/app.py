import csv
import time
import pymongo

number_rows_1 = [18, 19, 20, 29, 30, 31, 39, 40, 41, 79, 80, 81, 88, 89, 91]
number_rows_2 = [18, 19, 20, 29, 30, 31]
header = ["out_id", "birth", "sex", "region", "area_name", "ter_name", "reg_type_name", "ter_type_name",
          'class_prof_name', "class_lang_name", 'eo_name', "eo_type_name", "eo_reg_name", 'eo_area_name',
          'eo_ter_name', 'eo_parent', 'ukr_test', 'ukr_status', 'ukr_ball_100', 'ukr_ball_12', 'ukr_ball',
          'ukr_adapt_scale', 'ukr_pt_name', 'ukr_pt_reg', 'ukr_pt_area', 'ukr_pt_ter', 'hist_test', 'hist_lang',
          'hist_status', 'hist_ball_100', 'hist_ball_12', 'hist_ball', 'hist_pt_name', 'hist_pt_reg', 'hist_pt_area',
          'hist_pt_ter', 'math_test', 'math_lang', 'math_status', 'math_ball_100', 'math_ball', 'math_ball_12',
          'math_pt_name', 'math_pt_reg', 'math_pt_area', 'math_pt_ter', 'physics_test', 'physics_lang',
          'physics_status', 'physics_ball_100', 'physics_ball_12', 'physics_ball', 'physics_pt_name', 'physics_pt_reg',
          'physics_pt_area', 'physics_pt_ter', 'chem_test', 'chem_lang', 'chem_status', "chem_ball_100", 'chem_ball_12',
          "chem_ball", "chem_pt_name", "chem_pt_reg", "chem_pt_area", "chem_pt_ter", "bio_test", "bio_lang",
          "bio_status", "bio_ball_100", " bio_ball_12", "bio_ball", "bio_pt_name", "bio_pt_reg", "bio_pt_area",
          "bio_pt_ter", "geo_test", "geo_lang", "geo_status", "geo_ball_100", "geo_ball_12", "geo_ball", "geo_pt_name",
          "geo_pt_reg", "geo_pt_area", "geo_pt_ter", 'eng_test', "eng_status", 'eng_ball_100', 'eng_ball_12',
          "eng_dpa_level", 'eng_ball', 'eng_pt_name', 'eng_pt_reg', 'eng_pt_area', 'eng_pt_ter', 'fra_test',
          'fra_status', 'fra_ball_100', 'fra_ball_12', 'fra_dpa_level', 'fra_ball', 'fra_pt_name', 'fra_pt_reg',
          'fra_pt_area', 'fra_pt_ter', 'deu_test', 'deu_status', 'deu_ball_100', 'deu_ball_12', 'deu_dpa_level',
          'deu_ball', 'deu_pt_name', 'deu_pt_reg', 'deu_pt_area', 'deu_pt_ter', 'spa_test', 'spa_status',
          'spa_ball_100', 'spa_ball_12', 'spa_dpa_level', 'spa_ball', 'spa_pt_name', 'spa_pt_reg', 'spa_pt_area',
          'spa_pt_ter', 'year']

conn = pymongo.MongoClient('mongodb://user:password@mongodb')

db = conn.zno
collection = db.zno_collection


def pymongo_populate_2019(csv_reader, start_idx=1, end_idx=400):
    while start_idx < 1000:
        list1 = csv_reader[start_idx:end_idx + 1]
        for row in list1:
            for i in range(len(row)):
                if row[i] == 'null':
                    row[i] = None
            for i in range(len(row)):
                if row[i] is not None:
                    row[i] = row[i].replace(',', '.')
                if i in number_rows_1 and row[i] is not None:
                    row[i] = float(row[i])

            collection.insert_one(dict(zip(header, row + [2019])))

        print(end_idx, "rows loaded to db")
        start_idx += 400
        end_idx += 400
    print('Data from 2019 loaded')


def pymongo_populate_2021(csv_reader, start_idx=1, end_idx=400):
    while start_idx < 1000:
        list1 = csv_reader[start_idx:end_idx + 1]
        for row in list1:
            for i in range(len(row)):
                if row[i] == 'null':
                    row[i] = None
            for i in range(len(row)):
                if row[i] is not None:
                    row[i] = row[i].replace(',', '.')
                if i in number_rows_2 and row[i] is not None:
                    row[i] = float(row[i])
            if row[47] is not None:
                new_row = row[0:26] + row[37:50] + [row[50]] + [row[53]] + [row[51]] + row[54:58] + row[67:147]
            else:
                new_row = row[0:26] + row[37:49] + [row[49]] + [row[50]] + [row[62]] + [row[61]] + row[63:147]
            # print(row[50])
            collection.insert_one(dict(zip(header, new_row + [2021])))

        print(end_idx, "rows loaded to db")
        start_idx += 400
        end_idx += 400
    print('Data from 2021 loaded')


def aggregation_select():
    aggregation = collection.aggregate([
        {
            '$match': {
                'math_status': 'Зараховано'
            }
        },
        {
            '$group': {
                '_id': {'region': '$region', 'year': '$year'},
                'max_ball_100': {'$max': '$math_ball_100'},
            }
        },
        {
            '$sort': {"region": 1}
        }
    ])

    with open('../result.csv', 'w', encoding="utf-8") as result:
        writer = csv.writer(result)
        writer.writerow(['year', 'region', 'max_ball_100'])

        for key in aggregation:
            seq = [key['_id']['year'], key['_id']['region'], key['max_ball_100']]
            writer.writerow(seq)


start_time = time.time()

with open("Odata2019File.csv", 'r', encoding="cp1251") as inf:
    reader1 = list(csv.reader(inf, delimiter=';'))
    row_num1 = len(reader1)
    print('Number of rows 2019: ', row_num1)

pymongo_populate_2019(reader1)

with open("Odata2021File.csv", 'r', encoding="utf-8") as inf:
    reader2 = list(csv.reader(inf, delimiter=';'))
    row_num2 = len(reader2)
    print('Number of rows 2021: ', row_num2)

pymongo_populate_2021(reader2)

aggregation_select()

s = f" time --- {(time.time() - start_time)/60} minutes ---"
f = open('../time.txt', 'w')
f.write(s)
f.close()

db.drop_collection(collection)
