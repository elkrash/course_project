import sys, csv, os
import sqlalchemy
from sqlalchemy import *
from sqlalchemy_utils import database_exists, create_database
import xlrd
from datetime import  *

#Удаление лишних переводов строк в файле csv и удаление полей, которые не нужно будет в дальнейшем извлекать
def prepare_csv (input, output,csv_file_fields):
    r = csv.DictReader(input)
    w = csv.DictWriter(output,fieldnames=csv_file_fields)
    w.writeheader()
    for record in r:
        row=dict()
        for key,value in record.items():
            if key in csv_file_fields:
                row[key]=value.replace("\r\n", " ").replace("\n", " ").replace("\r", " ").replace("$", "").replace(",", "")
                if row[key]=="":
                    row[key]=0

        w.writerow(row)

def to_sqla_class(value):
    if (value.lower() == 'integer'): return Integer
    elif (value.lower() == 'varchar'): return String
    elif (value.lower() == 'date'): return Date
    elif (value.lower() == 'decimal'): return DECIMAL
    elif (value.lower() == 'tinyint'): return SMALLINT

def create_column(value):
    if value['length']=='':
        type=to_sqla_class(value['type'])
    else:
        l = [int(num) for num in str(value['length']).split(",")]
        s=to_sqla_class(value['type'])
        type= s(*tuple(l))

    return Column(
        value['field'], 
        type,
        ForeignKey("" + value['fk_src_table'] + "." + value['fk_src_field']),
        nullable= False
    ) if value['key_type']=='fk' else Column(
        value['field'], 
        type,
        primary_key= value['key_type']=='pk',
        nullable= value['key_type']!='pk'
    )

def create_db_tables():
    meta_create=[]

    rb = xlrd.open_workbook('airbnb_metadata.xls',formatting_info=True)
    sheet = rb.sheet_by_name('create_tables')

    for rownum in range(1,sheet.nrows):
        row = sheet.row_values(rownum)
        meta_create.append( {
            'table': row[0],
            'field': row[1],
            'type': row[2],
            'length': row[3],
            'key_type': row[4],
            'fk_src_table': row[5],
            'fk_src_field': row[6],
        })

    tables = dict()
    for row in meta_create:
        if row['table'] not in tables: 
            tables[row['table']] = []
        tables[row['table']].append(create_column(row))
    
    for table in tables:
        Table(table, metadata, *tuple(tables[table]))   

    metadata.create_all(engine)

def values_generate (table,csv_dict,source_file,row,pk):
    
    values=[]
    load_date=date.today()
    actual_date=meta_engine['actual_date']
    for col in table.columns:
        if len(col.foreign_keys) !=0:
            values.append(pk[str(list(col.foreign_keys)[0].column)])
        else:
            if col.name=='source_file': values.append(source_file)
            elif col.name=='load_date': values.append(str(load_date))
            elif col.name=='actual_date': values.append(str(actual_date))
            else:
                csv_value=row[csv_dict[table.name][str(col.name)][0]]
                values.append(csv_value)
    return values


def load_data():
    
    sheet = rb.sheet_by_name('load_data')

    meta_load=[]

    for rownum in range(1,sheet.nrows):
        row = sheet.row_values(rownum)
        meta_load.append( {
            'table': row[0],
            'field': row[1],
            'csv_field': row[2],
        })
    
    csvf={}#словарь содержащий название файла csv и все поля, которые нужно будет из него извлечь
    csvf[meta_engine['source_file']]=[]
    for row in meta_load:
        csvf[meta_engine['source_file']].append(row['csv_field'])

    csv_dict={}#словарь с key=названию таблицы value=(словарь с key=названию поля value=[поле csv файла])
    for row in meta_load:
       if row['table'] not in csv_dict:
            csv_dict[row['table']]={}
            if row['field'] not in csv_dict[row['table']]:
                csv_dict[row['table']][row['field']]=[]
       csv_dict[row['table']][row['field']].append(row['csv_field'])

    for key,value in csvf.items():
        input_csv = open(key, "r", newline='',encoding='utf-8')
        output_csv = open(key.split(".")[0]+"_new.csv", "w", newline='',encoding='utf-8')
        prepare_csv (input_csv, output_csv, value)
    
    for key,value in csvf.items():
        input_csv = open(key.split(".")[0]+"_new.csv", "r", newline='',encoding='utf-8')
        reader = csv.DictReader(input_csv)
        pk={}
        for row in reader:   
            for table in metadata.sorted_tables:
                values=values_generate(table,csv_dict,key,row,pk)
                try:
                    en=engine.execute(insert(table, values=values).execution_options(autocommit=True))
                    pk.update({str(list(table.primary_key)[0]):int(en.inserted_primary_key[0])})
                except:
                    if len(list(table.primary_key)) !=0:
                        pk.update({str(list(table.primary_key)[0]):row[csv_dict[table.name][str(list(table.primary_key)[0].name)][0]]})
                    

meta_engine={}

rb = xlrd.open_workbook('airbnb_metadata.xls',formatting_info=True)
sheet = rb.sheet_by_name('create_engine')

for rownum in range(sheet.nrows):
    row = sheet.row_values(rownum)
    meta_engine[row[0]]=row[1]

engine = create_engine(("mysql+mysqldb://{username}:{password}@{host}/{database}").format(username=meta_engine['user'], password=meta_engine['password'], host=meta_engine['host'], database=meta_engine['dbname']), pool_recycle=7200)
metadata=MetaData()
metadata.bind = engine

if database_exists(engine.url):
    if meta_engine['create_db']=='true':
        print('Database {database} is already exists'.format(database=meta_engine['dbname']))
    else:
        metadata.reflect()
else:
    if meta_engine['create_db']=='true':
        create_database(engine.url)

if meta_engine['drop_existing_db_tables_1-st']=='true':
    engine.execute('DROP DATABASE {database}'.format(database=meta_engine['dbname']))
    engine = create_engine(("mysql+mysqldb://{username}:{password}@{host}/{database}").format(username=meta_engine['user'], password=meta_engine['password'], host=meta_engine['host'], database=meta_engine['dbname']), pool_recycle=7200)
    metadata=MetaData()
    metadata.bind = engine
    create_database(engine.url)

if meta_engine['create_db_tables']=='true':
    create_db_tables()

if meta_engine['load_data']=='true':
    load_data()