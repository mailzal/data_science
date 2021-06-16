#basic import
import csv
import pandas as pd
import numpy as np
import datetime

# read csv
trx = pd.read_csv(r'C:\Users\Rizal\Documents\all_donatur_2019_ori.csv', sep=';', encoding='latin1')

# read excel
file1 = pd.read_excel(open(main_path+file_name, 'rb'),
              sheet_name='Sheet1')

# Read csv specific column
fields = ['star_name', 'ra']
df = pd.read_csv('data.csv', skipinitialspace=True, usecols=fields)

# write csv
trx.to_csv(full_file_name+'.csv', sep = ';', quoting = csv.QUOTE_ALL, encoding = "utf8", index = False)

# make dataframe
column_name = ['tahun_rmd','tanggal_awal_rmd','tanggal_akhir_rmd']
ramadhan_df = pd.DataFrame(
    {column_name[0]: year_list,
     column_name[1]: tgl_awal_list,
     column_name[2]: tgl_akhir_list
    })

# drop dupplicate by column value
berisi.drop_duplicates(subset="ID_DONATUR", keep='first', inplace=False)

# count unique value
berisi['ID_DONATUR'].nunique()

# lowering column name
trx.columns = map(str.lower, trx.columns)

# convert to string
trx['unique'].astype(str)

#clean phone number, hp number
#set to string
trx_inder_not_paid['phone'] = trx_inder_not_paid['phone'].astype(str)

#find and set non numeric character in string then remove it
trx_inder_not_paid['phone_numeric'] = trx_inder_not_paid['phone'].str.extract('(\d+)', expand=False)

#add leading zero for prefix 8
trx_inder_not_paid['phone_numeric'] = np.where(trx_inder_not_paid['phone_numeric'].str[:1] == '8', '0' + trx_inder_not_paid['phone_numeric'], trx_inder_not_paid['phone_numeric'])

#replace prefix 62 to 0
trx_inder_not_paid['phone_numeric'] = np.where(trx_inder_not_paid['phone_numeric'].str[:2] == '62', '0' + trx_inder_not_paid['phone_numeric'].str[2:], trx_inder_not_paid['phone_numeric'])

#extract n character from left
trx_inder_not_paid['phone_numeric'].str[:1]

# get year number
year = trx_2012_2020.iloc[0]['tahun']

# lowering column name
trx_inv_inder.rename(str.lower, axis='columns', inplace=True)

# left join pandas
result = pd.merge(user_usage,
                 user_device[['use_id', 'platform', 'device']],
                 on='use_id', 
                 how='left')

# set string to datetime
tanggal_awal_rmd = '06-May-2019'
tanggal_awal_rmd_dt = datetime.datetime.strptime(tanggal_awal_rmd, '%d-%B-%Y')

# drop columns
del trx['BPN']

# drop multiple columns
trx.drop(['id_sumber_donasi_x',
       'nama_sumber_donasi_x', 'kanal_marketing_x', 'id_sumber_donasi_y',
       'nama_sumber_donasi_y', 'kanal_marketing_y'], axis = 1, inplace=True)

# get date number difference from datetime column
trx['selisih_tgl_bayar'] = (trx['tanggal_dc'] - trx['date_invoice']).days

trx['diff_days'] = trx['tanggal_dc_tanggal'] - trx['date_invoice']
trx['diff_days']=trx['diff_days']/np.timedelta64(1,'D')

# set error date to 1990-01-01
trx['tanggal_input'] = np.where(trx['tanggal_input'] == '0000-00-00', '1990-01-01', trx['tanggal_input'])

trx['tanggal_dc'] = pd.to_datetime(trx['tanggal_dc'], errors='coerce')

# rename
result.rename(columns={"tanggal": "date_dc", "waktu": "time_dc"}, inplace=True)

# find and show duplicate
trx_inv_inder['invoice_number'][trx_inv_inder.duplicated(['invoice_number'])]

# display long floating number
pd.options.display.float_format = '{:.2f}'.format

trx_2012_2020 = trx_2012.append([trx_2013,trx_2014,trx_2015,trx_2016,trx_2017,trx_2018,trx_2019,trx_2020])

# sort date
df.sort_values(by=['Date'], inplace=True, ascending=False)

# group by column then count the other column
trx[trx['payment_status'] == 'PAID'].groupby(['campaigns_id','campaigns']).sum()[['trx_amount']]

(trx['NAMA_AKUN'].append(trx['NAMA_PROGRAM']))[trx['PROGRAM_MOD'] == '-'].unique()

df.columns = df.columns.tolist()

df_sum = sum_donation.to_frame()

df_sum.reset_index(inplace=True)

# set date column as index
trx_2012_2020.set_index('tanggal', inplace=False)

# get date number
trx['tanggal_dc_tanggal'] = trx['tanggal_dc'].dt.date

# get day name
trx['HARI'] = trx['TANGGAL'].dt.weekday_name

# get month number
trx19_dec['MONTH'] = trx19_dec['TANGGAL'].dt.month
trx19_dec['MONTH'] = trx19_dec['TANGGAL'].str[0:2] #get first 2 character of column

# sort then groupby
df_sort = df.sort_values(['TANGGAL'],ascending=False).groupby('ID_DONATUR').head(1)

trx_2012_2020_sort_date.groupby('id_donatur').dates.agg({'date ': ['first', 'last']})

trx_2012_2020_sort_date_1 = trx_2012_2020_sort_date.groupby('id_donatur').first().reset_index()

# remove trailing space in string types
trx['email'] = trx['email'].str.strip()
# lower case upper case
trx['email'] = trx['email'].str.lower()
trx['email'] = trx['email'].str.upper()

# convert column to string
trx['penerimaan_id'] = trx['penerimaan_id'].astype(str)

# rearrange column of df, just change the order, delete, include the column you want
trx = trx[['id_unik', 'cabang', 'id_donatur', 'nama_donatur', 'kota',
           'provinsi', 'jenis_donatur', 'nama_akun', 'nama_program', 'tanggal',
           'bulan', 'tahun', 'brand', 'rekening', 'jumlah', 'tanggal_input',
           'indonesia_dermawan', 'keterangan', 'sumber_donasi', 'pj', 'penerimaan_id',
           'penerimaan_db', 'penerimaan_sumber']]

# function to generate id unik
def generate_id_unik(row):
    return row['penerimaan_db'] + row['penerimaan_sumber'] + row['penerimaan_id']
trx['id_unik'] = trx.apply(generate_id_unik, axis=1)

# function remove enter char
def remove_enter(row):
    return row['keterangan'].replace('\n', '').replace('\r', '')
trx['keterangan'] = trx.apply(remove_enter, axis=1)

# replace value multiple columns
cols = ["Weight","Height","BootSize","SuitSize","Type"]
df2[cols] = df2[cols].replace(['0', 0], np.nan)

#replace nan
df1 = df.replace(np.nan, '', regex=True)

#find nan
df1 = df[df.isna().any(axis = 1)]

#dropna
df = df.dropna(how='any')

# get current date
current_date = date.strftime("%Y-%m-%d")

import time
DATETIME = time.strftime('%Y%m%d-%H%M%S')

# get time
trx_inv['WAKTU'] = trx_inv['WAKTU'].dt.strftime('%H:%M')
.dt.strftime("%b")
.dt.strftime("%Y")

# get
# datetime plus or minus date
# datetime manipulation
today = datetime.date.today()
yesterday = today - datetime.timedelta(days = 1)
yesterday2 = today - datetime.timedelta(days = 2)

#timedelta less than one day, to string
df['duration1'] = df['duration'].astype(str).str[-18:-10]

# get date range from back from current date
if(hourly_status == 1):
    #trx = trx[trx['TANGGAL'].dt.month == date.month]
    day_from = pd.Timestamp.today() - datetime.timedelta(days = 30)
    trx = trx[(trx['TANGGAL'] >= day_from)&(trx['TANGGAL'] <= pd.Timestamp.today())]

# get exact date of previous year
last = today.replace(year=today.year - 1)

# define main path to read file
if(local_status == 1):
    main_path = 'C:\\Users\\Rizal\\Documents\\extracted_data\\transaction\\'
else:
    main_path = '/home/extracted_data/transaction/' # ubuntu server

# get all transaction
missing_values = ["n/a", "na"]

trx_2012 = pd.read_csv(main_path + 'all_trx_2012.csv', sep=';', encoding='latin1', na_values=missing_values)

trx_2013 = pd.read_csv(main_path + 'all_trx_2013.csv', sep=';', encoding='latin1', na_values=missing_values)

trx_2014 = pd.read_csv(main_path + 'all_trx_2014.csv', sep=';', encoding='latin1', na_values=missing_values)

trx_2015 = pd.read_csv(main_path + 'all_trx_2015.csv', sep=';', encoding='latin1', na_values=missing_values)

trx_2016 = pd.read_csv(main_path + 'all_trx_2016.csv', sep=';', encoding='latin1', na_values=missing_values)

trx_2017 = pd.read_csv(main_path + 'all_trx_2017.csv', sep=';', encoding='latin1', na_values=missing_values)

trx_2018 = pd.read_csv(main_path + 'all_trx_2018.csv', sep=';', encoding='latin1', na_values=missing_values)

trx_2019 = pd.read_csv(main_path + 'all_trx_2019.csv', sep=';', encoding='latin1', na_values=missing_values)

trx_2020 = pd.read_csv(main_path + 'all_trx_2020.csv', sep=';', encoding='latin1', na_values=missing_values)

trx_2021 = pd.read_csv(main_path + 'all_trx_2021.csv', sep=';', encoding='latin1', na_values=missing_values)

#trx_2020 = pd.read_csv(main_path + '20200402-09_00_all_trx_2020.csv', sep=';', encoding='latin1')

trx_2020 = pd.read_csv(main_path + 'all_trx_2020_jan_now.csv', sep=';', encoding='latin1')

trx[trx['BPN'] == 'GPN'].sort_values(by='TANGGAL')

#count, sum column value
trx.sum()['JUMLAH']

#dropna subset, choose which row to be deleted based on column condition empty value
trx.dropna(subset=['email'], inplace=True)

# Making a list of missing value types
missing_values = ["n/a", "na", "--"]
df = pd.read_csv("property data.csv", na_values = missing_values)

# Replace using median 
median = df['NUM_BEDROOMS'].median()
df['NUM_BEDROOMS'].fillna(median, inplace=True)

# Total missing values for each feature
print(df.isnull().sum())

# dataframe row count
len(df.index)

# define missing value list, then apply immediately after reading the csv
missing_values = ["n/a", "na"]
trx20 = pd.read_csv(r'C:\Users\rizal\Documents\GitHub\ACT-Data-Science\result\20200427-13_00_all_trx_2020_jan_now.csv', sep=';', encoding='latin1', na_values=missing_values)

load_sql = "LOAD DATA LOCAL INFILE 'test.csv' INTO TABLE analytics.master_monetary FIELDS TERMINATED BY ';' ENCLOSED BY '\"' IGNORE 1 LINES (CABANG, ID_DONATUR, NAMA_DONATUR, JENIS_DONATUR, NAMA_AKUN, NAMA_PROGRAM, TANGGAL, BULAN, TAHUN, BRAND, REKENING, JUMLAH, TANGGAL_INPUT, INDONESIA_DERMAWAN, KETERANGAN, SUMBER_DONASI, PJ, PENERIMAAN, CHANNEL, REKAP_CHANNEL, BPN, REKAP_PROGRAM, AKUN_MOD, PROGRAM_MOD, FIRST_TRX_INDER, TANGGAL_PERTAMA_TRANSAKSI_INDER, NEW_RETENTION_INDER, FIRST_TRX_NON_INDER, TANGGAL_PERTAMA_TRANSAKSI_NON_INDER, NEW_RETENTION_NON_INDER, SEGMENT_PUBLIK, SEGMENT_MITRA, TIME_STAMP, DATE_STAMP, STATUS_ACTIVE) SET c1 = STR_TO_DATE(@c1,'%d-%b-%y %H:%i:%s');"

# count donation by campaign per date of invoice. change count to sum for donation sum
result[result['payment_status'] == 'PAID'].groupby(['campaigns_id', pd.Grouper(key='date_invoice', freq='M')])['trx_amount'].count()

# sql replace new line, enter
REPLACE(REPLACE(pa.alamat,CHAR(10),''),CHAR(13),' ')

# set integer to column
df['col'] = pd.to_numeric(df['col'], downcast='integer').fillna(0)

#loop for edit several column except onef
cols=[i for i in program_lower_clean.columns if i not in ["id_donatur"]]
for col in cols:
    program_lower_clean[col] = pd.to_numeric(program_lower_clean[col], downcast='integer').fillna(0)

# isin lower
df[df['column'].str.lower().isin([x.lower() for x in mylist])]

# isin lower and rstrip (trailing spaces)
trx_qurban_20 = trx_qurban[trx_qurban['nama_program'].str.rstrip().str.lower().isin([x.lower() for x in list_qurban_2020])]

# print in python
a = 1
print("bulan {}".format(a))

# delete multiple column
df.drop(['C', 'D'], axis = 1) 

# group by sum
trx_2012_2020_sum2 = trx_2012_2020.groupby(['id_donatur']).sum()['jumlah'].to_frame()
trx_2012_2020_sum2.reset_index(inplace=True)

trx_2012_2020_count2 = trx_2012_2020.groupby(['id_donatur']).count()['jumlah'].to_frame()
trx_2012_2020_count2.reset_index(inplace=True)

#group by 2 columns then sum
clus = trx_jun_jul.groupby(['cabang','nama_akun'], as_index=False)['jumlah'].sum()
#sort value by 2 columns
df = clus.sort_values(['cabang', 'jumlah'], ascending=[True, False]).groupby('cabang').head(5)

data[‘Item_Weight’].fillna(value=data[‘Item_Weight’].mean(), inplace=True)

# read sql for jinja
sqlfile = "sql/sm_bounds_current.sql"
sql = open(sqlfile, mode='r', encoding='utf-8-sig').read()

# import external module from other python file, using external path
import sys
sys.path.append("C:\\Users\\rizal\\Documents\\GitHub\\ACT-Data-Science\\library\\")
from sql_templates_base import apply_sql_template

first_inder = pd.read_csv(main_path2 + 'donatur_first_transaction_inder.csv', sep=';', encoding='latin1', dtype={"donatur_id":"string"})

#get last donation date
trx_2012_2020_sort = trx_2012_2020.sort_values(by=['id_donatur','tanggal']).groupby('id_donatur')['tanggal'].last()
trx_2012_2020_sort.to_frame().reset_index(inplace=True)

#get first and last donation date
trx_gq_sort2 = trx_gq.groupby(['id_donatur']).tanggal.apply(lambda x: [min(x),max(x)])
trx_sort = trx.groupby(['id_donatur']).tanggal.apply(lambda x: min(x))
trx_sort_f = trx_sort.to_frame()
trx_sort_f.reset_index(inplace=True)

#new method, using lambda
grouped = trx_2012_2020.groupby(["id_donatur"]).apply(lambda x: x.sort_values(["tanggal"], ascending = False)).reset_index(drop=True)
donatur_last_transaction_info = grouped.groupby('id_donatur').head(1)

#worked and used so far
#get first transaction info
final_data = trx_2012_2020.sort_values(['tanggal'],ascending=True).groupby('id_donatur').head(1)

# sort all row in dataframe, take really long time
trx_sort = trx[['id_donatur','tanggal','akun_mod','brand']].sort_values(['id_donatur'], ascending=True) \
    .groupby(['id_donatur'], sort=False) \
    .apply(lambda x: x.sort_values(['tanggal'], ascending=True)) \
    .reset_index(drop=True)

trx[['keterangan','brand']][trx['keterangan'].str.contains('|'.join(campaigns_id))]

#extract id campaigns from keterangan column
trx_inv_inder['phase1'] = np.where(trx_inv_inder['keterangan'].str.contains('<br>'),trx_inv_inder['keterangan'].str.split('<br>',1).str.get(1), trx_inv_inder['keterangan'])

trx_inv_inder['phase2'] = np.where(trx_inv_inder['phase1'].str.contains(':'),trx_inv_inder['phase1'].str.split(':',1).str.get(1), trx_inv_inder['phase1'])

trx_inv_inder['phase3'] = np.where(trx_inv_inder['phase2'].str.contains('-'),trx_inv_inder['phase2'].str.split('-',1).str.get(0), trx_inv_inder['phase2'])

#get month name from month number
import calendar
df['Month'] = df['Month'].apply(lambda x: calendar.month_abbr[x])

#example lambda function
all_data['column'] = all['Purchase Address'].apply(lambda x: x.split(',')[1])

def get_city(address):
  return address.split(',')[1]
all_data['column'] = all['Purchase Address'].apply(lambda x: get_city())

#choose all column except one
month_name = trx_2012_2020_agg[trx_2012_2020_agg.columns.difference(['id_donatur'])].columns

# pivot transaction, donation
final_data = trx_2012_2020.pivot_table(index='tanggal', values='jumlah', aggfunc=[np.sum, len])

# pivot donatur, unique, not unique
final_data_d = trx_2012_2020.pivot_table(index='tanggal', values='id_donatur',
aggfunc=pd.Series.nunique
#aggfunc='count'
)

# clean non ascii and other latine character
final_data['email'] = final_data['email'].str.encode('ascii', 'ignore').str.decode('ascii')

# row count as new column
trx['row_id'] = np.arange(trx.shape[0])

pd.pivot_table(trx
               , values='jumlah'
               , index=['id_donatur']
               , columns=['rekap_program']
               , aggfunc='count'
               , fill_value=0
              )

# columns masking, replace with 1 for non 0
trx_2012_2020_agg[month_name] = trx_2012_2020_agg[month_name].mask(trx_2012_2020_agg[month_name]> 0, 1)

# plot python
import seaborn as sns
import matplotlib.pyplot as plt

#if column exist in dataframe
if 'WAKTU' in trx:

Jupyter wall/execution time
%%time

trx[trx['date_invoice'].dt.date.astype(str) == '2019-09-09']

# function convert date
def convert_datetime(row, column):
    row[column] = np.where(row[column] == '0000-00-00', '1990-01-01', row[column])
    row[column] = pd.to_datetime(row[column], errors='coerce')
convert_datetime(trx, 'date_invoice')

# function set nol tu kolom2 kambing
columnNa = ['kambing_global', 'sapi_global', 'kambing_syam', 'sapi_syam','kambing_yaman', 'sapi_yaman', 'unta', 'jumlah_setara_kambing']
def set_nol(row, column):
  row[column] = np.where(row[column].isna(), 0, row[column])
for column in columnNa:
  set_nol(result, column)


def convert_ascii(row, column):
    row[column] = trx[column].str.encode('ascii', 'ignore').str.decode('ascii')
convert_ascii(trx, convert)

# select data by year-month, partial datetime select
df['date'].dt.to_period('M') == pd.to_datetime('2017-06').to_period('M')

# rename column
trx.columns = trx.columns.str.replace(' ','_')

#python API
r = requests.get('https://xkcd.com/353/')
print(r)
print(dir(r))
print(help(r))
print(r.text) # print content of response in unicode
print(r.response) # print content in byte (picture, etc)
print(r.url)

payload = {'page' : 2, 'count':25}
r = requests.get('https://httpbin.org/get', params=payload)

payload = {'username':'rizal', 'password':'act'}
r = requests.post('https://httpbin.org/post', data=payload)
r_dict = r.json()
r_dict['form']

# python call then append to dict, to dataframe
col_names =  ['id','name','profile_name','profile_labels']
df_profile  = pd.DataFrame(columns = col_names)

class Profile(object):
    def __init__(self, id, name, profile_name, profile_labels):
        self.id = id
        self.name = name
        self.profile_name = profile_name
        self.profile_labels = profile_labels

if(response_dict['success'] == True):
    content = response_dict['profiles']
    print(type(content))
    print(len(content))
    for obj in content:
        profile = Profile(obj['id'], obj['name'], obj['profile_name'], obj['profile_labels'])
        df_profile.loc[len(df_profile)] = [obj['id'], obj['name'], obj['profile_name'], obj['profile_labels']]
        list_profile.append(profile)
        list_profile_l.append(obj)
        #print('---')

# python fill dataframe using list
col_names =  ['date','ids','msg']
df_log  = pd.DataFrame(columns = col_names)

%%time
rows = []
for line in log_data:
    rows.append([line.split(" ", 2)[0], line.split(" ", 2)[1], line.split(" ", 2)[2]])

df = pd.DataFrame(rows, columns=col_names)

#python dynamic variable
globals()[f"file_{init}"]

#read all files from directory
from os import listdir
from os.path import isfile, join
onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]

#size of list
len(items)