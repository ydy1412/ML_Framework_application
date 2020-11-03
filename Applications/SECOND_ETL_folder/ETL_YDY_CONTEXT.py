from sqlalchemy import create_engine, text

# ip to region data library
import requests
import json
import random

import pandas as pd
from datetime import datetime

import hashlib
import numpy as np
import os
import pickle
import sys

sys_path_list = sys.path
print(sys_path_list)
# ML_Train_Framework directory의 절대 경로를 환경 변수에 추가.
if sys_path_list.count(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))) == 0 :
    sys.path.append(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__)))))))

print(sys.path)

from Applications.ETC.Logger import Logger

class ETL_YDY_CONTEXT:
    def __init__(self, Local_Clickhouse_Id, Local_Clickhouse_password, Local_Clickhouse_Ip,
                 DB_NAME, TABLE_NAME):

        self.Local_Clickhouse_Id = Local_Clickhouse_Id
        self.Local_Clickhouse_password = Local_Clickhouse_password
        self.Local_Clickhouse_Ip = Local_Clickhouse_Ip
        self.DB_NAME = DB_NAME
        self.TABLE_NAME = TABLE_NAME
        self.connect_db()

        self.base_dir = os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
        self.encoder_dir = self.base_dir +'/Applications/encoder_folder'
        self.refined_data_dir = self.base_dir +'/Static_files/Refined_data_folder'

        # Add logger here
        self.logger = Logger('ETL_YDY_CONTEXT')
        ##

        print("ADVER_CATE_INFO Function", self.Extract_Adver_Cate_Info())
        print("Mobon_Com_Code Function", self.Extract_Mobon_Com_Code())
        print("MEDIA_Property_info Function", self.Extract_Media_Property_Info())
        print("Product_property_info Function ", self.Extract_Product_Property_Info())
        print("Function List :")
        print("1. Show_Inner_Df_List()")
        print("2. Extract_Sample_Log()")
        print("3. Extract_Click_View_Log(start_dttm, last_dttm, Adver_Info = False,\
                                Media_Info = False, Product_Info = False, data_size = 1000000)")

    def connect_db(self):
        self.Local_Click_House_Engine = create_engine('clickhouse://{0}:{1}@{2}/{3}'.format(self.Local_Clickhouse_Id,
                                                                                            self.Local_Clickhouse_password,
                                                                                            self.Local_Clickhouse_Ip,
                                                                                            self.DB_NAME))
        self.Local_Click_House_Conn = self.Local_Click_House_Engine.connect()

        return True

    def Extract_Adver_Cate_Info(self):
        self.connect_db()
        try:
            Adver_Cate_Df_sql = """
                    SELECT
                        ADVER_ID,
                        CTGR_SEQ_3,
                        CTGR_NM_3,
                        CTGR_SEQ_2,
                        CTGR_NM_2,
                        CTGR_SEQ_1,
                        CTGR_NM_1
                    FROM
                    TEST.ADVER_PROPERTY_INFO
                 """
            Adver_Cate_Df_sql = text(Adver_Cate_Df_sql)
            self.Adver_Cate_Df = pd.read_sql(Adver_Cate_Df_sql, self.Local_Click_House_Conn)
            return True
        except:
            print("Extract_Adver_Cate_Info error happend")
            return False

    def Extract_Media_Property_Info(self):
        self.connect_db()
        try:
            Media_Property_sql = """
                select
                   MEDIA_SCRIPT_NO,
                   MEDIASITE_NO,
                   MEDIA_ID,
                   SCRIPT_TP_CODE,
                   MEDIA_SIZE_CODE,
                   ENDING_TYPE,
                   M_BACON_YN,
                   ADVRTS_STLE_TP_CODE,
                   MEDIA_CATE_INFO,
                   MEDIA_CATE_NAME
                from
                    TEST.MEDIA_PROPERTY_INFO
                 """
            Media_Property_sql = text(Media_Property_sql)
            self.Media_Property_Df = pd.read_sql(Media_Property_sql, self.Local_Click_House_Conn)
            return self.Extract_Width_Height_Info()
        except Exception as e:
            print("{0} error happend".format(e))
            return False

    def Extract_Width_Height_Info(self):
        df = self.Mobon_Com_Code_Df
        size_mapping_df = df[(df['CODE_TP_ID'] == 'MEDIA_SIZE_CODE') & (~df['CODE_ID'].isin(['99', '16']))][
            ['CODE_ID', 'CODE_VAL']]
        size_mapping_df['WIDTH'] = size_mapping_df['CODE_VAL'].apply(lambda x: x.split('_')[0]).astype('int')
        size_mapping_df['HEIGHT'] = size_mapping_df['CODE_VAL'].apply(lambda x: x.split('_')[1]).astype('int')
        size_mapping_df['SIZE_RATIO'] = size_mapping_df['WIDTH'] / size_mapping_df['HEIGHT']
        size_mapping_df.rename(columns={'CODE_ID': 'MEDIA_SIZE_CODE'}, inplace=True)
        merged_df = pd.merge(self.Media_Property_Df,
                             size_mapping_df[['MEDIA_SIZE_CODE', 'WIDTH', 'HEIGHT', 'SIZE_RATIO']],
                             on=['MEDIA_SIZE_CODE'])
        self.Media_Property_Df = merged_df
        return True

    def Extract_Product_Property_Info(self):
        self.connect_db()
        try:
            PRODUCT_PROPERTY_INFO_sql = """
                select
                    ADVER_ID,
                   PCODE,
                   PRODUCT_CATE_NO,
                   FIRST_CATE,
                   SECOND_CATE,
                   THIRD_CATE,
                   PNM,
                   PRICE
                from
                TEST.SHOP_PROPERTY_INFO
            """
            sql_text = text(PRODUCT_PROPERTY_INFO_sql)
            self.Product_Property_Df = pd.read_sql(sql_text, self.Local_Click_House_Conn)
            return True
        except:
            return False

    def Extract_Mobon_Com_Code(self):
        self.connect_db()
        try:
            MOBON_COM_CODE_sql = """
                select
                    *
                from
                TEST.MOBON_COM_CODE
            """
            sql_text = text(MOBON_COM_CODE_sql)
            self.Mobon_Com_Code_Df = pd.read_sql(sql_text, self.Local_Click_House_Conn)
            return True
        except:
            return False

    def Show_Inner_Df_List(self):
        return ['Adver_Cate_Df', 'Product_Property_Df', 'Media_Property_Df', 'Mobon_Com_Code_Df']

    def Extract_Sample_Log(self, table_name, start_dttm, last_dttm, targeting='AD', sample_cnt=10000):
        data_per_date = int(sample_cnt / 7)
        result_df_list = []
        for WEEK in range(1, 8):
            print(WEEK, "completed")
            random_number = random.random()
            if targeting == 'ALL':
                sample_sql = """
                select 
                LOG_DTTM,
                STATS_DTTM,
                STATS_HH,
                STATS_MINUTE,
                MEDIA_SCRIPT_NO,
                SITE_CODE, 
                ADVER_ID,
                extract(REMOTE_IP,'[0-9]+.[0-9]+.[0-9]+.[0-9]+') AS REMOTE_IP,
                ADVRTS_PRDT_CODE,
                ADVRTS_TP_CODE,
                PLTFOM_TP_CODE,
                PCODE,
                PNAME,
                BROWSER_CODE,
                FREQLOG,
                T_TIME,
                KWRD_SEQ,
                GENDER,
                AGE,
                OS_CODE,
                FRAME_COMBI_KEY,
                CLICK_YN
                FROM TEST.{0}
                sample 0.1
                where 1=1 
                and STATS_DTTM >= {1}
                and STATS_DTTM <= {2}
                and toDayOfWeek(LOG_DTTM) = {0}
                and ADVRTS_PRDT_CODE = '01'
                and RANDOM_SAMPLE > {3}
                and RANDOM_SAMPLE < {4}
                limit {5}
                """.format(table_name, start_dttm, last_dttm, WEEK, random_number, random_number + 0.05, data_per_date)
            else:
                sample_sql = """
                select 
                LOG_DTTM,
                STATS_DTTM,
                STATS_HH,
                STATS_MINUTE,
                MEDIA_SCRIPT_NO,
                SITE_CODE, 
                ADVER_ID,
                extract(REMOTE_IP,'[0-9]+.[0-9]+.[0-9]+.[0-9]+') AS REMOTE_IP,
                ADVRTS_PRDT_CODE,
                ADVRTS_TP_CODE,
                PLTFOM_TP_CODE,
                PCODE,
                PNAME,
                BROWSER_CODE,
                FREQLOG,
                T_TIME,
                KWRD_SEQ,
                GENDER,
                AGE,
                OS_CODE,
                FRAME_COMBI_KEY,
                CLICK_YN
                FROM TEST.{0}
                sample 0.1
                where 1=1
                and STATS_DTTM >= {1}
                and STATS_DTTM >= {2}
                and toDayOfWeek(LOG_DTTM) = {3}
                and ADVRTS_PRDT_CODE = '01'
                and ADVRTS_TP_CODE = '{4}'
                and RANDOM_SAMPLE > {5}
                and RANDOM_SAMPLE < {6}
                limit {7}
                """.format(table_name, start_dttm, last_dttm, WEEK, targeting, random_number, random_number + 0.05,
                           data_per_date)
            sample_sql = text(sample_sql)
            sql_result = pd.read_sql(sample_sql, self.Local_Click_House_Conn)
            result_df_list.append(sql_result)
        result_df = pd.concat(result_df_list)
        return result_df

    def getting_ip(self, row):
        """This function calls the api and return the response"""
        url = f"https://freegeoip.app/json/{row}"  # getting records from getting ip address
        headers = {
            'accept': "application/json",
            'content-type': "application/json"
        }
        response = requests.request("GET", url, headers=headers)
        respond = json.loads(response.text)
        return respond

    def hash_trick(self, column, output_dim=10):
        h = hashlib.md5()
        return_array = np.zeros((column.shape[0], int(output_dim)))
        for i, row in enumerate(column):
            hashed_value = int(hashlib.md5(row.encode('UTF-8')).hexdigest(), 16) % output_dim
            return_array[i, hashed_value] += 1
        return return_array

    def load_encoder(self):
        self.encoder_dict = {}
        with open(self.encoder_dir+'/MEDIA_CATE_ENC.bin', 'rb') as f:
            self.encoder_dict['MEDIA_CATE_INFO'] = pickle.load(f)

        with open(self.encoder_dir+'/SCRIPT_TP_ENC.bin', 'rb') as f:
            self.encoder_dict['SCRIPT_TP_CODE'] = pickle.load(f)

        with open(self.encoder_dir+ '/MONTH_CATE_ENC.bin', 'rb') as f:
            self.encoder_dict['MONTH_CATE'] = pickle.load(f)

        with open(self.encoder_dir+'/WEEK_CATE_ENC.bin', 'rb') as f:
            self.encoder_dict['WEEK'] = pickle.load(f)

        with open(self.encoder_dir+ '/STATS_HH_ENC.bin', 'rb') as f:
            self.encoder_dict['STATS_HH'] = pickle.load(f)

        with open(self.encoder_dir+'/MINUTE_BINS_ENC.bin', 'rb') as f:
            self.encoder_dict['MINUTE_BINS'] = pickle.load(f)

        with open(self.encoder_dir+'/PLTFOM_TP_CODE_ENC.bin', 'rb') as f:
            self.encoder_dict['PLTFOM_TP_CODE'] = pickle.load(f)
        return True

    def save_refined_data_to_folder(self, refined_data=None, file_name = None, folder_name='ydy'):
        Refined_Data_dir = self.refined_data_dir+'/{0}'.format(folder_name)
        try :
            os.mkdir(Refined_Data_dir)
        except Exception as e:
            print("Error happend {0}".format(e))
        try :
            print(refined_data)
            file_name = Refined_Data_dir + '/' + file_name
            np.savez(file_name, x=refined_data[0], y=refined_data[1])
            return True
        except Exception as e :
            print("Save error happend : ", e)
            return False

    def Extract_log_per_hour(self, stats_dttm_hh, table_name = "CLICK_VIEW_YN_LOG_NEW", targeting = 'AD'):
        self.logger.log("Extract_log_per_hour{0}".format(stats_dttm_hh), "start")
        extract_sql = """
        select 
                LOG_DTTM,
                STATS_DTTM,
                STATS_HH,
                STATS_MINUTE,
                MEDIA_SCRIPT_NO,
                SITE_CODE, 
                ADVER_ID,
                extract(REMOTE_IP,'[0-9]+.[0-9]+.[0-9]+.[0-9]+') AS REMOTE_IP,
                ADVRTS_PRDT_CODE,
                ADVRTS_TP_CODE,
                PLTFOM_TP_CODE,
                PCODE,
                PNAME,
                BROWSER_CODE,
                FREQLOG,
                T_TIME,
                KWRD_SEQ,
                GENDER,
                AGE,
                OS_CODE,
                FRAME_COMBI_KEY,
                CLICK_YN
                FROM TEST.{0}
                where 1=1 
                and STATS_DTTM = {1}
                and STATS_HH = {2}
                and ADVRTS_TP_CODE = '{3}'
        """.format(table_name, str(stats_dttm_hh)[:-2],
                   str(stats_dttm_hh)[-2:], targeting)
        sql = text(extract_sql)
        data = pd.read_sql(sql, self.Local_Click_House_Conn)
        self.logger.log("Extract_log_per_hour{0}".format(stats_dttm_hh), "end")
        return data


    def preprocess_sample_data(self, stats_dttm_hh,
                               table_name='CLICK_VIEW_YN_LOG_NEW', targeting='AD', folder_name="ver_01"):
        self.logger.log("preprocess_sample_data_{0}".format(stats_dttm_hh),"start")
        self.logger.log("load encoder dict", self.load_encoder())

        # extract log data
        sample_data = self.Extract_log_per_hour(stats_dttm_hh, table_name, targeting=targeting)


        # preprocess start
        sample_data = sample_data[['STATS_DTTM', 'STATS_HH', 'STATS_MINUTE', 'MEDIA_SCRIPT_NO',
                                   'ADVRTS_PRDT_CODE', 'ADVRTS_TP_CODE', 'ADVER_ID',
                                   'PLTFOM_TP_CODE', 'BROWSER_CODE', 'OS_CODE', 'CLICK_YN']]
        sample_data = pd.merge(sample_data, self.Media_Property_Df, on=['MEDIA_SCRIPT_NO'])
        sample_data = pd.merge(sample_data,
                               self.Adver_Cate_Df[
                                   ['ADVER_ID', 'CTGR_SEQ_3', 'CTGR_SEQ_2', 'CTGR_SEQ_1']],
                               on=['ADVER_ID'])
        sample_data['STATS_DTTM'] = sample_data['STATS_DTTM'].astype('str')
        sample_data['WEEK'] = sample_data['STATS_DTTM'].apply(
            lambda x: datetime.strptime(x, '%Y%m%d').strftime('%a'))
        sample_data['MONTH_CATE'] = sample_data['STATS_DTTM'].apply(
            lambda x: 1 if int(x[-2:]) < 10 else (2 if int(x[-2:]) > 20 else 3))
        sample_data['MINUTE_BINS'] = \
            pd.cut(sample_data['STATS_MINUTE'], [0, 9, 19, 29, 39, 49, 59], labels=False, retbins=True, right=False)[0]
        sample_data['SCRIPT_TP_CODE'] = sample_data['SCRIPT_TP_CODE'].apply(lambda x: '03' if x == '14' else x)
        count_per_feature = sample_data[['MONTH_CATE', 'PLTFOM_TP_CODE', 'BROWSER_CODE', 'OS_CODE']].groupby(
            ['PLTFOM_TP_CODE', 'BROWSER_CODE', 'OS_CODE']).count().reset_index()
        count_per_feature['DELETE_YN'] = count_per_feature['MONTH_CATE'].apply(lambda x: 0 if x < 10 else 1)

        sample_data = pd.merge(sample_data,
                               count_per_feature[['PLTFOM_TP_CODE', 'BROWSER_CODE', 'OS_CODE', 'DELETE_YN']],
                               on=['PLTFOM_TP_CODE', 'BROWSER_CODE', 'OS_CODE'])

        sample_data = sample_data[sample_data['DELETE_YN'] == 1]
        sample_data['BROWSER_CODE'] = sample_data['BROWSER_CODE'].apply(lambda x: 'vacant' if x == '' else x)
        sample_data['OS_CODE'] = sample_data['OS_CODE'].apply(lambda x: 'vacant' if x == '' else x)
        sample_data['ADVER_MEDIA_HASH_KEY'] = sample_data['ADVER_ID'] + '_' + sample_data['MEDIASITE_NO']

        sample_data.dropna(inplace=True)
        sample_data[['WIDTH', 'HEIGHT']] = sample_data[['WIDTH', 'HEIGHT']] / 1000

        encode_column_list = ['MONTH_CATE', 'WEEK', 'STATS_HH', 'PLTFOM_TP_CODE']

        encoded_array_list = []
        for column in encode_column_list:
            print(column)
            encoded_array = self.encoder_dict[column].transform(sample_data[[column]]).toarray()
            encoded_array_list.append(encoded_array)

        encoded_array_list.append(self.hash_trick(sample_data[['MEDIA_CATE_INFO']], 20))
        encoded_array_list.append(self.hash_trick(sample_data[['SCRIPT_TP_CODE']], 25))
        encoded_array_list.append(self.hash_trick(sample_data[['ADVER_MEDIA_HASH_KEY']], 100))
        encoded_array_list.append(self.hash_trick(sample_data[['CTGR_SEQ_2']], 100))
        # encoded_array_list.append(hash_trick(input_data[['BROWSER_CODE']], 20))
        # encoded_array_list.append(hash_trick(input_data[['OS_CODE']], 15))
        encoded_array_list.append(sample_data[['WIDTH']].values)
        encoded_array_list.append(sample_data[['HEIGHT']].values)
        encoded_array_list.append(sample_data[['SIZE_RATIO']].values)

        input_data = np.hstack(encoded_array_list)

        output_data = sample_data[['CLICK_YN']].values

        save_to_folder_result = self.save_refined_data_to_folder( refined_data=(input_data, output_data),
                                                                  file_name=str(stats_dttm_hh), folder_name=folder_name)
        self.logger.log('save_to_folder_result',save_to_folder_result)
        return True


if __name__ == "__main__":
    # def __init__(self, maria_id, maria_password,
    #              Local_Clickhouse_Id, Local_Clickhouse_password, Local_Clickhouse_Ip,
    #              DB_NAME, TABLE_NAME):

    # test용 property data
    logger_name = "test"
    logger_file = "test.json"
    clickhouse_id = "analysis"
    clickhouse_password = "analysis@2020"
    maria_id = "dyyang"
    maria_password = "dyyang123!"
    local_clickhouse_id = "click_house_test1"
    local_clickhouse_password = "0000."
    local_clickhouse_ip = "192.168.100.237:8123"
    local_clickhouse_DB_name = "TEST"
    local_clickhouse_Table_name = 'CLICK_VIEW_YN_LOG'
    test_context = ETL_YDY_CONTEXT(local_clickhouse_id, local_clickhouse_password,
                                                local_clickhouse_ip, local_clickhouse_DB_name,
                                                local_clickhouse_Table_name)
    final_return = test_context.preprocess_sample_data('2020100101')
    # log_data = test_context.Extract_Sample_Log()
    # print(log_data)
    # print(os.getcwd()+'/Refined_data_folder')
    # print(test_context.Product_Property_Df)
    # print(test_context.Adver_Cate_Df)
    # print(test_context.Media_Property_Df)
    # print(test_context.Mobon_Com_Code_Df)