from sqlalchemy import create_engine, text
from clickhouse_driver import Client

import pandas as pd
from datetime import datetime
from dateutil.tz import tzlocal
from ETL_context_folder.Logger import Logger
import argparse


class PROPERTY_INFO_CONTEXT:
    def __init__(self, maria_id, maria_password, local_clickhouse_id,
                 local_clickhouse_password,
                 local_clickhouse_db_name):

        self.Maria_id = maria_id
        self.Maria_password = maria_password

        self.Clickhouse_id = local_clickhouse_id
        self.Clickhouse_password = local_clickhouse_password
        self.Clickhouse_DB = local_clickhouse_db_name

        local_tz = tzlocal()
        now_time = datetime.now(tz=local_tz).strftime("%Y%m%d%H%H")
        print(now_time)
        self.inner_logger = Logger(now_time, "shop_data_context_" + now_time + '.json')
        self.connect_db()

    def connect_db(self):
        self.MariaDB_Engine = create_engine('mysql+pymysql://{0}:{1}@192.168.100.108:3306/dreamsearch'
                                            .format(self.Maria_id, self.Maria_password))
        self.MariaDB_Engine_Conn = self.MariaDB_Engine.connect()

        self.MariaDB_Shop_Engine = create_engine('mysql+pymysql://{0}:{1}@192.168.100.106:3306/dreamsearch'
                                                 .format(self.Maria_id, self.Maria_password))
        self.MariaDB_Shop_Engine_Conn = self.MariaDB_Shop_Engine.connect()

        self.Local_Click_House_Engine = create_engine(
            'clickhouse://{0}:{1}@localhost/{2}'.format(self.Clickhouse_id, self.Clickhouse_password,
                                                        self.Clickhouse_DB))
        self.Local_Click_House_Conn = self.Local_Click_House_Engine.connect()

        return True

    def extract_product_cate_info(self):
        self.connect_db()
        product_cate_info_sql = """
        SELECT 
        apci.ADVER_ID,
        apci.PRODUCT_CODE as PCODE,
        apci.ADVER_CATE_NO as PRODUCT_CATE_NO,
        apsc.FIRST_CATE,
        apsc.SECOND_CATE,
        apsc.THIRD_CATE
        FROM dreamsearch.ADVER_PRDT_CATE_INFO as apci
        join
        (select * 
        from 
        dreamsearch.ADVER_PRDT_STANDARD_CATE) as apsc
        on apci.ADVER_CATE_NO = apsc.no;
        """
        text_sql = text(product_cate_info_sql)
        self.product_cate_info_df = pd.read_sql(text_sql, self.MariaDB_Shop_Engine_Conn)
        return True

    def return_adver_id_list(self):
        self.connect_db()
        adver_id_list_sql = """
        SELECT 
        distinct ADVER_ID
        FROM 
        dreamsearch.ADVER_PRDT_CATE_INFO;
        """
        sql_text = text(adver_id_list_sql)
        self.ADVER_ID_LIST = pd.read_sql(sql_text, self.MariaDB_Shop_Engine_Conn)['ADVER_ID']
        return True

    def create_shop_property_table(self, table_name):
        client = Client(host='localhost')
        DDL_sql = """
        CREATE TABLE IF NOT EXISTS TEST.{0}
        (
        STATS_DTTM String,
        ADVER_ID Nullable(String),
        PCODE Nullable(String),
        PRODUCT_CATE_NO Nullable(String),
        FIRST_CATE Nullable(String),
        SECOND_CATE Nullable(String),
        THIRD_CATE Nullable(String),
        PNM Nullable(String), 
        PRICE Nullable(UInt16)
        ) ENGINE = MergeTree
        PARTITION BY STATS_DTTM
        ORDER BY STATS_DTTM
        SETTINGS index_granularity=8192
        """.format(table_name)
        result = client.execute(DDL_sql)
        return result

    def update_shop_property_table(self, table_name):
        self.connect_db()
        self.extract_product_cate_info()
        self.return_adver_id_list()
        product_property_df_list = []
        size = self.ADVER_ID_LIST.shape[0]
        data_count = 0
        now_time = datetime.now(tz=tzlocal()).strftime("%Y%m%d%H%H")
        print("Start_time :", now_time)
        for i, ADVER_ID in enumerate(self.ADVER_ID_LIST):
            price_info_sql = """
             SELECT 
             USERID as ADVER_ID,
             PCODE,PNM,
             PRICE
             FROM dreamsearch.SHOP_DATA
             WHERE USERID = '{0}';
             """.format(ADVER_ID)
            sql_text = text(price_info_sql)
            try:
                product_price_info_df = pd.read_sql(sql_text, self.MariaDB_Shop_Engine_Conn)
                merged_df = pd.merge(self.product_cate_info_df, product_price_info_df, on=['ADVER_ID', 'PCODE'])
                product_property_df_list.append(merged_df)
            except:
                self.connect_db()
                product_price_info_df = pd.read_sql(sql_text, self.MariaDB_Shop_Engine_Conn)
                merged_df = pd.merge(self.product_cate_info_df, product_price_info_df, on=['ADVER_ID', 'PCODE'])
                product_property_df_list.append(merged_df)
            if i % 10 == 0:
                print("{0}/{1} : ".format(i, size), ADVER_ID)
        now_time = datetime.now(tz=tzlocal()).strftime("%Y%m%d%H%H")
        final_df = pd.concat(product_property_df_list)
        final_df['STATS_DTTM'] = datetime.now().strftime('%Y%m%d%H')
        print("End time : ", now_time)
        final_df.to_sql(table_name, con=self.Local_Click_House_Engine, index=False, if_exists='append')
        return True

    def create_adver_property_table(self, table_name):
        client = Client(host='localhost')
        DDL_sql = """
        CREATE TABLE IF NOT EXISTS TEST.{0}
        (
        STATS_DTTM String,
        ADVER_ID Nullable(String),
        CTGR_SEQ_3 Nullable(String),
        CTGR_NM_3 Nullable(String),
          CTGR_SEQ_2 Nullable(String),
        CTGR_NM_2 Nullable(String),
      CTGR_SEQ_1 Nullable(String),
        CTGR_NM_1 Nullable(String)
        ) ENGINE = MergeTree
        PARTITION BY STATS_DTTM
        ORDER BY STATS_DTTM
        SETTINGS index_granularity=8192
        """.format(table_name)
        result = client.execute(DDL_sql)
        return result

    def update_adver_property_table(self, table_name):
        self.connect_db()
        # try:
        Adver_Cate_Df_sql = """
                        select
                            MCUI.USER_ID as ADVER_ID, 
                            ctgr_info.CTGR_SEQ_3,
                            ctgr_info.CTGR_NM_3,
                            ctgr_info.CTGR_SEQ_2,
                            ctgr_info.CTGR_NM_2,
                            ctgr_info.CTGR_SEQ_1,
                            ctgr_info.CTGR_NM_1
                        from  dreamsearch.MOB_CTGR_USER_INFO as MCUI
                            join
                            (
                            SELECT 
                            third_depth.CTGR_SEQ as CTGR_SEQ_KEY, third_depth.CTGR_SEQ_NEW as CTGR_SEQ_3, third_depth.CTGR_NM as CTGR_NM_3,
                            second_depth.CTGR_SEQ_NEW as CTGR_SEQ_2, second_depth.CTGR_NM as CTGR_NM_2,
                            first_depth.CTGR_SEQ_NEW as CTGR_SEQ_1, first_depth.CTGR_NM as CTGR_NM_1
                            from dreamsearch.MOB_CTGR_INFO third_depth
                            join dreamsearch.MOB_CTGR_INFO second_depth
                            join dreamsearch.MOB_CTGR_INFO first_depth
                            on 1=1 
                            AND third_depth.CTGR_DEPT = 3
                            AND second_depth.CTGR_DEPT = 2
                            AND first_depth.CTGR_DEPT = 1
                            AND second_depth.USER_TP_CODE = '01'
                            AND second_depth.USER_TP_CODE = first_depth.USER_TP_CODE
                            AND third_depth.USER_TP_CODE = second_depth.USER_TP_CODE
                            AND second_depth.HIRNK_CTGR_SEQ = first_depth.CTGR_SEQ_NEW
                            AND third_depth.HIRNK_CTGR_SEQ = second_depth.CTGR_SEQ_NEW) as ctgr_info
                            on MCUI.CTGR_SEQ = ctgr_info.CTGR_SEQ_KEY;
                     """
        Adver_Cate_Df = pd.read_sql(Adver_Cate_Df_sql, self.MariaDB_Engine_Conn)
        Adver_Cate_Df = Adver_Cate_Df.drop_duplicates(subset='ADVER_ID')
        Adver_Cate_Df['STATS_DTTM'] = datetime.now().strftime('%Y%m%d%H')
        Adver_Cate_Df.to_sql(table_name, con=self.Local_Click_House_Engine, index=False, if_exists='append')
        return True
        # except:
        #    print("Extract_Adver_Cate_Info error happend")
        #    return False

    def create_media_property_table(self, table_name):
        client = Client(host='localhost')
        DDL_sql = """
            CREATE TABLE IF NOT EXISTS TEST.{0}
            (
            STATS_DTTM String,
            MEDIA_SCRIPT_NO Nullable(String),
            MEDIASITE_NO Nullable(String),
            MEDIA_ID Nullable(String),
            SCRIPT_TP_CODE Nullable(String),
            MEDIA_SIZE_CODE Nullable(String),
            ENDING_TYPE Nullable(String),
            M_BACON_YN Nullable(String),
            ADVRTS_STLE_TP_CODE Nullable(String),
            MEDIA_CATE_INFO Nullable(String),
            MEDIA_CATE_NAME Nullable(String)
            ) ENGINE = MergeTree
            PARTITION BY STATS_DTTM
            ORDER BY STATS_DTTM
            SETTINGS index_granularity=8192
            """.format(table_name)
        result = client.execute(DDL_sql)
        return result

    def update_media_property_table(self, table_name):
        self.connect_db()
        # try :
        PAR_PROPERTY_INFO_sql = """
        select 
            ms.no as MEDIA_SCRIPT_NO,
            MEDIASITE_NO,
            ms.userid as MEDIA_ID,
            mpi.SCRIPT_TP_CODE,
            mpi.MEDIA_SIZE_CODE,
            product_type as "ENDING_TYPE",
            m_bacon_yn as "M_BACON_YN",
            ADVRTS_STLE_TP_CODE as "ADVRTS_STLE_TP_CODE",
            media_cate_info.scate as "MEDIA_CATE_INFO",
            media_cate_info.ctgr_nm as "MEDIA_CATE_NAME"
            from dreamsearch.media_script as ms
            join
            (
            SELECT no, userid, scate, ctgr_nm
            FROM dreamsearch.media_site as ms
            join
            (SELECT mpci.CTGR_SEQ, CTGR_SORT_NO, mci.CTGR_NM
            FROM dreamsearch.MEDIA_PAR_CTGR_INFO as mpci
            join dreamsearch.MOB_CTGR_INFO as mci
            on mpci.CTGR_SEQ = mci.CTGR_SEQ_NEW) as media_ctgr_info
            on ms.scate = media_ctgr_info.CTGR_SORT_NO) as media_cate_info
            join
            (select PAR_SEQ, ADVRTS_PRDT_CODE,SCRIPT_TP_CODE, MEDIA_SIZE_CODE 
            from dreamsearch.MEDIA_PAR_INFO
            where PAR_EVLT_TP_CODE ='04') as mpi
            on ms.mediasite_no = media_cate_info.no
            and media_cate_info.scate = {0}
            and mpi.par_seq = ms.no;
        """
        result_list = []
        for i in range(1, 18):
            result = pd.read_sql(PAR_PROPERTY_INFO_sql.format(i), self.MariaDB_Engine_Conn)
            result_list.append(result)
        Media_Info_Df = pd.concat(result_list)
        Media_Info_Df['MEDIA_SCRIPT_NO'] = Media_Info_Df['MEDIA_SCRIPT_NO'].astype('str')
        Media_Info_Df['STATS_DTTM'] = datetime.now().strftime('%Y%m%d%H')
        Media_Info_Df.to_sql(table_name, con=self.Local_Click_House_Engine, index=False, if_exists='append')
        return True
        # except :
        #    return False

    ##### so many data.....
    def create_kwrd_property_table(self, table_name):
        client = Client(host='localhost')
        DDL_sql = """
            CREATE TABLE IF NOT EXISTS TEST.{0}
            (
            STATS_DTTM String,
            KWRD_SEQ Nullable(String),
            KWRD_NM Nullable(String)
            ) ENGINE = MergeTree
            PARTITION BY STATS_DTTM
            ORDER BY STATS_DTTM
            SETTINGS index_granularity=8192
            """.format(table_name)
        result = client.execute(DDL_sql)
        return result

    def update_kwrd_property_table(self, table_name):
        self.connect_db()
        kwrd_sql = """
        select
            CODE_TP_ID,
            CODE_ID,
            CODE_VAL,
            USE_YN,
            CODE_DESC
            FROM
        dreamsearch.MOBON_COM_CODE
        """
        kwrd_sql = text(kwrd_sql)
        kwrd_df = pd.read_sql(kwrd_sql, self.MariaDB_Engine_Conn)
        kwrd_df['STATS_DTTM'] = datetime.now().strftime('%Y%m%d%H')
        kwrd_df.to_sql('MOBON_COM_CODE', con=self.Local_Click_House_Engine, index=False, if_exists='append')
        return True

    ##### so skip!

    def create_mobon_com_code_table(self):
        client = Client(host='localhost')
        DDL_sql = """
        CREATE TABLE IF NOT EXISTS TEST.MOBON_COM_CODE
        (
        STATS_DTTM String,
        CODE_TP_ID Nullable(String),
        CODE_ID Nullable(String),
        CODE_VAL Nullable(String),
        USE_YN Nullable(String),
        CODE_DESC Nullable(String)
        ) ENGINE = MergeTree
        PARTITION BY STATS_DTTM
        ORDER BY STATS_DTTM
        SETTINGS index_granularity=8192
        """
        result = client.execute(DDL_sql)
        return result

    def update_mobon_com_code_table(self):
        self.connect_db()
        mobon_com_code_sql = """
        select
            CODE_TP_ID,
            CODE_ID,
            CODE_VAL,
            USE_YN,
            CODE_DESC
            FROM
        dreamsearch.MOBON_COM_CODE
        """
        mobon_com_code_sql = text(mobon_com_code_sql)
        mobon_com_code_df = pd.read_sql(mobon_com_code_sql, self.MariaDB_Engine_Conn)
        mobon_com_code_df['STATS_DTTM'] = datetime.now().strftime('%Y%m%d%H')
        mobon_com_code_df.to_sql('MOBON_COM_CODE', con=self.Local_Click_House_Engine, index=False, if_exists='append')
        return True

    def create_mob_camp_media_hh_stats(self):
        client = Client(host='localhost')
        DDL_sql = """
        CREATE TABLE IF NOT EXISTS TEST.MOB_CAMP_MEDIA_HH_STATS
        (
        STATS_DTTM Uint16,
        STATS_HH Uint16,
        PLTFOM_TP_CODE String,
        ADVRTS_PRDT_CODE String,
        ADVRTS_TP_CODE String,
        SITE_CODE String,
        MEDIA_SCRIPT_NO UInt16,
        ITL_TP_CODE String,
        ADVER_ID String, 
        TOT_EPRS_CNT UInt16,
        PAR_EPRS_CNT UInt16,
        CLICK_CNT UInt16
        ) ENGINE = MergeTree
        PARTITION BY ( STATS_DTTM, STATS_HH, ADVRTS_TP_CODE ) 
        ORDER BY STATS_DTTM
        SETTINGS index_granularity=8192
        """
        result = client.execute(DDL_sql)
        return result

    def update_mob_camp_media_hh_stats(self, initial_dttm, last_dttm, ADVRTS_TP_CODE='01'):
        self.connect_db()
        dt_index = pd.date_range(start=str(initial_dttm), end=str(last_dttm))
        stats_dttm_list = dt_index.strftime("%Y%m%d").tolist()
        stats_hh_list = ['0{0}'.format(i) if i < 10 else str(i) for i in range(0, 24)]

        for stats_dttm in stats_dttm_list:
            for stats_hh in stats_hh_list:
                sql = """
                SELECT
                STATS_DTTM,
                STATS_HH,
                PLTFOM_TP_CODE,
                ADVRTS_PRDT_CODE,
                ADVRTS_TP_CODE,
                SITE_CODE,
                MEDIA_SCRIPT_NO,
                ITL_TP_CODE,
                ADVER_ID,
                TOT_EPRS_CNT,
                PAR_EPRS_CNT,
                CLICK_CNT
                FROM BILLING.MOB_CAMP_MEDIA_HH_STATS
                WHERE STATS_DTTM = {0}
                and STATS_HH = '{1}'
                and ADVRTS_PRDT_CODE = '01'
                and ADVRTS_TP_CODE = '{2}'
                and ITL_TP_CODE = '01'
                and CLICK_CNT > 0;
                """.format(stats_dttm, stats_hh, ADVRTS_TP_CODE)
                sql = text(sql)
                stats_df = pd.read_sql(sql, self.MariaDB_Engine_Conn)
                stats_df.to_sql('MOB_CAMP_MEDIA_HH_STATS', con=self.Local_Click_House_Engine, index=False,
                                if_exists='append')
                print(stats_dttm, stats_hh, 'completed')
        return True

    def delete_old_data(self, table_name):
        self.connect_db()
        dttm_list_sql = """
        SELECT
        distinct STATS_DTTM
        FROM
        TEST.{0}
        """.format(table_name)
        dttm_list_sql = text(dttm_list_sql)
        dttm_list = list(pd.read_sql(dttm_list_sql, self.Local_Click_House_Engine)['STATS_DTTM'])
        int_dttm_list = [int(i) for i in dttm_list]
        max_dttm = max(int_dttm_list)
        if int_dttm_list.count(max_dttm) > 1:
            print('delete_old_data passed')
            return False
        else:
            client = Client(host='localhost')
            DDL_sql = """
            ALTER TABLE TEST.{0} DELETE WHERE STATS_DTTM <> '{1}'
                            """.format(table_name, max_dttm)
            result = client.execute(DDL_sql)
            print("delete old data")
            return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--create_table", help="--add_table ~~ ", default=None)
    args = parser.parse_args()

    # for develop
    # maria_id = "dyyang"
    # maria_password = "dyyang123!"

    # for service
    logger_name = input("logger name is : ")
    logger_file = input("logger file name is : ")
    maria_id = "analysis"
    maria_password = "analysis@2020"
    click_house_id = "click_house_test1"
    click_house_password = "0000"
    click_house_DB = "TEST"

    Property_Info_Context = PROPERTY_INFO_CONTEXT(maria_id, maria_password,
                                                  click_house_id, click_house_password, click_house_DB)

    logger = Logger(logger_name, logger_file)
    Shop_Property_Return = Property_Info_Context.create_shop_property_table('SHOP_PROPERTY_INFO')
    logger.log("Shop_Property_Return", Shop_Property_Return)
    Shop_property_update_return = Property_Info_Context.update_shop_property_table('SHOP_PROPERTY_INFO')
    logger.log("Shop_Property_Return", Shop_property_update_return)
    Delete_Old_data_return = Property_Info_Context.delete_old_data('SHOP_PROPERTY_INFO')
    logger.log('delete old data', Delete_Old_data_return)

    Adver_Property_Return = Property_Info_Context.create_adver_property_table('ADVER_PROPERTY_INFO')
    logger.log("Adver_Property_Return", Adver_Property_Return)
    Adver_Property_update_return = Property_Info_Context.update_adver_property_table('ADVER_PROPERTY_INFO')
    logger.log("Adver_Property_Return", Adver_Property_update_return)
    Delete_Old_data_return = Property_Info_Context.delete_old_data('ADVER_PROPERTY_INFO')
    logger.log('delete ADVER_PROPERTY_INFO return', Delete_Old_data_return)

    Media_Property_Return = Property_Info_Context.create_media_property_table('MEDIA_PROPERTY_INFO')
    logger.log("Media_Property_Return", Media_Property_Return)
    Media_Property_update_Return = Property_Info_Context.update_media_property_table('MEDIA_PROPERTY_INFO')
    logger.log("Media_Property_Return", Media_Property_update_Return)
    Delete_Old_data_return = Property_Info_Context.delete_old_data('MEDIA_PROPERTY_INFO')
    logger.log('delete MEDIA_PROPERTY_INFO data', Delete_Old_data_return)

    Mobon_Com_Code_return = Property_Info_Context.create_mobon_com_code_table()
    logger.log("create mobon com code table", "success")
    Update_Mobon_Com_Code_Return = Property_Info_Context.update_mobon_com_code_table()
    logger.log("update mobon com code table", "success")
    Delete_old_data_return = Property_Info_Context.delete_old_data('MOBON_COM_CODE')
    logger.log("detele mobon com code old data", "success")

    initial_dttm = input("MOB_CAMP_MEDIA_HH_STATS initial dttm ( ex) 20201010 ) :")
    last_dttm = input("MOB_CAMP_MEDIA_HH_STATS last dttm ( ex) 20201020 ) : ")
    Create_Camp_Media_STATS_Return = Property_Info_Context.create_mob_camp_media_hh_stats()
    logger.log("Create_Camp_Media_Return", Create_Camp_Media_STATS_Return)
    Update_Camp_Media_STATS_Return = Property_Info_Context.update_mob_camp_media_hh_stats(initial_dttm, last_dttm)
    logger.log("Update_Camp_Media_Return", Update_Camp_Media_STATS_Return)
