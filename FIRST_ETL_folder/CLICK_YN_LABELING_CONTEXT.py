# 설치가 필요한 파이썬 라이브러리 정보.
# pip install clickhouse-driver==0.1.3
# pip install clickhouse-sqlalchemy==0.1.4
# conda install sqlalchemy==1.3.16
# pip install ipython-sql==0.4.0

from sqlalchemy import create_engine, text
import numpy as np
import pandas as pd
import datetime
from datetime import timedelta
import time
import sys
import os

sys_path_list = sys.path
print(sys_path_list)
# ML_Train_Framework directory의 절대 경로를 환경 변수에 추가.
if sys_path_list.count(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))) == 0 :
    sys.path.append(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__)))))))

print(sys.path)

from ML_Framework_Applications.ETC.Logger import Logger
from clickhouse_driver import Client
import argparse

class CLICK_YN_LABELING_CONTEXT :

    def __init__( self, clickhouse_id, clickhouse_password, maria_id, maria_password, local_clickhouse_id,
                  local_clickhouse_password, local_clickhouse_db_name, logger_name = "test", logger_file="inner_logger.json",Maria_DB = False ):

        self.clickhouse_id = clickhouse_id
        self.clickhouse_password = clickhouse_password

        self.maria_id = maria_id
        self.maria_password = maria_password

        self.local_clickhouse_id = local_clickhouse_id
        self.local_clickhouse_password = local_clickhouse_password
        self.local_clickhouse_db_name = local_clickhouse_db_name
        self.logger = Logger(logger_name)

        self.Click_House_Engine = None
        self.Click_House_Conn = None

        self.MariaDB_Engine = None
        self.MariaDB_Engine_Conn = None

        self.Maria_DB = Maria_DB

        self.connect_db()

    def connect_db(self) :
        self.Click_House_Engine = create_engine('clickhouse://{0}:{1}@192.168.3.230:8123/'
                                                'MOBON_ANALYSIS'.format(self.clickhouse_id, self.clickhouse_password))
        self.Click_House_Conn = self.Click_House_Engine.connect()
        
        if self.Maria_DB : 
            self.MariaDB_Engine = create_engine('mysql+pymysql://{0}:{1}@192.168.100.108:3306/dreamsearch'
                                                .format(self.maria_id, self.maria_password))
            self.MariaDB_Engine_Conn = self.MariaDB_Engine.connect()

        self.Local_Click_House_Engine = create_engine(
            'clickhouse://{0}:{1}@localhost/{2}'.format(self.local_clickhouse_id, self.local_clickhouse_password,
                                                        self.local_clickhouse_db_name))
        self.Local_Click_House_Conn = self.Local_Click_House_Engine.connect()
        return True

    def create_local_table(self, table_name):
        client = Client(host='localhost')
        DDL_sql = """
        CREATE TABLE IF NOT EXISTS {0}.{1}
        (
            LOG_DTTM DateTime('Asia/Seoul'),
            STATS_DTTM  UInt32,
            STATS_HH  UInt8,
            STATS_MINUTE UInt8, 
            MEDIA_SCRIPT_NO String,
            SITE_CODE String,
            ADVER_ID String,
            REMOTE_IP String,
            ADVRTS_PRDT_CODE Nullable(String),
            ADVRTS_TP_CODE Nullable(String),
            PLTFOM_TP_CODE Nullable(String),
            PCODE Nullable(String),
            PNAME Nullable(String), 
            BROWSER_CODE Nullable(String),
            FREQLOG Nullable(String),
            T_TIME Nullable(String),
            KWRD_SEQ Nullable(String),
            GENDER Nullable(String),
            AGE Nullable(String),
            OS_CODE Nullable(String),
            FRAME_COMBI_KEY Nullable(String),
            CLICK_YN UInt8,
            BATCH_DTTM DateTime
        ) ENGINE = MergeTree
        PARTITION BY  ( STATS_DTTM, STATS_MINUTE)
        ORDER BY (STATS_DTTM, STATS_HH)
        SAMPLE BY ( STATS_DTTM, STATS_MINUTE )
        TTL BATCH_DTTM + INTERVAL 90 DAY
        SETTINGS index_granularity=8192
        """.format(self.local_clickhouse_db_name, table_name)
        result = client.execute(DDL_sql)
        return result

    def create_new_local_table(self, table_name):
        client = Client(host='localhost')
        DDL_sql = """
        CREATE TABLE IF NOT EXISTS {0}.{1}
        (
            LOG_DTTM DateTime('Asia/Seoul'),
            RANDOM_SAMPLE Float32,
            STATS_DTTM  UInt32,
            STATS_HH  UInt8,
            STATS_MINUTE UInt8, 
            MEDIA_SCRIPT_NO String,
            SITE_CODE String,
            ADVER_ID String,
            REMOTE_IP String,
            ADVRTS_PRDT_CODE Nullable(String),
            ADVRTS_TP_CODE Nullable(String),
            PLTFOM_TP_CODE Nullable(String),
            PCODE Nullable(String),
            BROWSER_CODE Nullable(String),
            FREQLOG Nullable(String),
            T_TIME Nullable(String),
            KWRD_SEQ Nullable(String),
            GENDER Nullable(String),
            AGE Nullable(String),
            OS_CODE Nullable(String),
            FRAME_COMBI_KEY Nullable(String),
            CLICK_YN UInt8,
            BATCH_DTTM DateTime
        ) ENGINE = MergeTree
        PARTITION BY  ( STATS_DTTM, STATS_MINUTE )
        PRIMARY KEY (STATS_DTTM, STATS_MINUTE)
        ORDER BY (STATS_DTTM, STATS_MINUTE)
        SAMPLE BY (  STATS_MINUTE )
        TTL BATCH_DTTM + INTERVAL 90 DAY
        SETTINGS index_granularity=8192
        """.format(self.local_clickhouse_db_name, table_name)
        result = client.execute(DDL_sql)
        return True

    def migrate_old_to_new_table(self, stats_dttm, old_table_name, new_table_name):
        self.connect_db()
        create_table_result = self.create_new_local_table(new_table_name)
        self.logger.log("create new table{0}".format(new_table_name), create_table_result)
        media_script_list_sql = """
        select 
        distinct MEDIA_SCRIPT_NO
        from 
        TEST.{0}
        where 1=1
        and STATS_DTTM= {1}
        """.format(old_table_name,str(stats_dttm))
        media_script_list_sql = text(media_script_list_sql)
        media_script_list = pd.read_sql(media_script_list_sql, self.Local_Click_House_Conn)['MEDIA_SCRIPT_NO']
        for MEDIA_SCRIPT_NO in media_script_list :
            log_data_sql = """
            select 
            *
            from TEST.{0}
            where 1=1
            and STATS_DTTM  = {1}
            and MEDIA_SCRIPT_NO = '{2}'
            """.format(old_table_name, str(stats_dttm), MEDIA_SCRIPT_NO)
            log_data_sql = text(log_data_sql)
            try :
                log_data_df = pd.read_sql(log_data_sql, self.Local_Click_House_Conn)
                log_data_df.to_sql(new_table_name, con=self.Local_Click_House_Engine, index=False, if_exists='append')
            except Exception as e :
                self.logger.log("Error happend",e)
                self.connect_db()
                log_data_df = pd.read_sql(log_data_sql, self.Local_Click_House_Conn)
                log_data_df.to_sql(new_table_name, con=self.Local_Click_House_Engine, index=False, if_exists='append')
        return True

    def create_entire_log_table(self, table_name):
        client = Client(host='localhost')
        DDL_sql = """
                CREATE TABLE IF NOT EXISTS {0}.{1}
                (
                    mediaId       String,
                    inventoryId   String,
                    frameId       String,
                    logType       String,
                    adType        String,
                    adProduct     String,
                    adCampain     String,
                    adverId       String,
                    productCode   String,
                    cpoint        Decimal(13, 2),
                    mpoint        Decimal(13, 2),
                    auid          String,
                    remoteIp      String,
                    platform      String,
                    device        String,
                    browser       String,
                    createdDate   DateTime default now(),
                    freqLog       Nullable(String),
                    tTime         Nullable(String),
                    kno           Nullable(String),
                    kwrdSeq       Nullable(String),
                    gender        Nullable(String),
                    age           Nullable(String),
                    osCode        Nullable(String),
                    price         Nullable(Decimal(13, 2)),
                    frameCombiKey Nullable(String)
                )  engine = MergeTree() 
                PARTITION BY toYYYYMMDD(createdDate)
                PRIMARY KEY (mediaId, inventoryId, adverId) 
                ORDER BY (mediaId, inventoryId, adverId) 
                SAMPLE BY mediaId 
                TTL createdDate + INTERVAL 90 DAY
                SETTINGS index_granularity = 8192
                """.format(self.local_clickhouse_db_name, table_name)
        result = client.execute(DDL_sql)
        return result

    def check_local_table_name(self, table_name):
        self.connect_db()
        check_table_name_sql = """
            SHOW TABLES FROM {0}
        """.format(self.local_clickhouse_db_name)
        sql_text = text(check_table_name_sql)
        sql_result = list(pd.read_sql(sql_text, self.Local_Click_House_Conn)['name'])
        print(sql_result)
        if table_name in sql_result:
            return True
        else:
            return False

    def Extract_Click_Stats_Date(self, stats_dttm_hh, hours=1):
        str_stats_dttm = str(stats_dttm_hh)
        stats_date = datetime.datetime(int(str_stats_dttm[:4]), int(str_stats_dttm[4:6]), int(str_stats_dttm[6:8]),
                                       int(str_stats_dttm[8:]))
        hour_delta = timedelta(hours=hours)
        previus_date = stats_date + hour_delta
        previus_date = previus_date.strftime('%Y%m%d%H')
        return int(stats_dttm_hh), int(previus_date)

    def Extract_Click_Df(self, stats_dttm_hh) :
        self.connect_db()
        try :
            Click_Date_List = [self.Extract_Click_Stats_Date(stats_dttm_hh,1)[0],self.Extract_Click_Stats_Date(stats_dttm_hh,1)[1]]
            Click_Data_Df_List = []
            for Click_Date_Key in Click_Date_List:
                Click_Df_sql = """
                select toTimeZone(createdDate, 'Asia/Seoul')            as KOREA_DATE,
                              inventoryId as MEDIA_SCRIPT_NO,
                              adCampain as SITE_CODE,
                              remoteIp as REMOTE_IP
                       from MOBON_ANALYSIS.MEDIA_CLICKVIEW_LOG
                       where 1 = 1
                         and inventoryId <> ''
                         and adCampain <> ''
                         and remoteIp <> ''
                         and logType = 'C'
                         and toYYYYMMDD(createdDate) = {0}
                         and toHour(createdDate) = {1}
                """.format(str(Click_Date_Key)[:-2], str(Click_Date_Key)[-2:])
                Click_Df_sql = text(Click_Df_sql)
                Click_Df = pd.read_sql_query(Click_Df_sql, self.Click_House_Conn)
                Click_Data_Df_List.append(Click_Df)
            self.Click_Df = pd.concat(Click_Data_Df_List)
            self.logger.log("Extract_click_Df_{0}".format(stats_dttm_hh),"success")
            return True
        except Exception as e :
            self.logger.log("Error happend",e)
            return False
    
    def Extract_Date_Range_From_DB(self) : 
        self.connect_db()
        try : 
            maria_db_sql = """
                select min(stats_dttm) as initial_date, max(stats_dttm) as last_date from BILLING.MOB_CAMP_MEDIA_HH_STATS
            """
            maria_db_sql = text(maria_db_sql)
            result = pd.read_sql(maria_db_sql,self.MariaDB_Engine_Conn)
            self.maria_initial_date = result['initial_date'].values[0]
            self.maria_last_date = result['last_date'].values[0]
        except Exception as e: 

            self.logger.log("Error happend",e)
            pass
        
        try :
            clickhouse_db_sql = """
                select
                min(toYYYYMMDD(createdDate)) as initial_date
                max(toYYYYMMDD(createdDate)) as last_date 
                from MOBON_ANALYSIS.MEDIA_CLICKVIEW_LOG
                where 1=1
                limit 10;
            """
            clickhouse_db_sql = text(clickhouse_db_sql)
            result = pd.read_sql(clickhouse_db_sql, self.Click_House_Conn)
            self.clickhouse_initial_date = result['initial_date'].values[0]
            self.clickhouse_last_date = result['last_date'].values[0]
        except Exception as e: 
            self.logger.log("Error happend",e)
            pass
        return True

    def Extract_Media_Script_List(self, stats_dttm_hh, min_click_cnt = 5) :
        self.connect_db()
        Ms_List_Sql = """
        SELECT
        tb.inventoryId as MEDIA_SCRIPT_NO, tb.cnt
        from
        (SELECT
        inventoryId, count(*) as cnt
            FROM MOBON_ANALYSIS.MEDIA_CLICKVIEW_LOG
               where 1 = 1
                 and inventoryId <> ''
                 and adCampain <> ''
                 and remoteIp <> ''
                 and logType = 'C'
                 and toYYYYMMDD(createdDate) = {0}
                 and toHour(createdDate) = {1}
        group by inventoryId
        order by  count() desc) as tb
        where tb.cnt >= {2}
        """.format(str(stats_dttm_hh)[:-2], str(stats_dttm_hh)[-2:], min_click_cnt)
        try :
            Ms_List_Sql = text(Ms_List_Sql)
            Ms_result = pd.read_sql_query(Ms_List_Sql, self.Click_House_Conn)
            self.Ms_List = Ms_result['MEDIA_SCRIPT_NO']
            self.logger.log("Extract_Media_Script_list function","Success")
            return True
        except Exception as e :
            self.logger.log("Extract_Media_Script_list function", e)
            return False

    def Extract_View_Df(self,
                        stats_dttm_hh,
                        table_name,
                        Maximum_Data_Size = 2000000,
                        Sample_Size = 500000) :
        
        Media_Script_No_Dict = self.Extract_Media_Script_List(stats_dttm_hh)
        i = 0
        if Media_Script_No_Dict == False :
            while i < 5 :
                i += 1
                Media_Script_No_Dict = self.Extract_Media_Script_List(stats_dttm_hh)
            if Media_Script_No_Dict == False:
                return "Extract_Media_Script_List Function error"
        Media_Script_cnt = self.Ms_List.shape[0]
        if Media_Script_cnt == 0 :
            return False
        print("Media_Script_Cnt shape  is ", Media_Script_cnt )
        i = 0
        Total_Data_Cnt = 0
        Merged_Df_List = []
        for MEDIA_SCRIPT_NO in self.Ms_List :
            i += 1
            # print("{0}/{1} start".format(i, Media_Script_List_Shape))
            View_Df_sql = """
            select 
                createdDate as LOG_DTTM,
                toYYYYMMDD(toTimeZone(createdDate, 'Asia/Seoul') )  as STATS_DTTM,
               toHour(toTimeZone(createdDate, 'Asia/Seoul') ) as STATS_HH,
               toMinute(toTimeZone(createdDate, 'Asia/Seoul') ) as STATS_MINUTE,
                      inventoryId as MEDIA_SCRIPT_NO,
                      adType                                           as ADVRTS_TP_CODE,
                      multiIf(
                              adProduct IN ('mba', 'nor', 'banner', 'mbw'), '01',
                              adProduct IN ('sky', 'mbb', 'sky_m'), '02',
                              adProduct IN ('ico', 'ico_m'), '03',
                              adProduct IN ('scn'), '04',
                              adProduct IN ('nct', 'mct'), '05',
                              adProduct IN ('pnt', 'mnt'), '07',
                              'null'
                          )                                            as ADVRTS_PRDT_CODE,
                      multiIf(
                              platform IN ('web', 'w', 'W'), '01',
                              platform IN ('mobile', 'm', 'M'), '02',
                              'null'
                          )                                            as PLTFOM_TP_CODE,
                      adCampain as SITE_CODE,
                      adverId as ADVER_ID,
                      productCode as PCODE,
                      remoteIp as REMOTE_IP,
                      visitParamExtractRaw(browser, 'code')            as BROWSER_CODE,
                      freqLog as FREQLOG,
                      tTime as T_TIME,
                      kwrdSeq as KWRD_SEQ,
                      gender as GENDER,
                      age as AGE,
                      osCode as OS_CODE,
                      frameCombiKey as FRAME_COMBI_KEY,
                    now() as BATCH_DTTM
               from MOBON_ANALYSIS.MEDIA_CLICKVIEW_LOG
               where 1 = 1
                 and inventoryId = '{0}'
                 and adCampain <> ''
                 and adType <> ''
                 and remoteIp <> ''
                 and logType = 'V'
                 and toYYYYMMDD(createdDate) = {1}
                 and toHour(createdDate) = {2}
            """.format(MEDIA_SCRIPT_NO, str(stats_dttm_hh)[:-2], str(stats_dttm_hh)[-2:])
            View_Df_sql = text(View_Df_sql)
            try:
                View_Df = pd.read_sql_query(View_Df_sql, self.Click_House_Conn)
                Click_View_Df = pd.merge(View_Df, self.Click_Df, on=['MEDIA_SCRIPT_NO', 'SITE_CODE', 'REMOTE_IP'],
                                         how='left')
                Merged_Df_List.append(Click_View_Df)
            except Exception as e:
                self.logger.log("Error happend",e)
                self.connect_db()
                View_Df = pd.read_sql_query(View_Df_sql, self.Click_House_Conn)
                Click_View_Df = pd.merge(View_Df, self.Click_Df, on=['MEDIA_SCRIPT_NO', 'SITE_CODE', 'REMOTE_IP'],
                                         how='left')
                Merged_Df_List.append(Click_View_Df)
            time.sleep(15)
            Total_Data_Cnt += Click_View_Df.shape[0]
            if Total_Data_Cnt >= Maximum_Data_Size :
                break
        try : 
            self.logger.log("extract log data {0}".format(stats_dttm_hh), "success")
            Concated_Df = pd.concat(Merged_Df_List)
            Concated_Df['CLICK_YN'] = Concated_Df['KOREA_DATE'].apply(lambda x: 0 if pd.isnull(x) else 1)
            if Concated_Df.shape[0] <= Sample_Size:
                final_df = Concated_Df.drop(columns=['KOREA_DATE'])
            else:
                final_df = Concated_Df.drop(columns=['KOREA_DATE']).sample(Sample_Size)
            random_array = np.random.rand(final_df.shape[0],)
            final_df['RANDOM_SAMPLE'] = random_array
            self.connect_db()
            self.logger.log("Add data to df","success")
            print(final_df.head())
            print(final_df.shape)
            final_df.to_sql(table_name,con = self.Local_Click_House_Engine, index=False, if_exists='append')
            self.logger.log("Insert Data to local db","success")
            return True
        except Exception as e :
            self.logger.log("something error happened",e)
            return False

    def Extract_All_Log_Data(self, stats_dttm, table_name):
        self.connect_db()
        log_sql = """
        SELECT * 
        FROM 
        MOBON_ANALYSIS.MEDIA_CLICKVIEW_LOG
        where 1 = 1
             and toYYYYMMDD(createdDate) = {0}
        """.format( str(stats_dttm))
        print(log_sql)
        log_sql = text(log_sql)
        try:
            View_Df = pd.read_sql_query(log_sql, self.Click_House_Conn)
            print(View_Df.shape)
            View_Df.to_sql(table_name, con=self.Local_Click_House_Engine, index=False, if_exists='append')
        except Exception as e:
            self.logger.log("Error happend",e)
            self.connect_db()
            View_Df = pd.read_sql_query(log_sql, self.Click_House_Conn)
            View_Df.to_sql(table_name, con=self.Local_Click_House_Engine, index=False, if_exists='append')
        self.logger.log("Extract view log to df", "success")
        return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--auto",help="(auto : Y, manual : N, migration : M, Test : T ) ", default='T')
    parser.add_argument("--create_table", help="--add_table ~~ ", default = None )
    args = parser.parse_args()

    # for service
    logger_name = input("logger name is : ")
    logger_file = input("logger file name is : ")

    clickhouse_id = input("click house id : ")
    clickhouse_password = input("clickhouse password : ")
    maria_id = input("maria id : ")
    maria_password = input("maria password : ")
    local_clickhouse_id = input("local clickhouse id : " )
    local_clickhouse_password = input("local clickhouse password : " )
    local_clickhouse_DB_name = input("local clickhouse DB name : " )
    local_table_name = input("local cllickhouse table name : " )

    data_cnt_per_hour = input("the number of data to extract per hour : " )
    sample_size = input("Sampling size : " )

    logger = Logger(logger_name, logger_file)
    logger.log("auto mode", args.auto.upper())

    # logger.log("extract start", 'Y')

    # clickhouse data extract context 생성
    click_house_context = CLICK_YN_LABELING_CONTEXT(clickhouse_id, clickhouse_password,
                                                     maria_id, maria_password,
                                                     local_clickhouse_id,local_clickhouse_password,
                                                     local_clickhouse_DB_name)

    logger.log("clickhouse context load", "success")
    if args.create_table == 'click_yn' :
        table_name = input("click_yn table name : " ) 
        click_house_context.create_new_local_table(table_name)
        logger.log("create clickhouse table {0} success".format(table_name), 'True')
    elif args.create_table == 'entire_log_yn' : 
        table_name = input("entire_log_yn table : " ) 
        click_house_context.create_entire_log_table(table_name)
        logger.log("create entire_log_yn table", "True")


    if args.auto.upper() == 'Y' :
        # automatic extracting logic start
        batch_date = datetime.datetime.now()
        click_house_context.Extract_Date_Range_From_DB()
        date_delta = timedelta(days=10)
        extract_date = batch_date - date_delta
        extract_date = extract_date.strftime('%Y%m%d')
        # automatic extracting logic end

    elif args.auto.upper() == 'N' :
        # manual extracting logic start
        start_dttm = input("extract start dttm is (ex) 20200801 ) : " )
        from_hh = input("start hour is (ex) 00 hour : 00 ) : ")
        last_dttm = input("extract last dttm is (ex) 20200827 ) : " )
        dt_list = pd.date_range(start=start_dttm, end=last_dttm).strftime("%Y%m%d").tolist()
        
        for stats_dttm in dt_list :        
            logger.log("manual_stats_dttm",stats_dttm)
            stats_dttm_list = [stats_dttm + '0{0}'.format(i) if i < 10 else stats_dttm + str(i) for i in range(int(from_hh), 24)]
            for Extract_Dttm in stats_dttm_list :
                extract_click_df_result = click_house_context.Extract_Click_Df(Extract_Dttm)
                extract_view_df_result = click_house_context.Extract_View_Df(Extract_Dttm, local_table_name, int(data_cnt_per_hour),
                                                                             int(sample_size))
                logger.log("Manual_extracting {0} result ".format(Extract_Dttm), extract_view_df_result)
            # manual extracting logic end

    if args.auto.upper() == 'M' :
        old_table_name = input("old log table name is :")
        new_table_name = input("new log table name is :")
        start_dttm = input("extract start dttm is (ex) 20200801 ) : ")
        last_dttm = input("extract last dttm is (ex) 20200827 ) : ")
        dt_list = pd.date_range(start=start_dttm, end=last_dttm).strftime("%Y%m%d").tolist()

        for stats_dttm in dt_list:
            logger.log("Migration stats_dttm", stats_dttm)
            migrate_log_df_result = click_house_context.migrate_old_to_new_table(stats_dttm, old_table_name, new_table_name)
            logger.log("Migration {0} result ".format(stats_dttm), migrate_log_df_result)

    elif args.auto.upper() == 'T' :
        return_value = click_house_context.Extract_Product_Property_Info()
        print(return_value)
    else :
        pass
