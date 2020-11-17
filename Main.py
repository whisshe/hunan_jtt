import time

import requests

from Public import Public
from bs4 import BeautifulSoup

p = Public()


def post_data(base_url):
    page_num = str(1)
    rows = str(10)
    post_data = {"page": page_num, "rows": rows}
    post_result = p.post_return(base_url, encoding='gbk', data=post_data)
    if post_result is not None:
        post_result = p.json2dict(post_result)
        total_rows = post_result['total']
        post_data['rows'] = total_rows
    post_result = p.json2dict(p.post_return(base_url, encoding='gbk', data=post_data))
    rows_data = post_result['rows']
    return rows_data


def get_traffic_data():
    base_url = 'http://218.76.40.86:8080/hnsjtt-lk/traffic/getTrafficData.do'
    post_result = p.post_return(base_url)
    if post_result is not None:
        post_result = p.json2dict(post_result)
        return post_result


def realtime_traffic2mysql(data):
    create_table_sql = """CREATE TABLE if not exists realtime_traffic(
    `id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'ID',
    `when_happen` datetime(0) NULL DEFAULT NULL COMMENT '发生日期 ',
    `highway_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '高速名称',
    `info` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '详细路况信息'
    ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '实时路况信息' ROW_FORMAT = Dynamic;
    """
    p.mysql_exec('172.18.69.191', 'hunan_jtt', 'hn_jtt', 'sin30=1/2', create_table_sql)
    sql_temp = "REPLACE INTO realtime_traffic " + "(id,when_happen,highway_name,info) VALUES (%s,%s,%s,%s)"
    print('开始插入数据到realtime_traffic表')
    p.save2mysql('172.18.69.191', 'hunan_jtt', 'hn_jtt', 'sin30=1/2', sql_temp, data)


def realtime_traffic():
    traffic_data = get_traffic_data()
    all_data = []
    if traffic_data is not None:
        for d in traffic_data:
            id = d['id']
            highway_name = d['highwayName']
            info = d['infoDescription']
            when_happen_str = info.split('，')[0].strip()
            try:
                if '曰' in when_happen_str:
                    continue
                if '分' in when_happen_str:
                    when_happen_strptime = time.strptime(when_happen_str, '%Y年%m月%d日%H时%M分')
                    when_happen_strftime = time.strftime('%Y-%m-%d %H:%M', when_happen_strptime)
                else:
                    when_happen_strptime = time.strptime(when_happen_str, '%Y年%m月%d日%H时')
                    when_happen_strftime = time.strftime('%Y-%m-%d %H:00', when_happen_strptime)
                single_data = [id, when_happen_strftime, highway_name, info]
                all_data.append(single_data)
            except ValueError as error:
                print(error)
        all_data = tuple(all_data)
        print(len(all_data))
        realtime_traffic2mysql(all_data)


def get_data_list(base_url):
    page_size = 20000
    page_num = 1
    url = base_url + '&pnum={}'.format(page_num) + '&psize={}'.format('')
    html = p.get_html_source(url)
    html_dict = p.json2dict(html)
    data_count = html_dict['dataCount']
    if int(data_count) <= 20000:
        url = base_url + '&pnum={}'.format(page_num) + '&psize={}'.format(data_count)
        json_data = p.get_html_source(url)
        data_list = p.json2dict(json_data)['dataList']
    else:
        page_total = data_count // 20000 + 1
        data_list = []
        while page_num <= page_total:
            url = base_url + '&pnum={}'.format(page_num) + '&psize={}'.format(page_size)
            json_data = p.get_html_source(url)
            data_list_temporary = p.json2dict(json_data)['dataList']
            data_list = data_list + data_list_temporary
            page_num += 1
    return data_list


def ship_transport():
    base_url = 'http://218.76.40.86/hjs/getList.do?id=713ee215-d5e1-473d-a3b2-a90e84b29e70&kwd=&'
    data_list = get_data_list(base_url)
    all_data = []
    for data in data_list:
        single_data = [data['ID'], data['QX'], int(data['DW']), int(data['KW']), data['YYZH'], data['ZJNS'], data['CZ'],
                       data['QM']]
        all_data.append(tuple(single_data))
    print('开始插入船舶运输数据')
    sql_temp = "REPLACE INTO ship_transport(id,ship_type,tonnage,guests_seats,business_license_no,latest_annual_review,ship_owner,ship_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
    all_data = tuple(all_data)
    p.save2mysql('172.18.69.191', 'hunan_jtt', 'hn_jtt', 'sin30=1/2', sql_temp, all_data)
    return all_data


def passenger_vehicles():
    base_url = 'http://218.76.40.86/hjs/getList.do?id=644bc604-9be8-438b-9202-775ea9214937&kwd=&'
    data_list = get_data_list(base_url)
    all_data = []
    for data in data_list:
        single_data = [data['ID'], data['DQ'], data['YHMC'], data['YXRQ'], data['PH']]
        all_data.append(tuple(single_data))
    sql_temp = "REPLACE INTO passenger_vehicles(id,region,owner,valid_data_transport,car_number) VALUES (%s,%s,%s,%s,%s)"
    all_data = tuple(all_data)
    print('开始插入汽车客运数据')
    p.save2mysql('172.18.69.191', 'hunan_jtt', 'hn_jtt', 'sin30=1/2', sql_temp, all_data)
    return all_data


def learner_driven_vehicle():
    base_url = 'http://218.76.40.86/hjs/getList.do?id=fbbec482-1a79-4901-bf36-12c0cfd99010&kwd=&'
    data_list = get_data_list(base_url)
    all_data = []
    for data in data_list:
        single_data = [data['PH'], data['DQ'], data['YHMC']]
        all_data.append(tuple(single_data))
    sql_temp = "REPLACE INTO learner_driven_vehicle(serial_num, region, owner) VALUES (%s,%s,%s)"
    all_data = tuple(all_data)
    print('开始插入教练车数据')
    p.save2mysql('172.18.69.191', 'hunan_jtt', 'hn_jtt', 'sin30=1/2', sql_temp, all_data)
    return all_data


def waterway_owner():
    base_url = 'http://218.76.40.86/hjs/getList.do?id=aa3e8760-bba3-45b9-b19a-944bc7f25180&kwd=&'
    data_list = get_data_list(base_url)
    all_data = []
    for data in data_list:
        single_data = [data['QYMC'], data['FRDB'], data['XKZH'], data['DZ'], data['YHLX'], data['ID']]
        all_data.append(tuple(single_data))
    sql_temp = "REPLACE INTO waterway_owner(corporate_name, Legal_representative, license_key,address,type,id) VALUES (%s,%s,%s,%s,%s,%s)"
    all_data = tuple(all_data)
    print('开始插入水路业户数据')
    p.save2mysql('172.18.69.191', 'hunan_jtt', 'hn_jtt', 'sin30=1/2', sql_temp, all_data)
    return all_data


def road_transport_owner():
    base_url = 'http://218.76.40.86/hjs/getList.do?id=4648112c-0cbc-4a9e-aab4-34c124d273cf&kwd=&'
    data_list = get_data_list(base_url)
    all_data = []
    for data in data_list:
        single_data = [data['JYFW'], data['MC'], data['XKZYXQ'], data['ID']]
        all_data.append(tuple(single_data))
    sql_temp = "REPLACE INTO road_transport_owner(business_scope,corporate_name,license_validity,id) VALUES (%s,%s,%s,%s)"
    all_data = tuple(all_data)
    print('开始插入道路运输业户数据')
    p.save2mysql('172.18.69.191', 'hunan_jtt', 'hn_jtt', 'sin30=1/2', sql_temp, all_data)
    return all_data


def vehicle_repair():
    base_url = 'http://218.76.40.86/hjs/getList.do?id=8e1a34e5-5c22-4503-ac36-dcf2cc73c49f&kwd='
    data_list = get_data_list(base_url)
    all_data = []
    for data in data_list:
        single_data = [data['ID'], data['JYFW'], data['MC'], data['WXLB'], data['WXLX'], data['XKZYXQ']]
        all_data.append(tuple(single_data))
    sql_temp = "REPLACE INTO vehicle_repair(id,business_scope,corporate_name,category,type,license_validity) VALUES (%s,%s,%s,%s,%s,%s)"
    all_data = tuple(all_data)
    print('开始插入汽车维护业户数据')
    p.save2mysql('172.18.69.191', 'hunan_jtt', 'hn_jtt', 'sin30=1/2', sql_temp, all_data)
    return all_data


def driver_school():
    base_url = 'http://218.76.40.86/hjs/getList.do?id=bfd031b5-16d8-4c87-9c40-56c1439a8577&kwd='
    data_list = get_data_list(base_url)
    all_data = []
    for data in data_list:
        single_data = [data['ID'], data['JB'], data['LX'], data['MC'], data['XKZYXQ']]
        all_data.append(tuple(single_data))
    sql_temp = "REPLACE INTO driver_school(id,level,type,corporate_name,license_validity) VALUES (%s,%s,%s,%s,%s)"
    all_data = tuple(all_data)
    print('开始插入驾培业户数据')
    p.save2mysql('172.18.69.191', 'hunan_jtt', 'hn_jtt', 'sin30=1/2', sql_temp, all_data)
    return all_data


def station_transportation():
    base_url = 'http://218.76.40.86/hjs/getList.do?id=98fd795c-d326-4cff-ac2c-07795d11af90'
    data_list = get_data_list(base_url)
    all_data = []
    for data in data_list:
        single_data = [data['ID'], data['DJ'], data['JYFW'], data['LX'], data['MC'], data['XKZYXQ']]
        all_data.append(tuple(single_data))
    sql_temp = "REPLACE INTO station_transportation(id,level,business_scope,type,corporate_name,license_validity) VALUES (%s,%s,%s,%s,%s,%s)"
    all_data = tuple(all_data)
    print('开始插入站场及运输服务业户数据')
    p.save2mysql('172.18.69.191', 'hunan_jtt', 'hn_jtt', 'sin30=1/2', sql_temp, all_data)
    return all_data


def supervision_qualification():
    base_url = 'http://218.76.40.86/hjs/getList.do?id=1a6f8a46-64e9-4157-87f9-ebacc290fa49'
    data_list = get_data_list(base_url)
    all_data = []
    for data in data_list:
        single_data = [data['QYLX'], data['QYMC'], data['YYZZ'], data['YXQ'], data['ZCSJ']]
        all_data.append(tuple(single_data))
    sql_temp = "REPLACE INTO supervision_qualification(corporate_type,corporate_name,business_license,license_validity,registration_time) VALUES (%s,%s,%s,%s,%s)"
    all_data = tuple(all_data)
    print('开始插入监理资质数据')
    p.save2mysql('172.18.69.191', 'hunan_jtt', 'hn_jtt', 'sin30=1/2', sql_temp, all_data)
    return all_data


def toll_station():
    base_url = 'http://218.76.40.86/hjs/getList.do?id=36cf0170-3056-4dc7-8e18-b0544ab8f5e7'
    data_list = get_data_list(base_url)
    all_data = []
    for data in data_list:
        single_data = [data['ID'], data['LD'], data['MC'], data['ZD_MC'], data['ZH']]
        all_data.append(tuple(single_data))
    sql_temp = "REPLACE INTO toll_station(id,road_section,highway_name,station_name,stake_no) VALUES (%s,%s,%s,%s,%s)"
    all_data = tuple(all_data)
    print('开始插入高速收费站数据')
    p.save2mysql('172.18.69.191', 'hunan_jtt', 'hn_jtt', 'sin30=1/2', sql_temp, all_data)
    return all_data


def rest_area():
    base_url = 'http://218.76.40.86/hjs/getList.do?id=1b20089c-85f2-4c4e-940d-537fd7e68730'
    data_list = get_data_list(base_url)
    all_data = []
    for data in data_list:
        single_data = [data['ID'], data['LD'], data['MC'], data['SYZ'], data['XYZ'], data['ZH']]
        all_data.append(tuple(single_data))
    sql_temp = "REPLACE INTO rest_area(id,road_section,rest_area_name,last_station,next_station,stake_no) VALUES (%s,%s,%s,%s,%s,%s)"
    all_data = tuple(all_data)
    print('开始插入高速服务区数据')
    p.save2mysql('172.18.69.191', 'hunan_jtt', 'hn_jtt', 'sin30=1/2', sql_temp, all_data)
    return all_data


def highway_gas_station():
    base_url = 'http://218.76.40.86/hjs/getList.do?id=4525949a-30ca-44be-ab00-c48bb6ee8851'
    data_list = get_data_list(base_url)
    all_data = []
    for data in data_list:
        single_data = [data['ID'], data['LD'], data['MC'], data['FWQ'], data['ZH']]
        all_data.append(tuple(single_data))
    sql_temp = "REPLACE INTO highway_gas_station(id,road_section,gas_station_name,rest_area_name,stake_no) VALUES (%s,%s,%s,%s,%s)"
    all_data = tuple(all_data)
    print('开始插入高速加油站数据')
    p.save2mysql('172.18.69.191', 'hunan_jtt', 'hn_jtt', 'sin30=1/2', sql_temp, all_data)
    return all_data


def highway_car_repair():
    base_url = 'http://218.76.40.86/hjs/getList.do?id=ead39de2-6a7c-49ff-b826-53b40a059772'
    data_list = get_data_list(base_url)
    all_data = []
    for data in data_list:
        single_data = [data['ID'], data['LD'], data['MC'], data['FWQ'], data['ZH']]
        all_data.append(tuple(single_data))
    sql_temp = "REPLACE INTO highway_car_repair(id,road_section,car_repair_name,rest_area_name,stake_no) VALUES (%s,%s,%s,%s,%s)"
    all_data = tuple(all_data)
    print('开始插入高速汽修点数据')
    p.save2mysql('172.18.69.191', 'hunan_jtt', 'hn_jtt', 'sin30=1/2', sql_temp, all_data)
    return all_data


def maintenance():
    base_url = 'http://218.76.40.86/hjs/getList.do?id=c9ffc54c-ce5c-4cd0-9039-db2d2cfbd50b'
    data_list = get_data_list(base_url)
    all_data = []
    for data in data_list:
        single_data = [data['ID'], data['LD'], data['MC'], data['FWQ'], data['ZH']]
        all_data.append(tuple(single_data))
    sql_temp = "REPLACE INTO maintenance(id,road_section,car_repair_name,rest_area_name,stake_no) VALUES (%s,%s,%s,%s,%s)"
    all_data = tuple(all_data)
    print('开始插入维修检测数据')
    p.save2mysql('172.18.69.191', 'hunan_jtt', 'hn_jtt', 'sin30=1/2', sql_temp, all_data)
    return all_data


def highway_help():
    base_url = 'http://218.76.40.86/hjs/getList.do?id=b8e3e064-d9bf-4630-9637-a03f1c87bcd7'
    data_list = get_data_list(base_url)
    all_data = []
    for data in data_list:
        single_data = [data['ID'], data['DZ'], data['FWZMC'], data['MC'], data['ZZH']]
        all_data.append(tuple(single_data))
    sql_temp = "REPLACE INTO highway_help(id,detail_address,highway_help_name,road_section,stake_no) VALUES (%s,%s,%s,%s,%s)"
    all_data = tuple(all_data)
    print('开始插入高速救援站数据')
    p.save2mysql('172.18.69.191', 'hunan_jtt', 'hn_jtt', 'sin30=1/2', sql_temp, all_data)
    return all_data


def overspeed_government_station():
    base_url = 'http://218.76.40.86/hjs/getList.do?id=7805a3ea-26f4-4e68-9918-8bbbbe964081'
    data_list = get_data_list(base_url)
    all_data = []
    for data in data_list:
        single_data = [data['ID'], data['DZ'], data['LB'], data['LXWZ'], data['MC']]
        all_data.append(tuple(single_data))
    sql_temp = "REPLACE INTO overspeed_government_station(id,location_detail,type,road_section,station_name) VALUES (%s,%s,%s,%s,%s)"
    all_data = tuple(all_data)
    print('开始插入高速治超站点数据')
    p.save2mysql('172.18.69.191', 'hunan_jtt', 'hn_jtt', 'sin30=1/2', sql_temp, all_data)
    return all_data


def driving_school_info():
    base_url = 'http://218.76.40.86/hjs/getList.do?id=bfd031b5-16d8-4c87-9c40-56c1439a8577'
    data_list = get_data_list(base_url)
    all_data = []
    for data in data_list:
        single_data = [data['ID'], data['JB'], data['LX'], data['XKZYXQ'], data['MC']]
        all_data.append(tuple(single_data))
    sql_temp = "REPLACE INTO driving_school_info(id,level,type,school_name,license_validity) VALUES (%s,%s,%s,%s,%s)"
    all_data = tuple(all_data)
    print('开始插入驾培点数据')
    p.save2mysql('172.18.69.191', 'hunan_jtt', 'hn_jtt', 'sin30=1/2', sql_temp, all_data)
    return all_data


def employees_common(rows_data):
    all_data = []
    for data in rows_data:
        single_data = [data['NAME'], data['SEX'], data['CARDNO'], data['CREDATE'], data['GRADE'], data['START_DATE'],
                       data['END_DATE'], data['OWNER_NAME']]
        all_data.append(tuple(single_data))
    return tuple(all_data)


def company_common(rows_data):
    all_data = []
    for data in rows_data:
        single_data = [data['OWNER_NAME'], data['LICENSE_NUMBER'], data['RPTCHKYEAR'], data['RPTCHKSCORE'],
                       data['CREDIT_LEVEL']]
        all_data.append(tuple(single_data))
    return tuple(all_data)


def company_sql_common(table_name, table_comment, data):
    create_table_sql = """CREATE TABLE if not exists {table_name}(
  `company_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '企业名称 ',
  `license_number` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '许可证字号',
  `rptchk_year` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '	考核年度',
  `rptchk_score` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '考核分数',
  `credit_level` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '考核等级'
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '{table_comment}' ROW_FORMAT = Dynamic;
    """.format(table_name=table_name, table_comment=table_comment)
    p.mysql_exec('172.18.69.191', 'hunan_jtt', 'hn_jtt', 'sin30=1/2', create_table_sql)
    sql_temp = "REPLACE INTO " + table_name + "(company_name,license_number,rptchk_year,rptchk_score,credit_level) VALUES (%s,%s,%s,%s,%s)"
    print('开始插入数据到{}表'.format(table_name))
    p.save2mysql('172.18.69.191', 'hunan_jtt', 'hn_jtt', 'sin30=1/2', sql_temp, data)


def employees_sql_common(table_name, table_comment, data):
    create_table_sql = """CREATE TABLE if not exists {table_name}(
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '名字',
  `sex` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '性别',
  `cardno` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '身份证号码',
  `credate` datetime(0) NULL DEFAULT NULL COMMENT '签注日期 ',
  `grade` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '考核等级',
  `start_date` datetime(0) NULL DEFAULT NULL COMMENT '考核周期起始日期 ',
  `end_date` datetime(0) NULL DEFAULT NULL COMMENT '考核周期截止日期 ',
  `owner_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '服务单位'
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '{table_comment}' ROW_FORMAT = Dynamic;
    """.format(table_name=table_name, table_comment=table_comment)
    p.mysql_exec('172.18.69.191', 'hunan_jtt', 'hn_jtt', 'sin30=1/2', create_table_sql)
    sql_temp = "REPLACE INTO " + table_name + "(name,sex,cardno,credate,grade,start_date,end_date,owner_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
    print('开始插入数据到{}表'.format(table_name))
    p.save2mysql('172.18.69.191', 'hunan_jtt', 'hn_jtt', 'sin30=1/2', sql_temp, data)


def employees_red_list():
    base_url = 'http://218.76.40.69:8521/hngzd/public/publicCmpsn/publicCmpsnRedSList/cmpsnRedSList.jsp?LEVEL=3'
    rows_data = post_data(base_url)
    all_data = employees_common(rows_data)
    employees_sql_common('employees_red_list', '从业人员信用红名单', all_data)


def employees_black_list():
    base_url = 'http://218.76.40.69:8521/hngzd/public/publicCmpsn/publicCmpsnBlackSList/cmpsnBlackSList.jsp?LEVEL=0'
    rows_data = post_data(base_url)
    all_data = employees_common(rows_data)
    employees_sql_common('employees_black_list', '从业人员信用黑名单', all_data)


def employees_integrity_assessment():
    base_url = 'http://218.76.40.69:8521/hngzd/public/publicCmpsn/publicCmpsnCreditList/cmpsnCreditList.jsp'
    rows_data = post_data(base_url)
    all_data = employees_common(rows_data)
    employees_sql_common('employees_integrity_assessment', '从业人员诚信考核', all_data)


def company_red_list():
    base_url = 'http://218.76.40.69:8521/hngzd/public/publicCmclt/publicCmcltRedList/cmcltRedSList.jsp?LEVEL=3'
    rows_data = post_data(base_url)
    all_data = company_common(rows_data)
    company_sql_common('company_red_list', '企业信用红名单', all_data)


def company_black_list():
    base_url = 'http://218.76.40.69:8521/hngzd/public/publicCmclt/publicCmcltBlackSList/cmcltBlackSList.jsp?LEVEL=0'
    rows_data = post_data(base_url)
    all_data = company_common(rows_data)
    company_sql_common('company_black_list', '企业信用黑名单', all_data)


def company_integrity_assessment():
    base_url = 'http://218.76.40.69:8521/hngzd/public/publicCmclt/publicCmcltCreditList/cmcltCreditList.jsp'
    rows_data = post_data(base_url)
    all_data = company_common(rows_data)
    company_sql_common('company_integrity_assessment', '企业诚信考核', all_data)


def run():
    ship_transport_data = ship_transport()
    passenger_vehicles()
    learner_driven_vehicle()
    waterway_owner()
    road_transport_owner()
    vehicle_repair()
    driver_school()
    station_transportation()
    supervision_qualification()
    toll_station()
    rest_area()
    # highway_gas_station()
    # highway_car_repair()
    highway_help()
    overspeed_government_station()
    driving_school_info()
    # maintenance()


if __name__ == '__main__':
    # run()
    # post_data('http://218.76.40.69:8521/hngzd/public/publicCmpsn/publicCmpsnRedSList/cmpsnRedSList.jsp')
    # employees_red_list()
    # employees_black_list()
    # employees_integrity_assessment()
    # company_red_list()
    # company_black_list()
    # company_integrity_assessment()
    realtime_traffic()
