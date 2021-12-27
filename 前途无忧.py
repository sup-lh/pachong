from bs4 import BeautifulSoup
import pymysql
import re
import time
import requests
import json
from urllib import parse

# 数据库操作
class MysqlDb():

    def open_db(self):
        # 建立连接
        self.conn = pymysql.connect(
            host='127.0.0.1',
            port=3306,
            user='root',
            passwd='root',
            db='web',
            charset='utf8',
            use_unicode=True
            )  # 有中文要存入数据库的话要加charset='utf8'和use_unicode=True
        # 创建游标
        self.cursor = self.conn.cursor()
    
    def create_table(self):
        try:
            self.open_db()
            table_sql = """
            create table if not exists qiantuwuyou_data(
                id int not null auto_increment,
                职位 varchar(500),
                工资 varchar(500),
                公司名 varchar(500),
                公司详情地址 varchar(500),
                工作地区 varchar(500),
                经验要求 varchar(500),
                学历要求 varchar(500),
                招聘人数 varchar(500),
                发布日期 varchar(500),
                福利 varchar(500),
                岗位信息 MEDIUMTEXT,
                上班地址 varchar(500),
                公司类型 varchar(500),
                公司人数 varchar(500),
                公司业务方向 varchar(500),
                primary key (id)
                )
                """
            self.cursor.execute(table_sql)
            self.conn.commit()
            print('创建数据表成功')
        except Exception as e:
            self.conn.rollback()
            print(e)
        finally:
            self.close_db()

    def insert_table(self,list_data):

        try:
            self.open_db()
            # sql语句
            insert_sql = """
            insert into qiantuwuyou_data(职位,工资,公司名,公司详情地址,工作地区,经验要求,学历要求,招聘人数,发布日期,福利,岗位信息,上班地址,公司类型,公司人数,公司业务方向)
             VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
            """
            # 执行插入数据到数据库操作
            self.cursor.execute(insert_sql,list_data)
            # 提交，不进行提交无法保存到数据库
            self.conn.commit()
            print('提交成功')

        except Exception as e:
            self.conn.rollback()
            print(e)
        finally:
            self.close_db()


    def close_db(self):
        # 关闭游标和连接
        self.cursor.close()
        self.conn.close()


# 网页操作
class WebDo():

    global theme
    global headers
    global Cookie

    # 希望爬取的主题
    theme = '爬虫'

    # 填入实时cookie
    Cookie = ''

    headers = {
            'Cookie': Cookie,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
        }
    def get_url(self):
        
        url = 'https://search.51job.com/list/030200,000000,0000,00,9,99,{},2,1.html?lang=c&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&ord_field=0&dibiaoid=0&line=&welfare='.format(
            format(parse.quote(theme)))

        resp = requests.get(url=url, headers=headers)
        
        ra = re.compile('job_href":"(.*?)",', re.I | re.S)
        rersult_ = ra.findall(resp.text)

        for i in rersult_:
            url = i.replace('\\', '')
            try:
                print(self.get_target(url))
                MysqlDb().insert_table(self.get_target(url))
            except AttributeError:
                print('身份过期')
                break
            time.sleep(1)



    def get_target(self,url):

        print('\n','请求网址: ',url,'\n')

        resp = requests.get(url=url, headers=headers)
        resp.encoding = 'gbk' #处理编码
        soup = BeautifulSoup(resp.text,'lxml')

        #职位大概信息
        title_data = soup.select_one('.tHjob .cn')

        #岗位信息
        content = ''
        for j in soup.select('.tBorderTop_box .job_msg p')[:-3]:
            content+=(j.text)
    
        # 竖线隔开的
        spls = title_data.select_one('p.ltype').text.replace('\xa0','').replace('\xa1','').split('|')
        try:
            sent_time = spls[4]
        except:
            sent_time = '未知'

        #福利
        jtag = []
        for i in title_data.select('.sp4'):
            jtag.append(i.text)
        
        #公司大概信息
        c_tags = soup.select('.com_tag p')
        

        dic_c_data = {
            '职位':title_data.select_one('h1')['title'],
            '工资':title_data.select_one('strong').text,
            '公司名':title_data.select_one('.cname a')['title'],
            '公司详情地址':title_data.select_one('.cname a')['href'],
            '工作地区':spls[0],
            '经验要求':spls[1],
            '学历要求':spls[2],
            '招聘人数':spls[3],
            '发布日期':sent_time,
            '福利':jtag,
            '岗位信息':content,
            '上班地址':soup.select('.fp')[-1].text.split('：')[1],
            '公司类型':c_tags[0].text,
            '公司人数':c_tags[1].text,
            '公司业务方向':c_tags[2].text.replace('\n',''),
        }
        with open('dic_data.json','a+',encoding='utf-8')as f:
            f.write(json.dumps(dic_c_data,ensure_ascii=False))
        return [
            title_data.select_one('h1')['title'],
            title_data.select_one('strong').text,
            title_data.select_one('.cname a')['title'],
            title_data.select_one('.cname a')['href'],
            spls[0],
            spls[1],
            spls[2],
            spls[3],
            sent_time,
            str(jtag),
            content,
            soup.select('.fp')[-1].text.split('：')[1],
            c_tags[0].text,
            c_tags[1].text,
            c_tags[2].text.replace('\n',''),
        ]


#建表 (在这之前先建库,库名叫 web )   create database web;
MysqlDb().create_table()


#爬取并更新数据
WebDo().get_url()