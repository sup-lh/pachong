from selenium import webdriver
from loguru import logger
import time
import json
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, WebDriverException
from lxml import etree
from retrying import retry
from selenium.webdriver import ActionChains
import pymysql

import pyautogui
pyautogui.PAUSE = 0.5


# 数据库操作
class MysqlDb():

    def open_db(self):
        # 建立连接
        self.conn = pymysql.connect(
            host='127.0.0.1',
            port=3306,
            user='root',
            passwd='root',
            db='taobao',
            charset='utf8',
            use_unicode=True
        )  # 有中文要存入数据库的话要加charset='utf8'和use_unicode=True
        # 创建游标
        self.cursor = self.conn.cursor()

    def create_table(self):
        try:
            self.open_db()
            table_sql = """
            create table if not exists taobao_things(
                id int not null auto_increment,
                标题 varchar(500),
                销量 varchar(500),
                价格 varchar(500),
                点名 varchar(500),
                店铺地址 varchar(500),
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

    def insert_table(self, list_data):

        try:
            self.open_db()
            # sql语句
            insert_sql = """
            insert into taobao_things(标题,销量,价格,点名,店铺地址)
             VALUES(%s,%s,%s,%s,%s) 
            """
            try:
                # 执行插入数据到数据库操作
                self.cursor.execute(insert_sql, list_data)
                # 提交，不进行提交无法保存到数据库
                self.conn.commit()
                print('提交成功')
            except Exception as e:
                print(e)

        except Exception as e:
            self.conn.rollback()
            print(e)
        finally:
            self.close_db()

    def close_db(self):
        # 关闭游标和连接
        self.cursor.close()
        self.conn.close()


class taobao(object):
    def __init__(self):
        self.browser = webdriver.Chrome(
            r'C:\Users\Hadoop\Desktop\chromedriver.exe')
        self.browser.maximize_window()
        self.domain = 'https://login.taobao.com/member/login.jhtml?spm=a21bo.jianhua.754894437.1.5af911d9xixuHY&f=top&redirectURL=https%3A%2F%2Fwww.taobao.com%2F'
        self.browser.implicitly_wait(5)
        self.browser.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """Object.defineProperty(navigator, 'webdriver', {get: () => undefined})""",
        })
        self.action_chains = ActionChains(self.browser)

    def login(self, username, password):
        self.browser.get(self.domain)
        time.sleep(1)
        self.browser.find_element_by_xpath(
            '//*[@id="fm-login-id"]').send_keys(username)
        self.browser.find_element_by_xpath(
            '//*[@id="fm-login-password"]').send_keys(password)
        time.sleep(1)
        coords = pyautogui.locateOnScreen("login.png", confidence=0.5)
        x, y = pyautogui.center(coords)
        time.sleep(2)
        pyautogui.leftClick(x, y)

    def get_product(self, product_name):
        self.browser.find_element_by_class_name(
            'search-combobox-input').send_keys(product_name)
        self.browser.find_element_by_xpath(
            "(//button[contains(@class, 'submit')]|//button[contains(@class,'btn-search')])").click()
        # 等待加载
        time.sleep(1)
        self.get_product_detail()

    # 重试3次，间隔1s
    # @retry(stop_max_attempt_number=3, wait_fixed=1000)
    def get_product_detail(self):
        while True:
            try:
                # 模拟往下滚动
                self.drop_down()
                ps = self.browser.page_source
                selector = etree.HTML(ps)
                page = ''.join(selector.xpath(
                    "//li[@class='item active']//text()")).strip('\n ')
                items = selector.xpath("//div[@id='mainsrp-itemlist']/div[contains(@class,'m-itemlist')]"
                                       "/div[contains(@class,'grid g-clearfix')]/div[contains(@class,'items')]"
                                       "/div[@class='item J_MouserOnverReq  ']")
                for item in items:
                    price = ''
                    for i in item.xpath(".//div[contains(@class,'price')]//text()"):
                        price += i
                    price = price.replace('\n', '').replace(' ', '')

                    sales = item.xpath(
                        ".//div[@class='deal-cnt']//text()")[0].replace('人付款', '')

                    title = ''
                    for i in item.xpath(".//div[contains(@class,'row-2')]//text()"):
                        title += i
                    title = title.replace('\n', '').replace(' ', '')

                    shop_name = ''
                    for i in item.xpath(".//div[contains(@class, 'shop')]//text()"):
                        shop_name += i
                    shop_name = shop_name.replace('\n', '').replace(' ', '')

                    location = ''
                    for i in item.xpath(".//div[@class='location']//text()"):
                        location += i
                    location = location.replace('\n', '').replace(' ', '')

                    data = {
                        '标题': title,
                        '销量': sales,
                        '价格': price,
                        '店名': shop_name,
                        '店铺地址': location
                    }

                    # 日志
                    logger.info(
                        f"标题:{title}|销量:{sales}|价格:{price}|店名:{shop_name}|商铺地址:{location}")

                    # json文件
                    with open('things.json', 'a+', encoding='utf-8') as f:
                        f.write(json.dumps(data, ensure_ascii=False))

                    # 数据库
                    MysqlDb().insert_table([title, sales, price, shop_name, location])

                logger.info(f'抓取第{page}页完成')

                # 下一页
                next = self.browser.find_element_by_xpath(
                    "//li[contains(@class, 'item next')]")
                if 'next-disabled' in next.get_attribute('class'):
                    logger.info('没有下一页，抓取完成')
                    break
                else:
                    next.click()

            # 出现滑块验证
            except ElementClickInterceptedException:
                slider = self.browser.find_element_by_xpath(
                    "//span[contains(@class, 'btn_slide')]")
                self.action_chains.drag_and_drop_by_offset(
                    slider, 258, 0).perform()
                time.sleep(0.5)
                self.action_chains.release().perform()

            except Exception as e:
                logger.error('出现未知错误:' + e)
                print('出现未知错误:' + e)
                self.browser.refresh()
                time.sleep(1)

    # js控制往下拖动
    def drop_down(self):
        for x in range(1, 9):
            time.sleep(0.3)
            j = x / 10
            js = f"document.documentElement.scrollTop = document.documentElement.scrollHeight * {j}"
            self.browser.execute_script(js)
        # 姐姐~太快容易出验证码
        time.sleep(2)

    def get_nickname(self):
        self.browser.get(self.domain)
        time.sleep(0.5)
        try:
            return self.browser.find_element_by_class_name('site-nav-user').text
        except NoSuchElementException:
            return ''


if __name__ == '__main__':
    # 建表 (在这之前先建库,库名叫 web )   create database web;
    MysqlDb().create_table()

    # 填入自己的用户名，密码
    username = '17304096155'
    password = 'qssbdlr.'
    tb = taobao()
    tb.login(username, password)
    # 可以改成想要商品的名称
    product_name = '零食'
    tb.get_product(product_name)
