# -*- coding: utf-8 -*-
import requests
import re
from bs4 import BeautifulSoup
import pandas as pd
import urllib
from PIL import Image
import os
from multiprocessing.dummy import Pool as ThreadPool
import sys



SPIDER_REQUEST_ERR = '爬虫请求失败!'
#搜索结果中的图片的保存路径
SEARCH_PIC_PATH = 'C:\\Users\\Python\\Desktop\\zufang\\pic\\'
DETAIL_PIC_PATH = 'C:\\Users\\Python\\Desktop\\zufang\\detail\\'

class LianjiaSpider():
    def __init__(self,category,city,page_num,house_num_per_page):
        '''
        category:爬取的类型,此参数的值可以是ershoufang(二手房),zufang(租房)
        city:    爬取的城市
        page_num:爬取的总页数
        house_num_per_page:搜索出来的结果中,每页显示的房子数量
        '''
        self.category = category
        self.city = city
        self.page_num = page_num
        self.house_num_per_page = house_num_per_page
        self.url_temp = 'https://' + city+ '.lianjia.com/'+category+'/pg{}'
        self.headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36'}
    
    def generate_url_list(self):
        '''
        生成需要爬取的分页的url
        '''
        return [self.url_temp.format(i) for i in range(1,self.page_num+1)]

    def get_response(self,url):#发送请求,获取响应
        print(url)
        response = requests.get(url,headers = self.headers,timeout=30)
        print(response.status_code)
        assert response.status_code == 200,SPIDER_REQUEST_ERR    
        return response.content.decode()

    def save_html(self,html_str,page_num):
        file_path = '{}-第{}页.html'.format(self.category,page_num)
        with open(file_path,'w',encoding='utf-8') as f:
            f.write(html_str)

    def get_house_url(self,text):
        '''
        获取每一个房子的url
        '''
        url_list = []
        # pattern = r'https://{}.lianjia.com/{}/{}.html'.format(self.city,self.category,'[0-9]+')
        # url_list = re.findall(pattern,text)
        # print('爬取的所有二手房源链接数量为:',len(url_list))
        # #因为整个页面中出现了四次url,所以剔除重复的
        # return [url_list[i] for i in range(len(url_list)) if i%2==0][0:30]

        soup = BeautifulSoup(text,'lxml')
        if self.category == 'zufang':
            house_code_list = soup.select(".house-lst > li")
        elif self.category == 'ershoufang':
            house_code_list = soup.select("a[class='noresultRecommend img ']")
        for code in house_code_list:
            url = r'https://{}.lianjia.com/{}/{}.html'.format(self.city,self.category,code.get('data-housecode'))
            url_list.append(url)
        print(url_list)
        return url_list

        

    def get_pic_url_from_search_result(self,text):
        '''
        获取在搜索结果中展示的图片的url
        此处只有一张客厅的图片
        '''
        soup = BeautifulSoup(text,'lxml')
        pic_url = [content.get('data-original') for content in soup.select('.lj-lazy')]
        pic_id = [code.get('data-housecode') for code in soup.select('.noresultRecommend')]
        return dict(zip(pic_id,pic_url))
    
    def get_pic_url_from_detail_page(self,text):
        '''
        从详情页获取房子的详细图片,包括客厅,卧室等,而且图片数量并不一定
        '''
        soup = BeautifulSoup(text,'lxml')
        #获取全部关于房子的图片
        if self.category == 'ershoufang':
            total_pic = soup.select("ul[class='smallpic'] > li")
        elif self.category == 'zufang':
            total_pic = soup.select("div[class='thumbnail'] > ul > li")

        pic_name = [content.get('data-desc') for content in total_pic]
        pic_url = [content.get('data-src') for content in total_pic]
        return dict(zip(pic_name,pic_url))
    
    def resize_pic(self,infile,outfile,x,y):
        '''
        此方法用来对图片尺寸进行修改,原因在于同一张图片在不同的地方以不同的尺寸使用了
        如果各种尺寸都爬一遍,一来数据库的设计会非常复杂,再者爬取效率会非常低下
        所以只爬取一个大的图片,在使用的时候直接剪裁成相应尺寸
        '''
        im = Image.open(infile)
        (x,y) = im.size #read image size
        x_s = 250 #define standard width
        y_s = y * x_s / x #calc height based on standard width
        out = im.resize((x_s,y_s),Image.ANTIALIAS) #resize image with high-quality
        out.save(outfile)

    def download_pic(self,url,path):
        data = urllib.request.urlopen(url).read()
        with open(path,"wb") as f:
            f.write(data)
    
    def gen_folder_name(self,pic_code,name):
        return SEARCH_PIC_PATH + pic_code + '\\'+'{}_{}.jpg'.format(pic_code,name)

    def parse_ershoufang_html(self,res):
        '''
        二手房和租房的HTML结构不同,所以写成两个方法,这个是二手房的解析
        '''
        info = {}
        soup = BeautifulSoup(res, 'lxml')
        info['标题'] = soup.select('.main')[0].text
        info['总价'] = soup.select('.total')[0].text + '万'
        info['面积'] = soup.select('.mainInfo')[2].text
        info['每平方售价'] = soup.select('.unitPriceValue')[0].text
        info['房间数量'] = soup.select('.mainInfo')[0].text
        info['楼层'] = soup.select('.subInfo')[0].text
        info['装修'] = soup.select('.subInfo')[1].text
        info['建造时间'] = soup.select('.subInfo')[2].text
        info['小区名称'] = soup.select('.info')[0].text
        info['所在区域'] = soup.select('.info a')[0].text + ':' + soup.select('.info a')[1].text
        info['链家编号'] = soup.select('.btnContainer')[0].get('data-lj_action_resblock_id')
        return info
    
    def parse_zufang_html(self,res):
        '''
        租房HTML的解析
        '''
        info = {}
        soup = BeautifulSoup(res, 'lxml')
        info['标题'] = soup.select('.main')[0].text
        info['价格'] = soup.select('.total')[0].text + '元/月'
        info['面积'] = soup.select('.lf')[0].text.split('：')[1]
        info['户型'] = soup.select('.lf')[1].text.split('：')[1]
        info['楼层'] = soup.select('.lf')[2].text.split('：')[1]
        info['链家编号'] = soup.select('.houseNum')[0].text.split('：')[1]
        return info

    def save_to_csv(self,info):
        df = pd.DataFrame(info)
        #df.set_index('链家编号')
        df.to_csv('{}.csv'.format(self.category),mode='a',encoding='utf_8_sig')

    def save_to_xlsx(self,info):
        pass
    
    def select_parser(self,html_text):
        info = {}
        if self.category == 'ershoufang':
            info = self.parse_ershoufang_html(html_text)
        elif self.category == 'zufang':
            info = self.parse_zufang_html(html_text)
        return info
    
    def get_house_info(self,page_url):
        '''
        从一页中获取到所有房子的url并分析保存,方便进行多线程爬取
        '''
        house_info_list = []
        try :
            html_str = self.get_response(page_url)
        except AssertionError as err:
            print(err)
            return
        #获取所有房子的url,放到house_url中
        house_list = self.get_house_url(html_str)

        #获取每个房子的基本信息并写入到csv文件
        for house_url in house_list:
            try:
                html_detail = self.get_response(house_url)
            except AssertionError as err:
                print('爬取详情页面失败')
            info = self.select_parser(html_detail)
            #把房子基本信息写入到一个字典列表中
            house_info_list.append(info)

            #为每个房子创建不同的文件夹用来分类存放图片
            try:
                os.mkdir(SEARCH_PIC_PATH+'{}'.format(info['链家编号']))
            except FileExistsError:
                print('此租户数据已经被抓取!')
                continue
            #保存房子的图片
            for pic_name,pic_url in self.get_pic_url_from_detail_page(html_detail).items():
                path = self.gen_folder_name(info['链家编号'],pic_name)
                print(path)
                self.download_pic(pic_url,path)
        self.save_to_csv(house_info_list)
        #return house_info_list

    def test(self):
        url = 'https://sz.lianjia.com/ershoufang/105101385718.html'
        text = self.get_response(url)
        #house_code_list = []
        
        soup = BeautifulSoup(text,'lxml')
        house_code_list = soup.select('.mainInfo')[2].text
        #print('数量为:',len(house_list)
        print(house_code_list)

    def run(self): #实现主要逻辑
        #house_info_list=[]
        #1.构造页面url列表
        url_list = self.generate_url_list()
        print(url_list)
        pool = ThreadPool(4) #4核电脑
        #2.遍历,发送请求,获取响应
        pool.map(self.get_house_info, url_list)#多线程工作
        pool.close()
        pool.join()


if __name__ == '__main__':
    ershoufang_spider = LianjiaSpider('ershoufang','sz',30,30)
    ershoufang_spider.run()
    
    # zufang_spider = LianjiaSpider('zufang','sz',1,30)
    # zufang_spider.run()

    





