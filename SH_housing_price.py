import requests
import time
import re
import pymongo
import json 

city = 'shanghai'
base_url = 'https://www.anjuke.com/fangjia/'
#url_district = 'https://www.anjuke.com/fangjia/shanghai2011/pudong/'
#url_town = 'https://www.anjuke.com/fangjia/shanghai2011/beicai/'
user_gent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
headers = {'User-Agent':user_gent}

year_start = 2011
year_end = 2021

client = pymongo.MongoClient("mongodb+srv://<username>:<password>@tony-mongodb-cluster-eahql.gcp.mongodb.net/test?retryWrites=true&w=majority")
m_db = client['housing_price']

def get_districts_name_list_by_city(city):
    districts_list = []

    url = base_url + city + str(year_start) + '/'

    # Sleep 10 seconds
    time.sleep(10)

    res = requests.get(url, headers=headers)
    res_text = res.text
    
    with open ('/Users/fengt/Desktop/1.html','+w') as f:
        f.write(res_text)
    
    url_list = re.findall('http://www.anjuke.com/fangjia/%s%d/[\w]*/">[\u4e00-\u9fa5]*</a>' %(city, year_start), res_text)

    for u in url_list:
        l = get_names(u)
        districts_list.append(l)

    return districts_list

def get_towns_name_list_by_district(city,district):
    towns_list = []

    url = base_url + city + str(year_start) + '/%s/' %district

    # Sleep 10 seconds
    time.sleep(10)

    res = requests.get(url, headers=headers)
    res_text = res.text
    
    with open ('/Users/fengt/Desktop/2.html','+w') as f:
        f.write(res_text)
    
    # filter the sub-item with regular expression single mode  
    towns_url_html = re.findall('<div class="sub-items">.*?</div>',res_text,flags=re.DOTALL)[0]
    # filter the towns' url list
    towns_url_list = re.findall('http://www.anjuke.com/fangjia/%s%d/[\w]*/">[\u4e00-\u9fa5]*</a>' %(city,year_start), towns_url_html)

    for u in towns_url_list:
        l = get_names(u)
        towns_list.append(l)

    return towns_list

def get_names(s):
    name_list = []

    pinyin_name = re.findall('(?<=[\d]{4}/)[\w]*', s)[0]
    chainese_name = re.findall('[\u4e00-\u9fa5]{2,8}', s)[0]
    if not (pinyin_name == 'shanghaizhoubian' or chainese_name == '上海周边'):
        name_list.append(chainese_name + ':' + pinyin_name)

    return name_list

def get_dist_price_by_month(year,city,area_cn_name,area_py_name):

    str_month_price = ''

    str_mongo_entry = '{"cn_name": "%s", "py_name": "%s", "level": "DIST", "district": "%s", "city": "%s", "year": "%d", "avg_price": [' %(area_cn_name, area_py_name, area_py_name, city, year)

    url = base_url + city + str(year) + '/' + area_py_name + '/'

    # Sleep 10 seconds
    time.sleep(10)

    res = requests.get(url,headers=headers)

    str_month = re.findall('(?<=xdata: \[).*?(?=\],)', res.text)
    str_price = re.findall('(?<=ydata: \[{"name":"%s","data":\[)[\d,]*' %area_cn_name, res.text)
    if len(str_price) == 0 or len(str_month) == 0:
        return None
    
    str_month = str_month[0]
    str_month_list = str_month.split(',')
    month_count = len(str_month_list)

    str_price = str_price[0]
    str_price_list =str_price.split(',')
 
    for i in range(month_count):
        if i < month_count - 1:
            str_month_price = str_month_price + '{"month": %s, "price": "%s"},' % (str_month_list[i],str_price_list[i])
        else:
            str_month_price = str_month_price + '{"month": %s, "price": "%s"}' % (str_month_list[i],str_price_list[i])

    str_mongo_entry = str_mongo_entry + str_month_price + ']}'
    print (str_mongo_entry)
    
    json_mongo_entry = json.loads(str_mongo_entry)
    return json_mongo_entry

def get_town_price_by_month(year,city,dist,area_cn_name,area_py_name):

    str_month_price = ''

    str_mongo_entry = '{"cn_name": "%s", "py_name": "%s", "level": "TOWN", "district": "%s", "city": "%s", "year": "%d", "avg_price": [' %(area_cn_name, area_py_name, dist, city, year)

    url = base_url + city + str(year) + '/' + area_py_name + '/'

    # Sleep 10 seconds
    time.sleep(10)

    res = requests.get(url,headers=headers)

    str_month = re.findall('(?<=xdata: \[).*?(?=\],)', res.text)


    str_price = re.findall('(?<=ydata: \[{"name":"%s","data":\[)[\d,]*' %area_cn_name, res.text)
    if len(str_price) == 0 or len(str_month) == 0:
        return None
    str_month = str_month[0]
    str_month_list = str_month.split(',')
    month_count = len(str_month_list)

    str_price = str_price[0]
    str_price_list =str_price.split(',')

    for i in range(month_count):
        if i < month_count - 1:
            str_month_price = str_month_price + '{"month": %s, "price": "%s"},' % (str_month_list[i],str_price_list[i])
        else:
            str_month_price = str_month_price + '{"month": %s, "price": "%s"}' % (str_month_list[i],str_price_list[i])

    str_mongo_entry = str_mongo_entry + str_month_price + ']}'
    print (str_mongo_entry)

    json_mongo_entry = json.loads(str_mongo_entry)
    return json_mongo_entry

if __name__ == '__main__':
    
    district_list = get_districts_name_list_by_city(city)

    for year in range (year_start,year_end):
        collection = m_db[city + '_' + str(year)]
        for dist in district_list:
            if dist == []:
                continue
            dist_names = dist[0].split(':')
            dist_cn_name = dist_names[0]
            dist_py_name = dist_names[1]
            post_dist_entry = get_dist_price_by_month(year,city,dist_cn_name,dist_py_name)
            if post_dist_entry is None:
                continue
            else:
                collection.insert_one(post_dist_entry)

            town_list = get_towns_name_list_by_district(city,dist_py_name)
            for town in town_list:
                town_names = town[0].split(':')
                town_cn_name = town_names[0]
                town_py_name = town_names[1]
                print(town_cn_name)
                print ('Start Time: ',time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                post_town_entry = get_town_price_by_month(year,city,dist_py_name,town_cn_name,town_py_name)
                if post_town_entry is None:
                    continue
                else:
                    collection.insert_one(post_town_entry)
                    print ('---Price of %s for year %d has been inserted into MongoDB---' %(town_cn_name,year))
                    print ('End Time: ', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))