#!/usr/bin/env python
# -*- coding: utf-8 -*-

import vk
import pymysql.cursors
import re
import time

session = vk.Session()
api = vk.API(session)

connection = pymysql.connect(host='localhost',
                             user='root',
                             password='3775578',
                             db='bsh',
                             charset='utf8',
                             cursorclass=pymysql.cursors.DictCursor)

#Запись данных в базу
def write_to_base(group_id,users_info,offset_mark):
    with connection.cursor() as cursor:
        if offset_mark==0:
            sql = "INSERT INTO `tbl_vk_1` (`group_id`, `users`) VALUES (%s, %s)"
            cursor.execute(sql, (group_id, users_info))
        else:
            sql = "UPDATE `tbl_vk_1` SET `users`= concat(users,%s) WHERE `group_id`=%s"
            cursor.execute(sql, (users_info, group_id))

    connection.commit()

def main():
    list_of_users_bash = []
    list_of_all = []
    timeout = 0.5
    #Открываем файл input.txt и забираем оттуда группы
    with open('input.txt','r') as input:
        groups = input.read().splitlines()
        #Проходим по каждой группе
        for group_id in groups:
            print(group_id)
            #Добаваем список пользователей и превращаем их в строку с разделителем запятая
            for offset in range(0,10000):
                try:
                    list_of_users_modify = ", ".join(repr(e) for e in api.groups.getMembers(group_id=group_id,count=1000,offset=offset*1000)['users'])
                    if len(list_of_users_modify)==0:
                        break
                except:
                    print((group_id,'Fail'))
                    break

                list_of_values = api.users.get(user_ids=list_of_users_modify,fields='sex, bdate, city, country, online, online_mobile, lists, domain, has_mobile, contacts, connections, site, education, universities, schools, status')
                print("Collect info to Range",offset)
                #Получаем список значений по этому списку пользователей
                #Создаем строку для записи в БД (регуляркой вычищаем не utf-8 символы и записываем в базу)
                string = str(list_of_values)
                a = re.findall('[^\'№¼,.}{\a-zA-Z0-9_А-Яа-я]', string)
                for item in a:
                    string = string.replace(item, '')
                print("Try to load to DB")
                write_to_base(group_id,string,offset)
                #Проверяем есть ли люди из Башкортостана
                with open('Bashdict.txt', 'r') as city_list:
                    city_list_new = city_list.read().replace(' ', '').split(",")

                    for value in list_of_values:
                        try:
                            user_city = str(value['city'])
                            list_of_all.append(value['uid'])
                            if user_city in city_list_new:
                                list_of_users_bash.append(value['uid'])
                        except:
                            pass
                time.sleep(timeout)

        print("This is all")

    #Возвращаем всех людей из региона Башкортостан
    print(list_of_users_bash)
    print(len(list_of_users_bash))
    print(len(list_of_all))
    #with open('output.txt','w+') as output:
    #    output.write(str(list_of_users_bash))

main()

