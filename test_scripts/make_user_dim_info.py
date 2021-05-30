from faker.factory import Factory
import random
import time


class Producer:
    def __init__(self, and_num=10000, ios_num=5000):
        self.Faker = Factory.create
        self.fake = self.Faker("zh_CN")
        self.app_version = (
            '1.1.221', '1.1.236', '1.1.248', '1.2.101', '1.2.213', '1.2.256', '1.2.258', '1.3.10', '1.3.34', '1.3.59',
            '1.3.89', '1.4.9', '1.4.268', '1.4.888', '1.4.999')  # App15个版本
        self.country = '中国'
        self.province_list = []  # 选取中国20个省级
        self.isp_list = ['中国移动', '中国电信', '中国联通']  # 运营商
        self.android_os_list = []  # 安卓系统版本
        self.ios_os_list = []  # ios系统版本
        self.mac_list = []
        self.ip_list = []
        self.and_uid_dict = {}
        self.ios_uid_dict = {}
        self.gender = ['男', '女', '男', '女', '男', '女', '男', '女', '男', '女', '未知']
        self.age = [i for i in range(10, 60)]
        self.and_num = and_num
        self.ios_num = ios_num

    def pro_province(self) -> None:
        """

        生成省份列表（20个）
        """
        t = time.time()
        province_set = set()
        while len(province_set) <= 20:
            province_set.add(self.fake.province())
        self.province_list = list(province_set)
        t1 = time.time()
        print('生成省份列表完成' + "\t time(s):" + str(round(t1 - t, 4)))

    def pro_and_sys_ver(self) -> None:
        """

        生成20个安卓系统版本
        """
        t = time.time()
        android_system_os = set()
        while len(android_system_os) <= 15:
            os_version = self.fake.android_platform_token()
            if os_version >= 'Android 5':
                continue
            android_system_os.add(os_version)
        self.android_os_list = list(android_system_os)
        t1 = time.time()
        print('生成安卓系统列表完成' + "\t time(s):" + str(round(t1 - t, 4)))

    def pro_ios_sys_ver(self) -> None:
        """

        生成20个ios系统版本
        """
        t = time.time()
        ios_system_os = set()
        while len(ios_system_os) <= 5:
            os_version = self.fake.ios_platform_token()
            if "iPhone" not in os_version:
                continue
            ios_system_os.add(os_version)
        self.ios_os_list = list(ios_system_os)
        t1 = time.time()
        print('生成ios系统列表完成' + "\t time(s):" + str(round(t1 - t, 4)))

    def pro_mac(self) -> None:
        """

        生成mac
        """
        t = time.time()
        mac_set = set()
        while len(mac_set) < self.and_num + self.ios_num:
            mac_set.add(self.fake.mac_address())
        self.mac_list = list(mac_set)
        t1 = time.time()
        print('生成mac列表完成' + "\t time(s):" + str(round(t1 - t, 4)))

    def pro_ip(self) -> None:
        """

        生成ip
        """
        t = time.time()
        ip_set = set()
        while len(ip_set) < self.and_num + self.ios_num:
            ip_set.add(self.fake.ipv4(network=False))
        self.ip_list = list(ip_set)
        t1 = time.time()
        print('生成ip列表完成' + "\t time(s):" + str(round(t1 - t, 4)))

    def pro_and_uid(self) -> None:
        """

        生成安卓uuid
        """
        t = time.time()
        for uid in range(100000000, 100000000+self.and_num):
            self.and_uid_dict[uid] = {'platform': 'android', 'country': self.country}
        t1 = time.time()
        print('生成安卓uuid列表完成' + "\t time(s):" + str(round(t1 - t, 4)))

    def pro_ios_uid(self) -> None:
        """

        生成ios uuid
        """
        t = time.time()
        for uid in range(200000000, 200000000 + self.ios_num):
            self.ios_uid_dict[uid] = {'platform': 'ios', 'country': self.country}
        t1 = time.time()
        print('生成ios uuid列表完成' + "\t time(s):" + str(round(t1 - t, 4)))

    def get_app_ver(self) -> str:
        """

        :return:<str>返回一个随机app版本
        """
        return random.choice(self.app_version)

    def get_and_sys_os(self) -> str:
        """

        :return:<str>返回一个随机安卓系统版本
        """
        if len(self.android_os_list) <= 0:
            self.pro_and_sys_ver()
        return random.choice(self.android_os_list)

    def get_ios_sys_os(self) -> str:
        """

        :return:<str>返回一个随机ios系统版本
        """
        if len(self.ios_os_list) <= 0:
            self.pro_ios_sys_ver()
        return random.choice(self.ios_os_list)

    def get_province(self) -> str:
        """

        :return: <str>返回一个随机省份
        """
        if len(self.province_list) <= 0:
            self.pro_province()
        return random.choice(self.province_list)

    def get_isp(self) -> str:
        """

        :return:<str>返回一个随机运营商
        """
        return random.choice(self.isp_list)

    def get_mac(self) -> str:
        """

        :return:返回一个随机mac地址，并从list中移除
        """
        if len(self.mac_list) <= 0:
            self.pro_mac()
        n = len(self.mac_list)
        rx = random.randint(0, n - 1)
        self.mac_list[rx], self.mac_list[-1] = self.mac_list[-1], self.mac_list[rx]
        return self.mac_list.pop()

    def get_ip(self) -> str:
        """

        :return:返回一个随机ip地址，并从list中移除
        """
        if len(self.ip_list) <= 0:
            self.pro_ip()
        n = len(self.ip_list)
        rx = random.randint(0, n - 1)
        self.ip_list[rx], self.ip_list[-1] = self.ip_list[-1], self.ip_list[rx]
        return self.ip_list.pop()

    def get_gender(self) -> str:
        """

        :return:<str>返回一个随机性别
        """
        return random.choice(self.gender)

    def get_age(self) -> int:
        """

        :return:<str>返回一个随机年龄
        """
        return random.choice(self.age)

    def pro_and_user_info(self) -> dict:
        """

        :return:<dict>返回安卓用户信息字典
        """
        t = time.time()
        if len(self.and_uid_dict) <= 0:
            self.pro_and_uid()
        for user, values in self.and_uid_dict.items():
            self.and_uid_dict[user]['province'] = self.get_province()
            self.and_uid_dict[user]['isp'] = self.get_isp()
            self.and_uid_dict[user]['app_version'] = self.get_app_ver()
            self.and_uid_dict[user]['os_version'] = self.get_and_sys_os()
            self.and_uid_dict[user]['mac'] = self.get_mac()
            self.and_uid_dict[user]['ip'] = self.get_ip()
            self.and_uid_dict[user]['gender'] = self.get_gender()
            self.and_uid_dict[user]['age'] = self.get_age()
        t1 = time.time()
        print('生成安卓 uuid列表完成' + "\t time(s):" + str(round(t1 - t, 4)))
        return self.and_uid_dict

    def pro_ios_user_info(self) -> dict:
        """

        :return:<dict>返回ios用户信息字典
        """
        t = time.time()
        if len(self.ios_uid_dict) <= 0:
            self.pro_ios_uid()
        for user, values in self.ios_uid_dict.items():
            self.ios_uid_dict[user]['province'] = self.get_province()
            self.ios_uid_dict[user]['isp'] = self.get_isp()
            self.ios_uid_dict[user]['app_version'] = self.get_app_ver()
            self.ios_uid_dict[user]['os_version'] = self.get_ios_sys_os()
            self.ios_uid_dict[user]['mac'] = self.get_mac()
            self.ios_uid_dict[user]['ip'] = self.get_ip()
            self.ios_uid_dict[user]['gender'] = self.get_gender()
            self.ios_uid_dict[user]['age'] = self.get_age()
        t1 = time.time()
        print('生成ios uuid列表完成' + "\t time(s):" + str(round(t1 - t, 4)))
        return self.ios_uid_dict

    def pro_user_info(self) -> dict:
        """

        :return:<dict>返回安卓和ios 整体用户字典
        """
        t = time.time()
        if len(self.and_uid_dict) <= 0:
            self.pro_and_user_info()
        if len(self.ios_uid_dict) <= 0:
            self.pro_ios_user_info()
        uuid_dict = self.and_uid_dict.copy()
        uuid_dict.update(self.ios_uid_dict)
        t1 = time.time()
        print('生成全部 uuid列表完成' + "\t time(s):" + str(round(t1 - t, 4)))
        return uuid_dict
