from faker.factory import Factory
import random
import json
import time


class Producer:
    def __init__(self, andnum=10000, iosnum=5000):
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
        self.and_uuid_dict = {}
        self.ios_uuid_dict = {}
        self.gender = ['男', '女', '男', '女', '男', '女', '男', '女', '男', '女', '未知']
        self.age = [i for i in range(10, 60)]
        self.andnum = andnum
        self.iosnum = iosnum

    def proProvince(self) -> None:
        """

        :return:<list> 省份列表（20个）
        """
        t = time.time()
        province_set = set()
        while len(province_set) <= 20:
            province_set.add(self.fake.province())
        self.province_list = list(province_set)
        t1 = time.time()
        print('生成省份列表完成' + "\t time(s):" + str(round(t1 - t, 4)))

    def proAndSysVer(self) -> None:
        """

        :return:<list>20个安卓系统版本
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

    def proIosSysVer(self) -> None:
        """

        :return:<list>20个ios系统版本
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

    def proMac(self) -> None:
        """

        :return:<list> 15000个mac
        """
        t = time.time()
        mac_set = set()
        while len(mac_set) < self.andnum + self.iosnum:
            mac_set.add(self.fake.mac_address())
        self.mac_list = list(mac_set)
        t1 = time.time()
        print('生成mac列表完成' + "\t time(s):" + str(round(t1 - t, 4)))

    def proIp(self) -> None:
        """

        :return:<list>15000个ip
        """
        t = time.time()
        ip_set = set()
        while len(ip_set) < self.andnum + self.iosnum:
            ip_set.add(self.fake.ipv4(network=False))
        self.ip_list = list(ip_set)
        t1 = time.time()
        print('生成ip列表完成' + "\t time(s):" + str(round(t1 - t, 4)))

    def proAndUuid(self) -> None:
        """

        :return:<list>10000个安卓uuid
        """
        t = time.time()
        for uuid in range(100000000,100000000+self.andnum):
            self.and_uuid_dict[uuid] = {'platform': 'android', 'country': self.country}
        t1 = time.time()
        print('生成安卓uuid列表完成' + "\t time(s):" + str(round(t1 - t, 4)))

    def proIosUuid(self) -> None:
        """

        :return:<list>5000个ios uuid
        """
        t = time.time()
        for uuid in range(200000000, 200000000 + self.iosnum):
            self.ios_uuid_dict[uuid] = {'platform': 'ios', 'country': self.country}
        t1 = time.time()
        print('生成ios uuid列表完成' + "\t time(s):" + str(round(t1 - t, 4)))

    def getAppVersion(self) -> str:
        """

        :return:<str>返回一个随机app版本
        """
        return random.choice(self.app_version)

    def getAndSysOs(self) -> str:
        """

        :return:<str>返回一个随机安卓系统版本
        """
        if len(self.android_os_list) <= 0:
            self.proAndSysVer()
        return random.choice(self.android_os_list)

    def getIosSysOs(self) -> str:
        """

        :return:<str>返回一个随机ios系统版本
        """
        if len(self.ios_os_list) <= 0:
            self.proIosSysVer()
        return random.choice(self.ios_os_list)

    def getProvince(self) -> str:
        """

        :return: <str>返回一个随机省份
        """
        if len(self.province_list) <= 0:
            self.proProvince()
        return random.choice(self.province_list)

    def getIsp(self) -> str:
        """

        :return:<str>返回一个随机运营商
        """
        return random.choice(self.isp_list)

    def getMac(self) -> str:
        """

        :return:返回一个随机mac地址，并从list中移除
        """
        if len(self.mac_list) <= 0:
            self.proMac()
        n = len(self.mac_list)
        rx = random.randint(0, n - 1)
        self.mac_list[rx], self.mac_list[-1] = self.mac_list[-1], self.mac_list[rx]
        return self.mac_list.pop()

    def getIp(self) -> str:
        """

        :return:返回一个随机ip地址，并从list中移除
        """
        if len(self.ip_list) <= 0:
            self.proIp()
        n = len(self.ip_list)
        rx = random.randint(0, n - 1)
        self.ip_list[rx], self.ip_list[-1] = self.ip_list[-1], self.ip_list[rx]
        return self.ip_list.pop()

    def getGender(self) -> str:
        """

        :return:<str>返回一个随机性别
        """
        return random.choice(self.gender)

    def getAge(self) -> int:
        """

        :return:<str>返回一个随机年龄
        """
        return random.choice(self.age)

    def proAndUserInfo(self) -> dict:
        """

        :return:<dict>返回安卓1w的用户信息字典
        """
        t = time.time()
        if len(self.and_uuid_dict) <= 0:
            self.proAndUuid()
        for user, values in self.and_uuid_dict.items():
            self.and_uuid_dict[user]['province'] = self.getProvince()
            self.and_uuid_dict[user]['isp'] = self.getIsp()
            self.and_uuid_dict[user]['app_version'] = self.getAppVersion()
            self.and_uuid_dict[user]['os_version'] = self.getAndSysOs()
            self.and_uuid_dict[user]['mac'] = self.getMac()
            self.and_uuid_dict[user]['ip'] = self.getIp()
            self.and_uuid_dict[user]['gender'] = self.getGender()
            self.and_uuid_dict[user]['age'] = self.getAge()
        t1 = time.time()
        print('生成安卓 uuid列表完成' + "\t time(s):" + str(round(t1 - t, 4)))
        return self.and_uuid_dict

    def proIosUserInfo(self) -> dict:
        """

        :return:<dict>返回ios 5000的用户信息字典
        """
        t = time.time()
        if len(self.ios_uuid_dict) <= 0:
            self.proIosUuid()
        for user, values in self.ios_uuid_dict.items():
            self.ios_uuid_dict[user]['province'] = self.getProvince()
            self.ios_uuid_dict[user]['isp'] = self.getIsp()
            self.ios_uuid_dict[user]['app_version'] = self.getAppVersion()
            self.ios_uuid_dict[user]['os_version'] = self.getIosSysOs()
            self.ios_uuid_dict[user]['mac'] = self.getMac()
            self.ios_uuid_dict[user]['ip'] = self.getIp()
            self.ios_uuid_dict[user]['gender'] = self.getGender()
            self.ios_uuid_dict[user]['age'] = self.getAge()
        t1 = time.time()
        print('生成ios uuid列表完成' + "\t time(s):" + str(round(t1 - t, 4)))
        return self.ios_uuid_dict

    def proUserInfo(self) -> dict:
        """

        :return:<dict>返回安卓和ios 整体15w的用户
        """
        t = time.time()
        if len(self.and_uuid_dict) <= 0:
            self.proAndUserInfo()
        if len(self.ios_uuid_dict) <= 0:
            self.proIosUserInfo()
        uuid_dict = self.and_uuid_dict.copy()
        uuid_dict.update(self.ios_uuid_dict)
        t1 = time.time()
        print('生成全部 uuid列表完成' + "\t time(s):" + str(round(t1 - t, 4)))
        return uuid_dict


pro = Producer()
t = time.time()
user_dict = pro.proUserInfo()
# 生成安卓uuid 个数
andnum = 10000
# 生成ios uuid 个数
iosnum = 5000
with open('testJsonFiles.txt', 'w', encoding='utf-8') as f1:
    n = 0
    for k, v in user_dict.items():
        v['uid'] = k
        mainTxt = json.dumps(v, ensure_ascii=False)
        f1.write(mainTxt + '\n')
t1 = time.time()
print('（整体）生成用户信息完成' + "\t time(s):" + str(round(t1 - t, 4)))
