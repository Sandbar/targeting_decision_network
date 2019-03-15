

import datetime
import tdn_mongo
import tdn_mysql
import log_maker
import time
import pytz

tz = pytz.timezone('Asia/Shanghai')


class TDN:
    def __init__(self):
        self.date = datetime.datetime.fromtimestamp(int(time.time()), pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d')
        self.res_evas = dict()

    @staticmethod
    def get_yesterday():
        n_days = (datetime.datetime.fromtimestamp(int(time.time()), pytz.timezone('Asia/Shanghai')) + datetime.timedelta(days=-1))
        return n_days.strftime('%Y-%m-%d')

    def version_creator(self):
        iosVersion = ['10#0', '10#1', '10#2', '10#3', '11#0', '11#1', '11#2', '11#3']
        androidVersion = ['6#0', '7#0', '7#1', '8#0', '8#1']
        Android = 'Android_ver_{0}_and_above'
        iOS = 'iOS_ver_{0}_and_above'
        ios_dct = dict()
        for version in iosVersion:
            tmp = iOS.format(version)
            if tmp not in ios_dct:
                ios_dct[tmp] = [0, 0, 0, 0, 0, 0, 0, 0]
        andr_dct = dict()
        for version in androidVersion:
            tmp = Android.format(version)
            if tmp not in andr_dct:
                andr_dct[tmp] = [0, 0, 0, 0, 0, 0, 0, 0]
        return ios_dct, andr_dct

    def struct_blank_tdn(self, method, stype):
        ios_dct, andr_dct = self.version_creator()
        blank_tdn = {
            'method': method,
            'type': stype,
            'config': {'gender': {'1': [0, 0, 0, 0, 0, 0, 0, 0], '2': [0, 0, 0, 0, 0, 0, 0, 0]},
                       'wifi_settings': {'Wifi': [0, 0, 0, 0, 0, 0, 0, 0], 'all': [0, 0, 0, 0, 0, 0, 0, 0]},
                       'delivery_mode': {'event': [0, 0, 0, 0, 0, 0, 0, 0], 'value': [0, 0, 0, 0, 0, 0, 0, 0]},
                       'ios_version': ios_dct, 'android_version': andr_dct, 'video': tdn_mysql.select_videoid(tdn_mongo.find_deltname())
                       },
            'object': tdn_mysql.select_object(method, stype),
            'value': [0, 0, 0, 0, 0, 0, 0, 0]
        }
        return blank_tdn

    def get_other_infos(self, ads, tw, tmp, master):
        if ads['spend'] >= 1:
            self.res_evas[tmp]['value'][2*tw] += master
        if ads['pay'] > 0:
            self.res_evas[tmp]['value'][2*tw+1] += master
        if 'pt' in ads:
            if 'name' in ads['pt']:
                delivery_name = ads['pt']['name'].split(' ')[0].lower()
                delivery_name = delivery_name.replace('_test', '').replace('_standard', '')
                if delivery_name in self.res_evas[tmp]['config']['video'] and 'creative' in ads['pt'] and 'object_story_spec' in ads['pt']['creative'] and 'video_data' in ads['pt']['creative']['object_story_spec'] and 'video_id' in ads['pt']['creative']['object_story_spec']['video_data']:
                    videoid = ads['pt']['creative']['object_story_spec']['video_data']['video_id']
                    videoid = str(videoid)
                    if delivery_name in self.res_evas[tmp]['config']['video'] and videoid in self.res_evas[tmp]['config']['video'][delivery_name]:
                        if ads['spend'] >= 1:
                            self.res_evas[tmp]['config']['video'][delivery_name][videoid][2*tw] += master
                        if ads['pay'] > 0:
                            self.res_evas[tmp]['config']['video'][delivery_name][videoid][2*tw+1] += master

            if 'adset_spec' in ads['pt']:
                if 'optimization_goal' in ads['pt']['adset_spec']:
                    if ads['pt']['adset_spec']['optimization_goal'].lower() == 'value' or ('name' in ads['pt'] and 'value' in ads['pt']['name'].lower()):
                        if ads['spend'] >= 1:
                            self.res_evas[tmp]['config']['delivery_mode']['value'][2*tw] += master
                        if ads['pay'] > 0:
                            self.res_evas[tmp]['config']['delivery_mode']['value'][2*tw+1] += master
                    else:
                        if ads['spend'] >= 1:
                            self.res_evas[tmp]['config']['delivery_mode']['event'][2*tw] += master
                        if ads['pay'] > 0:
                            self.res_evas[tmp]['config']['delivery_mode']['event'][2*tw+1] += master

                if 'targeting' in ads['pt']['adset_spec']:
                    if 'genders' in ads['pt']['adset_spec']['targeting'] and len(ads['pt']['adset_spec']['targeting']['genders']) > 0:
                        genders = ads['pt']['adset_spec']['targeting']['genders']
                        for gender in genders:
                            gender = str(gender)
                            if gender not in self.res_evas[tmp]['config']['gender']:
                                self.res_evas[tmp]['config']['gender'][gender] = [0, 0, 0, 0, 0, 0, 0, 0]
                            if ads['spend'] >= 1:
                                self.res_evas[tmp]['config']['gender'][gender][2*tw] += master
                            if ads['pay'] > 0:
                                self.res_evas[tmp]['config']['gender'][gender][2*tw+1] += master
                    elif 'genders' not in ads['pt']['adset_spec']['targeting'] or len(ads['pt']['adset_spec']['targeting']['genders']):
                        for gens in self.res_evas[tmp]['config']['gender'].keys():
                            if ads['spend'] >= 1:
                                self.res_evas[tmp]['config']['gender'][gens][2*tw] += master
                            if ads['pay'] > 0:
                                self.res_evas[tmp]['config']['gender'][gens][2*tw+1] += master

                    if 'user_os' in ads['pt']['adset_spec']['targeting']:
                        user_os = ads['pt']['adset_spec']['targeting']['user_os']
                        if 'platform' in ads['pt']:
                            if ads['pt']['platform'].lower() == 'android':
                                platform = 'android_version'
                            else:
                                platform = 'ios_version'
                            for uos in user_os:
                                uos = uos.replace(r'.', '#')
                                if uos not in self.res_evas[tmp]['config'][platform]:
                                    self.res_evas[tmp]['config'][platform][uos] = [0, 0, 0, 0, 0, 0, 0, 0]
                                if ads['spend'] >= 1:
                                    self.res_evas[tmp]['config'][platform][uos][2*tw] += master
                                if ads['pay'] > 0:
                                    self.res_evas[tmp]['config'][platform][uos][2*tw+1] += master

                    if 'wireless_carrier' in ads['pt']['adset_spec']['targeting']:
                        wireless_carriers = ads['pt']['adset_spec']['targeting']['wireless_carrier']
                        for wc in wireless_carriers:
                            if wc not in self.res_evas[tmp]['config']['wifi_settings']:
                                self.res_evas[tmp]['config']['wifi_settings'][wc] = [0, 0, 0, 0, 0, 0, 0, 0]
                            if ads['spend'] >= 1:
                                self.res_evas[tmp]['config']['wifi_settings'][wc][2*tw] += master
                            if ads['pay'] > 0:
                                self.res_evas[tmp]['config']['wifi_settings'][wc][2*tw+1] += master
                    else:
                        if ads['spend'] >= 1:
                            self.res_evas[tmp]['config']['wifi_settings']['all'][2*tw] += master
                        if ads['pay'] > 0:
                            self.res_evas[tmp]['config']['wifi_settings']['all'][2*tw+1] += master

    def get_audience_infos(self, ads, tw, method, mtypes, master):
        if 'adset_spec' in ads['pt'] and 'targeting' in ads['pt']['adset_spec'] and \
                'custom_audiences' in ads['pt']['adset_spec']['targeting']:
            custom_audiences = ads['pt']['adset_spec']['targeting']['custom_audiences']
            for caudience in custom_audiences:
                if isinstance(caudience, dict):
                    for audiece in caudience.values():
                        if audiece in mtypes and method+'@'+mtypes[audiece] in self.res_evas and 'platform' in ads['pt']:
                            tmp = method+'@'+mtypes[audiece]
                            if 'name' in ads['pt']:
                                delivery_name = ads['pt']['name'].split(' ')[0].lower()
                                delivery_name = delivery_name.replace('_test', '').replace('_standard', '')
                                if delivery_name in self.res_evas[tmp]['object']:
                                    self.get_other_infos(ads, tw, tmp, master)
                                    if ads['spend'] >= 1:
                                        self.res_evas[tmp]['object'][delivery_name][audiece][2*tw] += master
                                    if ads['pay'] > 0:
                                        self.res_evas[tmp]['object'][delivery_name][audiece][2*tw+1] += master
                elif isinstance(caudience, list):
                    for audiece in caudience:
                        if audiece in mtypes and method+'@'+mtypes[audiece] in self.res_evas and 'platform' in ads['pt']:
                            tmp = method+'@'+mtypes[audiece]
                            if 'name' in ads['pt']:
                                delivery_name = ads['pt']['name'].split(' ')[0].lower()
                                delivery_name = delivery_name.replace('_test', '').replace('_standard', '')
                                if delivery_name in self.res_evas[tmp]['object']:
                                    self.get_other_infos(ads, tw, tmp, master)
                                    if ads['spend'] >= 1:
                                        self.res_evas[tmp]['object'][delivery_name][audiece][2*tw] += master
                                    if ads['pay'] > 0:
                                        self.res_evas[tmp]['object'][delivery_name][audiece][2*tw+1] += master

    def get_pt_infos(self, ads, tw, method, master):
        if 'pt' in ads and 'adset_spec' in ads['pt'] and 'targeting' in ads['pt']['adset_spec']:
            if 'interests' in ads['pt']['adset_spec']['targeting']:
                interests = ads['pt']['adset_spec']['targeting']['interests']
                tmp = method+'@interest'
                if tmp in self.res_evas:
                    if len(interests) > 0:
                        self.get_other_infos(ads, tw, tmp, master)
                    if isinstance(interests, list):
                        for interest in interests:
                            if isinstance(interest, dict) and 'id' in interest:
                                interest['id'] = str(interest['id']).replace('.0', '')
                                if interest['id'] in self.res_evas[tmp]['object']:
                                    if ads['spend'] >= 1:
                                        self.res_evas[tmp]['object'][interest['id']][2*tw] += master
                                    if ads['pay'] > 0:
                                        self.res_evas[tmp]['object'][interest['id']][2*tw+1] += master
                            elif isinstance(interest, str) or isinstance(interest, float):
                                interest = str(interest).replace('.0', '')
                                if interest in self.res_evas[tmp]['object']:
                                    if ads['spend'] >= 1:
                                        self.res_evas[tmp]['object'][interest][2*tw] += master
                                    if ads['pay'] > 0:
                                        self.res_evas[tmp]['object'][interest][2*tw+1] += master

                    elif isinstance(interests, dict):
                        for interest in interests.values():
                            interest['id'] = str(interest['id']).replace('.0', '')
                            if interest['id'] in self.res_evas[tmp]['object']:
                                if ads['spend'] >= 1:
                                    self.res_evas[tmp]['object'][interest['id']][2*tw] += master
                                if ads['pay'] > 0:
                                    self.res_evas[tmp]['object'][interest['id']][2*tw+1] += master

            if 'behaviors' in ads['pt']['adset_spec']['targeting']:
                behaviors = ads['pt']['adset_spec']['targeting']['behaviors']
                tmp = method+'@behavior'
                if tmp in self.res_evas:
                    if len(behaviors) > 0:
                        self.get_other_infos(ads, tw, tmp, master)
                    if isinstance(behaviors, list):
                        for behavior in behaviors:
                            if isinstance(behavior, dict):
                                behavior['id'] = str(behavior['id']).replace('.0', '')
                                if behavior['id'] in self.res_evas[tmp]['object']:
                                    if ads['spend'] >= 1:
                                        self.res_evas[tmp]['object'][behavior['id']][2*tw] += master
                                    if ads['pay'] > 0:
                                        self.res_evas[tmp]['object'][behavior['id']][2*tw+1] += master
                            elif isinstance(behavior, str) or isinstance(behavior, float):
                                behavior = str(behavior).replace('.0', '')
                                if behavior in self.res_evas[tmp]['object']:
                                    if ads['spend'] >= 1:
                                        self.res_evas[tmp]['object'][behavior][2*tw] += master
                                    if ads['pay'] > 0:
                                        self.res_evas[tmp]['object'][behavior][2*tw+1] += master

                    elif isinstance(behaviors, dict):
                        for behavior in behaviors.values():
                            if 'id' in behavior:
                                behavior['id'] = str(behavior['id']).replace('.0', '')
                                if behavior['id'] in self.res_evas[tmp]['object']:
                                    if ads['spend'] >= 1:
                                        self.res_evas[tmp]['object'][behavior['id']][2*tw] += master
                                    if ads['pay'] > 0:
                                        self.res_evas[tmp]['object'][behavior['id']][2*tw+1] += master

    def get_data(self, is_all=True):
        date = datetime.datetime.strptime(self.date, '%Y-%m-%d')
        _, week, days = (date + datetime.timedelta(days=-1)).isocalendar()
        if is_all:
            first_date = (date + datetime.timedelta(days=-(21+days))).strftime('%Y-%m-%d')
        else:
            first_date = (date + datetime.timedelta(days=-days)).strftime('%Y-%m-%d')
        log_maker.logger.info('The date range  %s to %s' % (str(first_date), str(self.date)))
        evas = tdn_mongo.get_evaluation(first_date, self.date)
        log_maker.logger.info('There are %d pieces of data' % (len(evas)))
        mtypes = tdn_mysql.select_method_type()
        for eva in evas:
            if 'pt' in eva and eva['pt']:
                tweek = week - datetime.datetime.strptime(eva['report_date'], '%Y-%m-%d').isocalendar()[1]
                if tweek < 0:
                    tweek += 52
                if tweek > 0:
                    master = 1.0/7.01
                else:
                    master = 1.0/(days+0.01)
                if ('name' in eva['pt'] and 'SEED' in eva['pt']['name'].upper()) or ('adset_spec' in eva['pt'] and 'targeting' in eva['pt']['adset_spec'] and 'custom_audiences' in eva['pt']['adset_spec']['targeting'] and len(eva['pt']['adset_spec']['targeting']['custom_audiences']) > 0):
                    self.get_audience_infos(eva, tweek, 'seed', mtypes, master)
                else:
                    self.get_pt_infos(eva, tweek, 'random', master)

    def struct_blank_creator(self):
        names = ['random@interest', 'random@behavior', 'seed@customer_file', 'seed@app_activity', 'seed@value_based_lookalike']
        for name in names:
            if name not in self.res_evas:
                mst = name.split('@')
                if len(mst) == 2:
                    self.res_evas[name] = self.struct_blank_tdn(mst[0], mst[1])

    def insert_mongo_data(self):
            tdn_mongo.insert_tdn_base(self.res_evas.values())

    def main_entry(self):
        t1 = time.time()
        log_maker.logger.info('Entry into tdsn_maker')
        try:
            self.struct_blank_creator()
            self.get_data(is_all=True)
            log_maker.logger.info('%s different methods want to updating' % (len(self.res_evas)))
            self.insert_mongo_data()
            print(time.time()-t1)
            log_maker.logger.info('It cost %s seconds...' % (str(round(time.time()-t1, 3))))
            return {'status': '1'}
        except Exception as e:
            log_maker.logger.info(str(e))
            return {'status': '0'}

