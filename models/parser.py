from models.logger import Logger
from models.card_parser import Card_Parser
from urllib.request import urlopen
from bs4 import BeautifulSoup
from threading import Thread
import datetime
import requests
import json
import re

class Parser:
    # Спарсить все карточки, удовлетворяющие условиям соответствующих фильтров
    # filters [ [
    #   name,   # название фильтра для составление запроса
    #   value,  # значение фильтра
    #   field   # название поля под фильтр в таблице связей cards_filters
    # ], ]
    def parse(self, filters, level=1, thead=-1):
        data = {}
        for filter in filters: data[filter['name']] = filter['value']

        url = 'https://lk.rs-class.org/regbook/regbookVessel'
        headers = { 'Content-Type': 'application/x-www-form-urlencoded' }
        response = requests.post(url, data=data, headers=headers)
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')

        tr_index = 0
        for tr in soup.find_all('tr'):
            tds = tr.find_all('td')
            tr_length = len(tds)
            if tr_length == 0: continue
            last_column = tds[tr_length-1]

            with_status = False; identifier = None
            links = last_column.find_all('a')
            for link in links:
                href = link.get('href')
                matches = re.findall(r'(vessel|status)\?fleet_id=(\d+)', href)
                action = matches[0][0]
                identifier = matches[0][1]
                with_status = with_status or (action == 'status')
                if action != 'status' and action != 'vessel':
                    print('ЕСТЬ ЕЩЕ ССЫЛКА ДЛЯ ПАРСИНГА', href)

            card_time_start = datetime.datetime.now()
            Logger().print_start_status(f'[{tr_index+1}] Карточка #{identifier}', level)
            Card_Parser(identifier).parse(filters, with_status)
            Logger().print_end_status(card_time_start, level)
            tr_index += 1

        return

    # ----------------------------------------------------------------------------------------------------------------------

    def sequential(self, trs, thead, filters, level):
        thead_time_start = datetime.datetime.now()
        thead_caption = f'[thead #{thead}][{len(trs)} шт]'
        for tr in trs:
            tds = tr.find_all('td')
            tr_length = len(tds)
            if tr_length == 0: continue
            last_column = tds[tr_length-1]

            with_status = False; identifier = None
            links = last_column.find_all('a')
            for link in links:
                href = link.get('href')
                matches = re.findall(r'(vessel|status)\?fleet_id=(\d+)', href)
                action = matches[0][0]
                identifier = matches[0][1]
                with_status = with_status or (action == 'status')
                if action != 'status' and action != 'vessel':
                    print('ЕСТЬ ЕЩЕ ССЫЛКА ДЛЯ ПАРСИНГА', href)

            card_time_start = datetime.datetime.now()
            caption = f'[thead #{thead}] Карточка #{identifier}'
            Card_Parser(identifier).parse(filters, with_status)
            Logger().print_end_status(card_time_start, level, caption)
        Logger().print_end_status(thead_time_start, level+1, thead_caption)

        return

    # Спарсить все карточки, удовлетворяющие условиям соответствующих фильтров
    # filters [ [
    #   name,   # название фильтра для составление запроса
    #   value,  # значение фильтра
    #   field   # название поля под фильтр в таблице связей cards_filters
    # ], ]
    def parse_with_threads(self, filters, level=1, force=False):
        data = {}
        for filter in filters: data[filter['name']] = filter['value']

        url = 'https://lk.rs-class.org/regbook/regbookVessel'
        headers = { 'Content-Type': 'application/x-www-form-urlencoded' }
        response = requests.post(url, data=data, headers=headers)
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')

        trs = soup.find_all('tr'); n_proc = 10; n_trs = len(trs)
        n = int(n_trs / n_proc) if n_trs % n_proc == 0 else int(n_trs // n_proc + 1)
        if n_trs > 0:
#             Logger().print_start_status(', '.join(map(lambda x: x['name'] + ':' + x['value'], filters)), level)
            Logger().print_start_status(f'Max кол-во потоков: {n_proc} | Кол-во карточек: {n_trs} | Кол-во карточек на поток: {n}', level)

        if force == True:
            threads = []; thead_index = 1
            for trs_chunk in [trs[i:i+n] for i in range(0, n_trs, n)]:
                t = Thread(target=self.sequential, args=(trs_chunk, thead_index, filters, level)); thead_index += 1
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

        return n_trs