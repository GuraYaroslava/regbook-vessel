from urllib.request import urlopen
import re
from bs4 import BeautifulSoup

filters = [
    { 'code': 'gorodRegbook', 'columns': [ 'name', 'name_eng', 'country_ru' ] },
    { 'code': 'countryId', 'columns': [ 'name', 'name_eng' ] },
    { 'code': 'statgr', 'columns': [ 'code', 'name', 'name_eng' ] },
    { 'code': 'icecat', 'columns': [ 'name' ] }
]

def parse_filter(filter):
  url = 'https://lk.rs-class.org/regbook/getDictionary2?d={filter_code}&f=formfield'.format(filter_code=filter['code'])
  page = urlopen(url)
  html = page.read().decode('utf-8')
  soup = BeautifulSoup(html, 'html.parser')

  tr_index = 0
  trs = soup.find_all('tr')
  for tr in trs:
      tds = tr.find_all('td')
      fields = { 'id': 0 }

      td_index = 0
      for td in tds:
          a = td.find('a')
          a_onclick = a.get('onclick')
          matches = re.findall(r'[\,\']?([0-9A-Za-zА-Яа-я\-\s]+)[\,\']?', a_onclick, flags=re.U)

          if fields['id'] == 0:
              fields['id'] = matches[3]

          if filter['columns'][td_index] not in fields.keys():
              fields[filter['columns'][td_index]] = a.text

          td_index += 1

      print('#', tr_index, fields)
      tr_index += 1

# for filter in filters:
#     parse_filter(filter)

# parse_filter({ 'code': 'countryId', 'columns': [ 'name', 'name_eng' ] })
# parse_filter({ 'code': 'gorodRegbook', 'columns': [ 'name', 'name_eng', 'country_ru' ] })
# parse_filter({ 'code': 'statgr', 'columns': [ 'code', 'name', 'name_eng' ] })
# parse_filter({ 'code': 'icecat', 'columns': [ 'name' ] })

