import requests
from bs4 import BeautifulSoup
import pandas as pd


def get_novel_details_by_novel_id(novelid):
    url = f"https://www.jjwxc.net/onebook.php?novelid={novelid}"
    response = requests.get(url)
    response.encoding = 'gb18030'
    soup = BeautifulSoup(response.text, 'html.parser')

    data = {
        "总书评数": None,
        "当前被收藏数": None,
        "营养液数": None
    }

    tables = soup.find_all('table')
    if len(tables) >= 3:
        last_tr = tables[1].find_all('tr')[-1]
        spans = last_tr.find_all('span')
        if len(spans) >= 4:
            data["总书评数"] = spans[1].get_text(strip=True)
            data["当前被收藏数"] = spans[2].get_text(strip=True)
            data["营养液数"] = spans[3].get_text(strip=True)

    return data


def check_novel_in_purchased(novel_name, purchased_df):
    return 'Yes' if novel_name in purchased_df['name'].astype(str).values else 'No'


def get_novel_rows(third_table, purchased_df):
    rows = []
    for tr in third_table.find_all('tr')[1:]:
        cells = tr.find_all(['td', 'th'])
        # row = [cell.get_text(strip=True).replace(',', '') if i == 6 else cell.get_text(strip=True) for i, cell in enumerate(cells)]
        row = []
        for i, cell in enumerate(cells):
            if i != 3:
                text = cell.get_text(strip=True)
                if i == 6:
                    text = text.replace(',', '')  # Remove comma if index is 6
                row.append(text)

        novelid = None
        for a in tr.find_all('a', href=True):
            if 'onebook.php?novelid=' in a['href']:
                novelid = a['href'].split('novelid=')[-1]
                break

        name = row[2]
        if novelid:
            novel_details = get_novel_details_by_novel_id(novelid)
            row.append(novelid)
            row.append(check_novel_in_purchased(name, purchased_df))
            row.extend([
                novel_details.get('总书评数'),
                novel_details.get('当前被收藏数'),
                novel_details.get('营养液数')
            ])

        rows.append(row)
    return rows


def runner():
    purchased_df = pd.read_csv('purchased.csv')
    url = "https://www.jjwxc.net/topten.php?orderstr=7&t=1"
    response = requests.get(url)
    response.encoding = 'gb18030'
    soup = BeautifulSoup(response.text, 'html.parser')

    tables = soup.find_all('table')
    if len(tables) >= 3:
        third_table = tables[2]
        headers = ['序号', '作者', '作品', '进度', '字数', '作品积分', '发表时间', 'NovelID', 'Drama', '总书评数', '当前被收藏数', '营养液数']
        rows = get_novel_rows(third_table, purchased_df)

        df = pd.DataFrame(rows, columns=headers)
        df.to_csv('jjwxc_200.csv', index=False)
    else:
        print("The third table was not found on the webpage.")


if __name__ == '__main__':
    runner()
