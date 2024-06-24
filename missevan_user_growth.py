import csv
import datetime
import logging
import time
import xml.etree.ElementTree as ETree
import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "https://www.missevan.com"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9'
}

def get_drama_sound_lists(drama_id):
    url = f"{BASE_URL}/dramaapi/getdrama?drama_id={drama_id}"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json().get("info", {})
        drama = data.get('drama', {})
        episodes = data.get("episodes", {}).get("episode", [])

        sound_lists = [{
            "sound_id": episode["sound_id"],
            "sound_title": episode["soundstr"],
            'need_pay': episode.get("need_pay", 0)
        } for episode in episodes]

        return sound_lists, drama.get('name'), drama.get('price'), drama.get('view_count'), drama.get('catalog_name')
    except requests.RequestException as e:
        logging.error(f"Error fetching sound lists for drama ID {drama_id}: {e}")
        return [], '', '', '', ''


def get_sound_detail(sound_id):
    url = f"{BASE_URL}/sound/getsound?soundid={sound_id}"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        sound = response.json().get("info", {}).get("sound", {})

        return {
            "sound_id": sound_id,
            "view_count": sound.get("view_count"),
            "view_count_formatted": sound.get("view_count_formatted"),
            "comment_count": sound.get("comment_count"),
            "favorite_count": sound.get("favorite_count"),
            "username": sound.get("username"),
            "create_time": datetime.datetime.fromtimestamp(sound.get('create_time', 0)) if sound.get('create_time', 0) > 0 else None,
        }
    except requests.RequestException as e:
        logging.error(f"Error fetching sound detail for sound ID {sound_id}: {e}")
        return {}


def fetch_all_danmakus(sound_id):
    url = f"{BASE_URL}/sound/getdm?soundid={sound_id}"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        pp_comments_xml = ETree.fromstring(response.text)
        return {int(item.attrib["p"].split(",")[6]) for item in pp_comments_xml.findall("d") if
                item.attrib["p"].split(",")[1] != "4"}
    except (requests.RequestException, ETree.ParseError) as e:
        logging.error(f"Error fetching popup comments for sound ID {sound_id}: {e}")
        return set()


def extract_user_ids(data):
    user_ids = {int(comment["userid"]) for comment in data["info"]["comment"]["Datas"]}
    user_ids.update(
        int(sub["userid"]) for comment in data["info"]["comment"]["Datas"] for sub in comment["subcomments"])
    return user_ids


def fetch_all_uids_by_comments(sound_id):
    endpoint = f"{BASE_URL}/site/getcomment?type=1&e_id={sound_id}&order=3&p={{}}&pagesize=100"
    comments_uids = set()
    page = 1

    while True:
        response = requests.get(endpoint.format(page), headers=headers)
        response.raise_for_status()
        data = response.json()
        comments_uids.update(extract_user_ids(data))

        if not data["info"]["comment"]["hasMore"]:
            break
        page += 1

    return comments_uids


def get_user_input():
    return input("Enter the drama ids (separate with commas, e.g, 62452,68690,72732,74464,74005,68204,74309,52382): ")


def process_sound(sound):
    sound_id = sound.get('sound_id')
    sound_detail = get_sound_detail(sound_id)
    danmaku_uids = fetch_all_danmakus(sound_id)
    comment_uids = fetch_all_uids_by_comments(sound_id)

    sound_detail.update({
        'sound_id': sound_id,
        'sound_title': sound.get('sound_title'),
        'need_pay': sound.get('need_pay'),
        'danmaku_uids': danmaku_uids,
        'comment_uids': comment_uids,
        'total_sound_uids': danmaku_uids.union(comment_uids),
    })

    return sound_detail


def get_top_50_coin(drama_id):
    try:
        total_coin = 0
        url = f"{BASE_URL}/reward/user-reward-rank?drama_id={drama_id}&period=3"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            rewards = response.json().get("info").get("data")
            if rewards:
                for reward in rewards:
                    total_coin += (int(reward.get('coin')) if reward.get('coin') is not None else 0)

        return total_coin
    except:
        return 0


def process_drama_id(drama_id, sound_writer, drama_writer, previous_paid_uids):

    logging.info(f"Processing drama: (ID: {drama_id})")
    sound_lists, name, price, view_count, catalog_name = get_drama_sound_lists(drama_id)

    fetch_top_50_coin = get_top_50_coin(drama_id)

    sound_data = []
    total_paid_udis = set()
    total_free_udis = set()

    total_paid_danmaku_udis = set()
    total_paid_comment_uids = set()
    total_free_danmaku_udis = set()
    total_free_comment_uids = set()

    paid_view_count = 0
    free_view_count = 0
    first_sound_create_time = None

    if sound_lists:
        for sound in sound_lists:
            sound_detail = process_sound(sound)
            if sound_detail['create_time'] is not None and (first_sound_create_time is None or sound_detail['create_time'] < first_sound_create_time):
                first_sound_create_time = sound_detail['create_time']
            if sound_detail:
                if sound.get('need_pay') > 0:
                    total_paid_udis.update(sound_detail['total_sound_uids'])
                    paid_view_count += (int(sound_detail['view_count']) if sound_detail['view_count'] is not None else 0)

                    total_paid_danmaku_udis.update(sound_detail['danmaku_uids'])
                    total_paid_comment_uids.update(sound_detail['comment_uids'])
                else:
                    total_free_udis.update(sound_detail['total_sound_uids'])
                    total_free_danmaku_udis.update(sound_detail['danmaku_uids'])
                    total_free_comment_uids.update(sound_detail['comment_uids'])
                    free_view_count += (int(sound_detail['view_count']) if sound_detail['view_count'] is not None else 0)

                sound_data.append(sound_detail)
                print(sound_detail['sound_title'], sound_detail['create_time'], sound_detail['need_pay'],
                      len(sound_detail['danmaku_uids']), len(sound_detail['comment_uids']),
                      len(sound_detail['total_sound_uids']), sound_detail['view_count'])

    # Calculate the growth in paid user IDs
    new_paid_uids = total_paid_udis.difference(previous_paid_uids)
    paid_uids_growth = len(new_paid_uids)

    # Order sound_data by sound_id
    sound_data.sort(key=lambda x: x['sound_id'])

    for sound_detail in sound_data:
        sound_writer.writerow([
            sound_detail['sound_title'], sound_detail['create_time'], ('PAID' if int(sound_detail['need_pay']) > 0 else 'FREE'),
            len(sound_detail['danmaku_uids']), len(sound_detail['comment_uids']),
            len(sound_detail['total_sound_uids']), sound_detail['view_count']
        ])

    # Add two new rows after the sound data
    sound_writer.writerow(['End of data for drama ID', drama_id, '', '', '', '', ''])
    sound_writer.writerow(['', '', '', '', '', '', ''])
    sound_writer.writerow(['', '', '', '', '', '', ''])

    drama_writer.writerow([
        drama_id, name, first_sound_create_time, price, view_count, paid_view_count, free_view_count,
        len(total_paid_danmaku_udis), len(total_paid_comment_uids), len(total_free_danmaku_udis),
        len(total_free_comment_uids), len(total_paid_udis), len(total_free_udis), fetch_top_50_coin, paid_uids_growth
    ])

    return sound_data, total_paid_udis


def runner():
    drama_ids = get_user_input()
    drama_sound = {}
    all_paid_total_uids = set()
    previous_paid_uids = set()

    with open(f"{datetime.date.today()}_sound_data.csv", mode='a', newline='', encoding='utf-8') as sound_file, \
            open(f"{datetime.date.today()}_drama_data.csv", mode='a', newline='', encoding='utf-8') as drama_file:
        sound_writer = csv.writer(sound_file)
        drama_writer = csv.writer(drama_file)

        # Check if the file is empty before writing headers
        sound_file_empty = sound_file.tell() == 0
        drama_file_empty = drama_file.tell() == 0

        if sound_file_empty:
            sound_writer.writerow(["声音标题", "创建时间", "是否需要付费", "弹幕用户ID", "评论用户ID", "总用户ID", "观看次数"])

        if drama_file_empty:
            drama_writer.writerow(
                ["剧集ID", "剧集名称", "首个声音创建时间", "价格", "总观看次数", "付费观看次数", "免费观看次数",
                 "付费弹幕用户ID", "付费评论用户ID", "免费弹幕用户ID", "免费评论用户ID",
                 "付费总用户ID", "免费总用户ID", "前五十打赏", "新增付费用户增长"])

        for drama_id in drama_ids.split(','):
            sound_data, total_paid_udis = process_drama_id(drama_id.strip(), sound_writer, drama_writer, previous_paid_uids)
            drama_sound[drama_id] = sound_data
            all_paid_total_uids.update(total_paid_udis)
            previous_paid_uids = total_paid_udis

            print("--------------------- Taking a break -------------------------")
            time.sleep(60)

    print('-------------------------------------------------')
    print(f"All Paid Total UIDs: {len(all_paid_total_uids)}")
    print('-------------------------------------------------')
    return drama_sound, all_paid_total_uids


if __name__ == '__main__':
    runner()
