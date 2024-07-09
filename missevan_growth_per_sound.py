import csv
import datetime
import logging
import time
import xml.etree.ElementTree as ETree
from typing import Dict, Optional, List, Set, Tuple

import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "https://www.missevan.com"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9'
}
# SoundTianGuanXianMian = ['8321733', '8326714', '8331496', '8336360', '8341274']
# SoundTianGuanXianMian = []
# start_date = datetime.datetime(2023,8,24,18,0,0)
# end_date = datetime.datetime(2023,8,29,18,0,0)

SoundTianGuanXianMian = ['9648138']
start_date = datetime.datetime(2024,6,26,18,0,0)
end_date = datetime.datetime(2024,7,3,18,0,0)


def measure_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logging.info(f"{func.__name__} took {end_time - start_time:.4f} seconds")
        return result
    return wrapper


@measure_time
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
        sound_lists.sort(key=lambda x: x['sound_id'])
        return sound_lists, drama.get('name'), drama.get('price'), drama.get('view_count'), drama.get('catalog_name')
    except requests.RequestException as e:
        logging.error(f"Error fetching sound lists for drama ID {drama_id}: {e}")
        return [], '', '', '', ''


@measure_time
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


@measure_time
def fetch_all_danmakus(sound_id: int) -> Set[int]:
    url = f"{BASE_URL}/sound/getdm?soundid={sound_id}"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return parse_danmakus(response.content, sound_id)
    except requests.RequestException as request_error:
        logging.error(f"Error fetching popup comments for sound ID {sound_id}: {request_error}")

    return set()


def parse_danmakus(xml_data: bytes, sound_id: int) -> Set[int]:
    pp_comments_xml = ETree.fromstring(xml_data)
    danmakus = set()

    for item in pp_comments_xml.findall("d"):
        attributes = item.attrib["p"].split(",")
        danmaku_type = attributes[1]
        danmaku_id = int(attributes[6])

        if danmaku_type != "4" and not should_skip_danmaku(attributes, sound_id):
            danmakus.add(danmaku_id)

    return danmakus


def should_skip_danmaku(attributes: list, sound_id: int) -> bool:
    if SoundTianGuanXianMian and sound_id in SoundTianGuanXianMian:
        date_time = datetime.datetime.fromtimestamp(int(attributes[4]))
        return start_date <= date_time <= end_date
    return False


def extract_user_ids(data, sound_id):
    user_ids = set()
    comments = data["info"]["comment"]["Datas"]

    for comment in comments:
        if SoundTianGuanXianMian and sound_id in SoundTianGuanXianMian:
            comment_time = datetime.datetime.fromtimestamp(comment["ctime"])
            if start_date and end_date and start_date <= comment_time <= end_date:
                continue

            user_ids.add(int(comment["userid"]))

            subcomments = comment["subcomments"]
            for sub in subcomments:
                subcomment_time = datetime.datetime.fromtimestamp(sub["ctime"])
                if start_date and end_date and start_date <= subcomment_time <= end_date:
                    continue

                user_ids.add(int(sub["userid"]))
        else:
            user_ids.add(int(comment["userid"]))
            subcomments = comment["subcomments"]
            user_ids.update(int(sub["userid"]) for sub in subcomments)

    return user_ids


@measure_time
def fetch_all_uids_by_comments(sound_id):
    endpoint = f"{BASE_URL}/site/getcomment?type=1&e_id={sound_id}&order=3&p={{}}&pagesize=100"
    comments_uids = set()
    page = 1

    while True:
        response = requests.get(endpoint.format(page), headers=headers)
        data = response.json()
        if not data:
            break

        comments_uids.update(extract_user_ids(data, sound_id))

        if not data["info"]["comment"]["hasMore"]:
            break
        page += 1

    return comments_uids


def get_user_input():
    return input("Enter the drama ids (separate with commas, e.g, 62452,68690,72732): ")


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


@measure_time
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


def process_sound_detail(sound_detail: Dict, first_sound_create_time: Optional[str]) -> Optional[str]:
    """Process individual sound detail and update the first sound creation time if needed."""
    create_time = sound_detail['create_time']
    if create_time and (first_sound_create_time is None or create_time < first_sound_create_time):
        first_sound_create_time = create_time
    return first_sound_create_time


@measure_time
def update_user_sets(sound_detail: Dict, total_paid_udis: Set, total_free_udis: Set,
                     total_paid_danmaku_udis: Set, total_paid_comment_uids: Set,
                     total_free_danmaku_udis: Set, total_free_comment_uids: Set,
                     paid_view_count: int, free_view_count: int) -> Tuple[int, int]:
    """Update sets of user IDs based on whether the sound is paid or free and update view counts."""
    if sound_detail['need_pay'] > 0:
        total_paid_udis.update(sound_detail['total_sound_uids'])
        paid_view_count += int(sound_detail['view_count']) if sound_detail['view_count'] else 0
        total_paid_danmaku_udis.update(sound_detail['danmaku_uids'])
        total_paid_comment_uids.update(sound_detail['comment_uids'])
    else:
        total_free_udis.update(sound_detail['total_sound_uids'])
        free_view_count += int(sound_detail['view_count']) if sound_detail['view_count'] else 0
        total_free_danmaku_udis.update(sound_detail['danmaku_uids'])
        total_free_comment_uids.update(sound_detail['comment_uids'])
    return paid_view_count, free_view_count


@measure_time
def write_sound_data(drama_id, sound_data: List[Dict], sound_writer, previous_paid_uids: Set) -> None:
    """Write sound data to CSV and calculate new paid user IDs."""
    previous_paid_uids_set = set()
    for sound_detail in sound_data:
        current_paid_uids = sound_detail['total_sound_uids'] if sound_detail['need_pay'] > 0 else set()
        new_paid_uids = current_paid_uids.difference(previous_paid_uids_set)
        previous_paid_uids_set.update(current_paid_uids)
        sound_detail['new_paid_uids'] = len(new_paid_uids)
        sound_writer.writerow([
            sound_detail['sound_title'], sound_detail['create_time'],
            'PAID' if int(sound_detail['need_pay']) > 0 else 'FREE',
            len(sound_detail['danmaku_uids']), len(sound_detail['comment_uids']),
            len(sound_detail['total_sound_uids']), sound_detail['view_count'],
            sound_detail['new_paid_uids']
        ])

    sound_writer.writerow(['End of data for drama ID', drama_id, '', '', '', '', '', ''])
    sound_writer.writerow(['', '', '', '', '', '', '', ''])
    sound_writer.writerow(['', '', '', '', '', '', '', ''])


@measure_time
def process_drama_id(drama_id: str, sound_writer, drama_writer, previous_paid_uids: Set) -> Tuple[List[Dict], Set]:
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
            first_sound_create_time = process_sound_detail(sound_detail, first_sound_create_time)
            paid_view_count, free_view_count = update_user_sets(
                sound_detail, total_paid_udis, total_free_udis,
                total_paid_danmaku_udis, total_paid_comment_uids,
                total_free_danmaku_udis, total_free_comment_uids,
                paid_view_count, free_view_count
            )
            sound_data.append(sound_detail)
            print(sound_detail['sound_title'], sound_detail['create_time'],
                  'PAID' if int(sound_detail['need_pay']) > 0 else 'FREE',
                  len(sound_detail['danmaku_uids']), len(sound_detail['comment_uids']),
                  len(sound_detail['total_sound_uids']), sound_detail['view_count'])

    new_paid_uids = total_paid_udis.difference(previous_paid_uids)
    paid_uids_growth = len(new_paid_uids)

    write_sound_data(drama_id, sound_data, sound_writer, previous_paid_uids)

    drama_writer.writerow([
        drama_id, name, first_sound_create_time, price, view_count, paid_view_count, free_view_count,
        len(total_paid_danmaku_udis), len(total_paid_comment_uids), len(total_free_danmaku_udis),
        len(total_free_comment_uids), len(total_paid_udis), len(total_free_udis), fetch_top_50_coin, paid_uids_growth
    ])

    return sound_data, total_paid_udis


@measure_time
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
            sound_writer.writerow(
                ["声音标题", "创建时间", "是否需要付费", "弹幕用户ID", "评论用户ID", "总用户ID", "观看次数",
                 "每集新增付费ID"])

        if drama_file_empty:
            drama_writer.writerow(
                ["剧集ID", "剧集名称", "首个声音创建时间", "价格", "总观看次数", "付费观看次数", "免费观看次数",
                 "付费弹幕用户ID", "付费评论用户ID", "免费弹幕用户ID", "免费评论用户ID",
                 "付费总用户ID", "免费总用户ID", "前五十打赏", "新增付费用户增长"])

        for drama_id in drama_ids.split(','):
            sound_data, total_paid_udis = process_drama_id(drama_id.strip(), sound_writer, drama_writer,
                                                           previous_paid_uids)
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
