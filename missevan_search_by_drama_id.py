import xml.etree.ElementTree as ETree
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests


def fetch_all_popup_comments(sound_id):
    pp_comments_url = f"https://www.missevan.com/sound/getdm?soundid={sound_id}"
    pp_reponse_txt = requests.get(pp_comments_url).text
    pp_comments_xml = ETree.fromstring(pp_reponse_txt)
    uids = set()

    for items in pp_comments_xml.findall("d"):
        _stime, _mode, _size, _color, _date, _class, _uid, _dmid = items.attrib["p"].split(",")
        if _mode != "4":
            uids.add(_uid)

    return uids


def get_drama_sound_lists(drama_id):
    sound_lists = []
    url = f"https://www.missevan.com/dramaapi/getdrama?drama_id={drama_id}"
    response = requests.get(url)
    if response.status_code == 200:
        episodes = response.json().get("info").get("episodes").get("episode")
        if episodes:
            for episode in episodes:
                if episode["need_pay"] > 0:
                    sound_lists.append({
                    "sound_id": episode["sound_id"],
                    "sound_title": episode["soundstr"],
                })

    return sound_lists


def extract_user_ids(data):
    user_ids = set()
    for comment_data in data["info"]["comment"]["Datas"]:
        user_ids.add(int(comment_data["userid"]))
        for subcomment in comment_data["subcomments"]:
            user_ids.add(int(subcomment["userid"]))

    return user_ids


def fetch_all_uids_by_comments(sound_id):

    endpoint = "https://www.missevan.com/site/getcomment?type=1&e_id={}&order=3&p={}&pagesize=100"
    all_user_ids = set()
    page = 1

    while True:
        response = requests.get(endpoint.format(sound_id, page))
        data = response.json()
        user_ids = extract_user_ids(data)
        all_user_ids.update(user_ids)
        if not data["info"]["comment"]["hasMore"]:
            break
        page += 1

    return all_user_ids


def get_sound_detail(sound_id):
    sound_lists = {}
    url = f"https://www.missevan.com/sound/getsound?soundid={sound_id}"
    response = requests.get(url)
    if response.status_code == 200:
        sound = response.json().get("info").get("sound")
        if sound:
            sound_lists = {
                "view_count": sound.get("view_count"),
            }

    return sound_lists


def fetch_top_50_reward(drama_id):
    reward_ids = set()
    url = f"https://www.missevan.com/reward/user-reward-rank?drama_id={drama_id}&period=3"
    response = requests.get(url)
    if response.status_code == 200:
        rewards = response.json().get("info").get("data")
        if rewards:
            for reward in rewards:
                reward_ids.add(reward["id"])

    return reward_ids


def runner():
    drama_id = input("Enter the MaoerFM Drama ID (e.g., 73214 from https://www.missevan.com/mdrama/73214): ")
    sound_lists = get_drama_sound_lists(drama_id)

    m_ids = set()
    reward_uids = fetch_top_50_reward(drama_id)
    for reward_uid in reward_uids:
        m_ids.add(int(reward_uid))

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for sound in sound_lists:
            futures.append(executor.submit(process_sound, sound, m_ids))
        for future in as_completed(futures):
            future.result()

    print(f"Total count of paid episode IDs: {len(m_ids)}")


def process_sound(sound, m_ids):
    sound_details = get_sound_detail(sound.get('sound_id'))
    u_m_ids = set()
    popup_comment_uids = fetch_all_popup_comments(sound.get('sound_id'))
    for u_id in popup_comment_uids:
        m_ids.add(int(u_id))
        u_m_ids.add(int(u_id))

    main_comment_uids = fetch_all_uids_by_comments(sound.get('sound_id'))
    for c_u_id in main_comment_uids:
        m_ids.add(int(c_u_id))
        u_m_ids.add(int(c_u_id))

    print(f"Loading the ids -- {sound.get('sound_title')}, ids: {len(u_m_ids)}, total_view_count: {sound_details.get('view_count')}")


if __name__ == '__main__':
    runner()
