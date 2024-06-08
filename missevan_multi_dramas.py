import xml.etree.ElementTree as ETree
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def fetch_all_popup_comments(sound_id):
    try:
        pp_comments_url = f"https://www.missevan.com/sound/getdm?soundid={sound_id}"
        pp_reponse_txt = requests.get(pp_comments_url).text
        pp_comments_xml = ETree.fromstring(pp_reponse_txt)
        uids = set()

        for items in pp_comments_xml.findall("d"):
            _stime, _mode, _size, _color, _date, _class, _uid, _dmid = items.attrib["p"].split(",")
            if _mode != "4":
                uids.add(int(_uid))

        return uids
    except Exception as e:
        logging.error(f"Error fetching popup comments for sound ID {sound_id}: {e}")
        return set()


def get_drama_sound_lists(drama_id):
    try:
        sound_lists = []
        url = f"https://www.missevan.com/dramaapi/getdrama?drama_id={drama_id}"
        response = requests.get(url)
        if response.status_code == 200:
            episodes = response.json().get("info", {}).get("episodes", {}).get("episode", [])
            for episode in episodes:
                if episode.get("need_pay", 0) > 0:
                    sound_lists.append({
                        "sound_id": episode["sound_id"],
                        "sound_title": episode["soundstr"],
                    })
        return sound_lists
    except Exception as e:
        logging.error(f"Error fetching sound lists for drama ID {drama_id}: {e}")
        return []


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
    try:
        url = f"https://www.missevan.com/sound/getsound?soundid={sound_id}"
        response = requests.get(url)
        if response.status_code == 200:
            sound = response.json().get("info", {}).get("sound", {})
            return {"view_count": sound.get("view_count")}
        return {}
    except Exception as e:
        logging.error(f"Error fetching sound detail for sound ID {sound_id}: {e}")
        return {}


def fetch_top_50_reward(drama_id):
    try:
        reward_ids = set()
        url = f"https://www.missevan.com/reward/user-reward-rank?drama_id={drama_id}&period=3"
        response = requests.get(url)
        if response.status_code == 200:
            rewards = response.json().get("info", {}).get("data", [])
            for reward in rewards:
                reward_ids.add(int(reward["id"]))
        return reward_ids
    except Exception as e:
        logging.error(f"Error fetching top 50 reward for drama ID {drama_id}: {e}")
        return set()


def fetch_drama_sound_by_search(search_name):
    try:
        url = "https://www.missevan.com/dramaapi/search"
        params = {"s": search_name, "page": 1}
        drama_ids = set()

        while True:
            response = requests.get(url, params=params)
            response.raise_for_status()
            dramas = response.json()["info"]["Datas"]
            for drama in dramas:
                if drama["pay_type"] > 0:
                    drama_ids.add((drama["id"], drama["name"]))

            pagination = response.json()["info"]["pagination"]
            if pagination["p"] >= pagination["maxpage"]:
                break
            params["page"] += 1

        return drama_ids
    except Exception as e:
        logging.error(f"Error searching for drama by name {search_name}: {e}")
        return set()


def process_sound(sound, m_ids):
    try:
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

        # logging.info(f"Loaded IDs -- {sound.get('sound_title')}, IDs: {len(u_m_ids)}, total view count: {sound_details.get('view_count')}")
        logging.info(f"Loaded IDs -- {sound.get('sound_title')}, IDs: {len(u_m_ids)}.")
    except Exception as e:
        logging.error(f"Error processing sound {sound.get('sound_title')}: {e}")


def runner():
    drama_ids = input("Enter the drama ids (separate with commas, e.g, 64911,68837): ")

    total_m_ids = set()  # Using a set to ensure unique IDs
    drama_user_counts = set()

    for drama_id in drama_ids.split(','):
        logging.info(f"Processing drama: (ID: {drama_id})")

        drama_m_ids = set()

        reward_uids = fetch_top_50_reward(drama_id)
        drama_m_ids.update(int(reward_uid) for reward_uid in reward_uids)

        sound_lists = get_drama_sound_lists(drama_id)
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(process_sound, sound, drama_m_ids) for sound in sound_lists]
            for future in as_completed(futures):
                future.result()

        total_m_ids.update(drama_m_ids)
        drama_user_counts.update(drama_m_ids)

        logging.info(f"Total count of unique user IDs for drama: {len(drama_user_counts)}")

    logging.info(f"Total count of unique user IDs across all dramas: {len(total_m_ids)}")
    return total_m_ids


if __name__ == '__main__':
    total_m_ids = runner()
    print(f"Total unique user IDs of: {len(total_m_ids)}")
