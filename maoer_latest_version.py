import xml.etree.ElementTree as ETree
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import logging
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def fetch_all_danmakus(sound_id):
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


def process_sound(sound, m_ids, popup_ids_set, main_ids_set):
    try:
        sound_details = get_sound_detail(sound.get('sound_id'))
        u_m_ids = set()

        # Fetch popup comments
        danmaku_uids = fetch_all_danmakus(sound.get('sound_id'))
        popup_ids_set.update(danmaku_uids)
        for u_id in danmaku_uids:
            m_ids.add(int(u_id))
            u_m_ids.add(int(u_id))

        # Fetch main comments
        main_comment_uids = fetch_all_uids_by_comments(sound.get('sound_id'))
        main_ids_set.update(main_comment_uids)
        for c_u_id in main_comment_uids:
            m_ids.add(int(c_u_id))
            u_m_ids.add(int(c_u_id))

        print(f"{sound.get('sound_title')}, IDs: {len(u_m_ids)}.")
    except Exception as e:
        logging.error(f"Error processing sound {sound.get('sound_title')}: {e}")


def runner():
    drama_ids = input("Enter the drama ids (separate with commas, e.g, 64911,68837): ").split(',')

    results = {}
    all_drama_user_ids = {}

    for drama_id in drama_ids:
        logging.info(f"Processing drama: (ID: {drama_id})")

        total_m_ids = set()  # Using a set to ensure unique IDs
        danmaku_total_ids = set()
        main_comment_total_ids = set()
        reward_total_ids = set()

        reward_uids = fetch_top_50_reward(drama_id)
        reward_total_ids.update(int(reward_uid) for reward_uid in reward_uids)
        total_m_ids.update(reward_total_ids)

        sound_lists = get_drama_sound_lists(drama_id)
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(process_sound, sound, total_m_ids, danmaku_total_ids, main_comment_total_ids) for
                sound in sound_lists]
            for future in as_completed(futures):
                future.result()

        all_drama_user_ids[drama_id] = total_m_ids

    all_unique_user_ids = set.union(*all_drama_user_ids.values())

    for drama_id, user_ids in all_drama_user_ids.items():
        other_drama_user_ids = all_unique_user_ids - user_ids

        only_in_danmaku = danmaku_total_ids - main_comment_total_ids - reward_total_ids - other_drama_user_ids
        only_in_comments = main_comment_total_ids - danmaku_total_ids - reward_total_ids - other_drama_user_ids
        in_both = (danmaku_total_ids & main_comment_total_ids) - other_drama_user_ids
        only_in_rewards = reward_total_ids - danmaku_total_ids - main_comment_total_ids - other_drama_user_ids

        unique_total_ids_in_drama = only_in_danmaku | only_in_comments | in_both | only_in_rewards

        results[drama_id] = {
            "total_ids": len(user_ids),
            # "only_in_danmaku": len(only_in_danmaku),
            # "only_in_comments": len(only_in_comments),
            # "in_both": len(in_both),
            # "only_in_rewards": len(only_in_rewards),
            # "unique_total_ids_in_drama": len(unique_total_ids_in_drama)
        }

        # logging.info(f"Results for drama ID {drama_id}: {results[drama_id]}")

    total_unique_user_ids_across_dramas = len(all_unique_user_ids)

    # Convert results to DataFrame for better visualization
    df_results = pd.DataFrame.from_dict(results, orient='index')
    df_results["drama_id"] = df_results.index
    df_results = df_results.reset_index(drop=True)
    # df_results["total_unique_user_ids_across_dramas"] = total_unique_user_ids_across_dramas

    return df_results


if __name__ == '__main__':
    results_df = runner()
    print(results_df.to_string(index=False))
