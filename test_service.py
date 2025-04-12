import logging
import requests

# Настройка логирования
file_log = logging.FileHandler("test_service.log", mode="w")
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(file_log)

# URL сервиса рекомендаций
recommendation_url = 'http://127.0.0.1:8000'

# Стандартные заголовки для запросов
headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

# Сценарий 1: Пользователь без персональных рекомендаций
def test_user_no_personal_recs():
    logger.info("Testing user without personal recommendations")
    params = {"user_id": 1, "k": 3}
    
    response = requests.post(recommendation_url + "/recommendations", params=params, headers=headers)
    if response.status_code == 200:
        data = response.json()
        logger.info(f"Success: got {len(data['recs'])} recommendations for user without personalized recs")
        logger.info(f"Response: {data}")
    else:
        logger.error(f"Failed: status code {response.status_code}")

# Сценарий 2: Пользователь с персональными рекомендациями
def test_user_with_personal_recs():
    logger.info("Testing user with personal recommendations")
    params = {"user_id": 2, "k": 3}
    
    response = requests.post(recommendation_url + "/recommendations", params=params, headers=headers)
    if response.status_code == 200:
        data = response.json()
        logger.info(f"Success: got {len(data['recs'])} recommendations for user with personalized recs")
        logger.info(f"Response: {data}")
    else:
        logger.error(f"Failed: status code {response.status_code}")

# Сценарий 3: Пользователь с онлайн и оффлайн рекомендациями
def test_user_online_and_offline_recs():
    logger.info("Testing user with online and offline recommendations")
    params = {"user_id": 3, "k": 3}
    
    response = requests.post(recommendation_url + "/recommendations", params=params, headers=headers)
    if response.status_code == 200:
        data = response.json()
        logger.info(f"Success: got {len(data['recs'])} recommendations for user with online and offline recs")
        logger.info(f"Response: {data}")
    else:
        logger.error(f"Failed: status code {response.status_code}")

# Запуск всех тестов
if __name__ == "__main__":
    logger.info("Starting test session")
    
    test_user_no_personal_recs()
    test_user_with_personal_recs()
    test_user_online_and_offline_recs()
    
    logger.info("Ending test session")
    file_log.close()