import logging
from typing import List, Dict, Any, Tuple, Optional
from contextlib import asynccontextmanager

import requests
import pandas as pd
from fastapi import FastAPI, HTTPException, status

logger = logging.getLogger("uvicorn.error")

# Конфигурация URL внешних сервисов
FEATURES_STORE_URL = "http://127.0.0.1:8010"
HISTORY_STORE_URL = "http://127.0.0.1:8020"

# Стандартные заголовки для HTTP-запросов
DEFAULT_HEADERS = {"Content-type": "application/json", "Accept": "text/plain"}


class Recommendations:
    def __init__(self):
        self._recs = {}
        self._stats = {
            "request_personal_count": 0,
            "request_default_count": 0
            
        }

    def load(self, rec_type: str, path: str, **kwargs) -> None:
        """
        Загружает рекомендации из parquet файла.
        
        Args:
            rec_type: Тип рекомендаций ('personal' или 'default')
            path: Путь к файлу с рекомендациями
            **kwargs: Дополнительные параметры для pd.read_parquet
        """
        logger.info(f"Loading data, type: {rec_type}")
        try:
            self._recs[rec_type] = pd.read_parquet(path, **kwargs)
            if rec_type == "personal":
                self._recs[rec_type] = self._recs[rec_type].set_index("user_id")
            logger.info(f"Successfully loaded {rec_type} recommendations")
        except Exception as e:
            logger.error(f"Failed to load {rec_type} recommendations: {str(e)}")
            raise

    def get(self, user_id: int, k: int = 100) -> List[int]:
        """
        Получает рекомендации для пользователя.
        
        Args:
            user_id: ID пользователя
            k: Количество рекомендаций
            
        Returns:
            Список ID треков для рекомендации
        """
        try:
            recs = self._recs["personal"].loc[user_id]
            recs = recs["track_id"].to_list()[:k]
            self._stats["request_personal_count"] += 1
            logger.info(f"Retrieved personal recommendations for user_id: {user_id}")
            
        except KeyError:
            logger.info(f"No personal recommendations found for user_id: {user_id}, using default")
            recs = self._recs["default"]
            recs = recs["track_id"].to_list()[:k]
            self._stats["request_default_count"] += 1
        except Exception as e: 
            logger.error(f"Unknown error retrieving recommendations: {str(e)}")
            # Возвращаем пустой список в случае ошибки
            recs = []
        return recs

    def stats(self) -> None:
        """
        Выводит статистику использования рекомендаций в лог.
        """
        logger.info("Stats for recommendations")
        for name, value in self._stats.items():
            logger.info(f"{name:<30} {value}")
            
def dedup_ids(ids: List[int]) -> List[int]:
    """
    Удаляет дубликаты из списка ID, сохраняя порядок первого появления.
    
    Args:
        ids: Список ID с возможными дубликатами
        
    Returns:
        Список уникальных ID с сохранением порядка первого появления
    """
    seen = set()
    result = []
    for id in ids:
        if id not in seen:
            seen.add(id)
            result.append(id)
    return result

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Инициализирует сервис при запуске и выполняет завершающие действия при остановке.
    
    Args:
        app: Экземпляр FastAPI приложения
    """
    logger.info("Starting recommendations service")
    try:
        rec_store = Recommendations()
        rec_store.load(
            "personal",
            "recsys/recommendations/recommendations.parquet",  # путь к файлу с персональными рекомендациями
            columns=["user_id", "track_id", "track_seq"],
        )
        rec_store.load(
            "default",
            "recsys/recommendations/top_popular.parquet",  # путь к файлу с дефолтными рекомендациями
            columns=["track_id", "track_seq"],
        )
        app.state.recs = rec_store
        logger.info("Recommendations service initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize recommendations service: {str(e)}")
        raise
    
    yield
    
    logger.info("Stopping recommendations service")
    app.state.recs.stats()

# Создаём приложение FastAPI
app = FastAPI(
    title="Recommendations Service",
    description="API для получения рекомендаций музыкальных треков",
    lifespan=lifespan
)

@app.post("/recommendations_offline", response_model=Dict[str, List[int]])
async def recommendations_offline(user_id: int, k: int = 100) -> Dict[str, List[int]]:
    """
    Получает офлайн рекомендации для пользователя.
    
    Args:
        user_id: ID пользователя
        k: Количество рекомендаций
        
    Returns:
        Словарь с ключом 'recs' и списком ID треков для рекомендации
    """
    logger.info(f"Getting offline recommendations for user_id: {user_id}, k: {k}")
    recs = app.state.recs.get(user_id, k)
    return {"recs": recs}

def dedup_ids(ids):
    seen = set()
    ids = [id for id in ids if not (id in seen or seen.add(id))]
    return ids

@app.post("/recommendations_online", response_model=Dict[str, List[int]])
async def recommendations_online(user_id: int, k: int = 100) -> Dict[str, List[int]]:
    """
    Получает онлайн рекомендации для пользователя на основе его истории.
    
    Args:
        user_id: ID пользователя
        k: Количество рекомендаций
        
    Returns:
        Словарь с ключом 'recs' и списком ID треков для рекомендации
    """
    logger.info(f"Getting online recommendations for user_id: {user_id}, k: {k}")
    
    try:
        # Получаем историю пользователя
        params = {"user_id": user_id, "k": 3}
        resp = requests.post(
            f"{HISTORY_STORE_URL}/get", 
            headers=DEFAULT_HEADERS, 
            params=params
        )
        
        if resp.status_code != 200:
            logger.warning(f"Failed to get user history: status code {resp.status_code}")
            return {"recs": []}
        
        events = resp.json()
        events = events.get("track_id", [])
        
        if not events:
            logger.info(f"No history found for user_id: {user_id}")
            return {"recs": []}
        
        # Получаем похожие треки для каждого трека в истории
        items = []
        scores = []
        for track_id in events:
            similar_items_params = {"item_id": track_id, "k": k}
            
            try:
                similar_items_resp = requests.post(
                    f"{FEATURES_STORE_URL}/similar_items", 
                    headers=DEFAULT_HEADERS, 
                    params=similar_items_params
                )
                
                if similar_items_resp.status_code != 200:
                    logger.warning(f"Failed to get similar items for track_id {track_id}: status code {similar_items_resp.status_code}")
                    continue
                
                item_similar_items = similar_items_resp.json()
                items.extend(item_similar_items.get("item_id_2", []))
                scores.extend(item_similar_items.get("track_seq", []))
                
            except Exception as e:
                logger.error(f"Error getting similar items for track_id {track_id}: {str(e)}")
                continue
        
        # Сортируем и дедуплицируем рекомендации
        combined = list(zip(items, scores))
        combined = sorted(combined, key=lambda x: x[1], reverse=True)
        combined = [item for item, _ in combined]
        recs = dedup_ids(combined)
        
        logger.info(f"Generated {len(recs)} online recommendations for user_id: {user_id}")
        return {"recs": recs}
        
    except Exception as e:
        logger.error(f"Error generating online recommendations: {str(e)}")
        return {"recs": []}

@app.post("/recommendations", response_model=Dict[str, List[int]])
async def recommendations(user_id: int, k: int = 100) -> Dict[str, List[int]]:
    """
    Получает смешанные рекомендации (офлайн + онлайн) для пользователя.
    
    Стратегия смешивания:
    1. Чередуем офлайн и онлайн рекомендации
    2. Добавляем оставшиеся рекомендации из обоих источников
    3. Удаляем дубликаты, сохраняя порядок
    4. Ограничиваем количество рекомендаций параметром k
    
    Args:
        user_id: ID пользователя
        k: Количество рекомендаций
        
    Returns:
        Словарь с ключом 'recs' и списком ID треков для рекомендации
    """
    logger.info(f"Getting blended recommendations for user_id: {user_id}, k: {k}")
    
    try:
        # Получаем офлайн и онлайн рекомендации
        recs_offline = await recommendations_offline(user_id, k)
        recs_online = await recommendations_online(user_id, k)

        # Проверка формата данных
        if not isinstance(recs_offline.get("recs", []), list):
            logger.error(f"recs_offline['recs'] is not a list: {recs_offline}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, 
                detail="Invalid format of offline recommendations"
            )
        
        if not isinstance(recs_online.get("recs", []), list):
            logger.error(f"recs_online['recs'] is not a list: {recs_online}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, 
                detail="Invalid format of online recommendations"
            )
        
        # Смешивание рекомендаций путем чередования
        recs_blended = []
        min_length = min(len(recs_offline["recs"]), len(recs_online["recs"]))
        
        # 1. Чередуем офлайн и онлайн рекомендации
        for i in range(min_length):
            recs_blended.append(recs_offline["recs"][i])
            recs_blended.append(recs_online["recs"][i])
        
        # 2. Добавляем оставшиеся рекомендации
        recs_blended.extend(recs_offline["recs"][min_length:])
        recs_blended.extend(recs_online["recs"][min_length:])
        
        # 3. Удаляем дубликаты и ограничиваем количество
        recs_blended = dedup_ids(recs_blended)
        recs_blended = recs_blended[:k]
        
        logger.info(f"Generated {len(recs_blended)} blended recommendations for user_id: {user_id}")
        return {"recs": recs_blended}
        
    except Exception as e:
        logger.error(f"Error generating blended recommendations: {str(e)}")
        # В случае ошибки возвращаем пустой список рекомендаций
        return {"recs": []}