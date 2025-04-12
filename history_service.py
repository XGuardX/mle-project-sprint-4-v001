import logging
from typing import Dict, List, Any, Optional
from contextlib import asynccontextmanager

import pandas as pd
from fastapi import FastAPI, HTTPException, status

logger = logging.getLogger("uvicorn.error")

class History:
    """
    Класс для управления историей пользователей.
    Загружает и хранит историю прослушивания треков.
    """
    def __init__(self):
        self._history = None
        self._stats = {
            "request_count": 0,
            "not_found_count": 0
        }

    def load(self, path: str, **kwargs) -> None:
        """
        Загружает данные истории из parquet файла.
        
        Args:
            path: Путь к файлу с историей
            **kwargs: Дополнительные параметры для pd.read_parquet
        """
        logger.info(f"Loading user history data from {path}")
        try:
            self._history = pd.read_parquet(path, **kwargs)
            self._history = self._history.set_index('user_id')
            logger.info(f"Successfully loaded user history data, shape: {self._history.shape}")
        except Exception as e:
            logger.error(f"Failed to load user history data: {str(e)}")
            raise

    def get(self, user_id: int, k: int = 3) -> Dict[str, List[Any]]:
        """
        Получает историю прослушивания для пользователя.
        
        Args:
            user_id: ID пользователя
            k: Количество последних треков для возврата
            
        Returns:
            Словарь с ключами 'track_id' и 'track_seq', содержащими списки ID треков и их последовательности
        """
        self._stats["request_count"] += 1
        
        try:
            history = self._history.loc[user_id]
            result = history[["track_id", "track_seq"]].head(k).to_dict(orient="list")
            logger.info(f"Found {len(result['track_id'])} history items for user_id: {user_id}")
            return result
        except KeyError:
            logger.warning(f"No history found for user_id: {user_id}")
            self._stats["not_found_count"] += 1
            return {"track_id": [], "track_seq": []}
        except Exception as e:
            logger.error(f"Error getting history for user_id {user_id}: {str(e)}")
            return {"track_id": [], "track_seq": []}

    def stats(self) -> None:
        """
        Выводит статистику использования в лог.
        """
        logger.info("Stats for user history")
        for name, value in self._stats.items():
            logger.info(f"{name:<30} {value}")

# Создаем экземпляр класса
history_store = History()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Инициализирует сервис при запуске и выполняет завершающие действия при остановке.
    
    Args:
        app: Экземпляр FastAPI приложения
    """
    logger.info("Starting history service")
    try:
        # Код выполнится один раз при запуске сервиса
        history_store.load(
            "recsys/recommendations/personal_als.parquet",
            columns=["user_id", "track_id", "track_seq"],
        )
        app.state.history = history_store
        logger.info("History service initialized successfully and ready to serve requests!")
    except Exception as e:
        logger.error(f"Failed to initialize history service: {str(e)}")
        raise
    
    yield
    
    # Код выполнится один раз при остановке сервиса
    logger.info("Stopping history service")
    app.state.history.stats()

# Создаём приложение FastAPI
app = FastAPI(
    title="History Service",
    description="API для получения истории прослушивания пользователей",
    version="1.0.0",
    lifespan=lifespan
)

@app.post("/get", response_model=Dict[str, List[Any]])
async def get_history(user_id: int, k: int = 3) -> Dict[str, List[Any]]:
    """
    Получает историю прослушивания для пользователя.
    
    Args:
        user_id: ID пользователя
        k: Количество последних треков для возврата
        
    Returns:
        Словарь с ключами 'track_id' и 'track_seq', содержащими списки ID треков и их последовательности
    """
    logger.info(f"Received request for user history, user_id: {user_id}, k: {k}")
    
    try:
        history = app.state.history.get(user_id, k)
        return history
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving user history"
        )