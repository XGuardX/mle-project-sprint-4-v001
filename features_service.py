import logging
from typing import Dict, List, Any, Optional
from contextlib import asynccontextmanager

import pandas as pd
from fastapi import FastAPI, HTTPException, status

logger = logging.getLogger("uvicorn.error")

class SimilarItems:
    """
    Класс для управления похожими элементами (треками).
    Загружает и хранит информацию о похожих треках.
    """
    def __init__(self):
        self._similar_items = None
        self._stats = {
            "request_count": 0,
            "not_found_count": 0
        }

    def load(self, path: str, **kwargs) -> None:
        """
        Загружает данные о похожих элементах из parquet файла.
        
        Args:
            path: Путь к файлу с похожими элементами
            **kwargs: Дополнительные параметры для pd.read_parquet
        """
        logger.info(f"Loading similar items data from {path}")
        try:
            self._similar_items = pd.read_parquet(path, **kwargs)
            self._similar_items = self._similar_items.set_index('item_id_1')
            logger.info(f"Successfully loaded similar items data, shape: {self._similar_items.shape}")
        except Exception as e:
            logger.error(f"Failed to load similar items data: {str(e)}")
            raise

    def get(self, item_id: int, k: int = 10) -> Dict[str, List[Any]]:
        """
        Получает список похожих элементов для данного item_id.
        
        Args:
            item_id: ID элемента
            k: Количество похожих элементов для возврата
            
        Returns:
            Словарь с ключами 'item_id_2' и 'track_seq', содержащими списки ID и коэффициенты похожести
        """
        self._stats["request_count"] += 1
        
        try:
            similar_items = self._similar_items.loc[item_id]
            result = similar_items[["item_id_2", "track_seq"]].head(k).to_dict(orient="list")
            logger.info(f"Found {len(result['item_id_2'])} similar items for item_id: {item_id}")
            return result
        except KeyError:
            logger.warning(f"No similar items found for item_id: {item_id}")
            self._stats["not_found_count"] += 1
            return {"item_id_2": [], "track_seq": []}
        except Exception as e:
            logger.error(f"Error getting similar items for item_id {item_id}: {str(e)}")
            return {"item_id_2": [], "track_seq": []}

    def stats(self) -> None:
        """
        Выводит статистику использования в лог.
        """
        logger.info("Stats for similar items")
        for name, value in self._stats.items():
            logger.info(f"{name:<30} {value}")

# Создаем экземпляр класса
sim_items_store = SimilarItems()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Инициализирует сервис при запуске и выполняет завершающие действия при остановке.
    
    Args:
        app: Экземпляр FastAPI приложения
    """
    logger.info("Starting features service")
    try:
        # Код ниже (до yield) выполнится только один раз при запуске сервиса
        sim_items_store.load(
            "recsys/recommendations/similar.parquet",  # путь к файлу с похожими элементами
            columns=["item_id_1", "item_id_2", "track_seq"],
        )
        app.state.sim_items = sim_items_store
        logger.info("Features service initialized successfully and ready to serve requests!")
    except Exception as e:
        logger.error(f"Failed to initialize features service: {str(e)}")
        raise
    
    yield
    
    # Код ниже выполнится только один раз при остановке сервиса
    logger.info("Stopping features service")
    app.state.sim_items.stats()

# Создаём приложение FastAPI
app = FastAPI(
    title="Features Service",
    description="API для получения похожих элементов",
    lifespan=lifespan
)

@app.post("/similar_items", response_model=Dict[str, List[Any]])
async def similar_items(item_id: int, k: int = 10) -> Dict[str, List[Any]]:
    """
    Получает список похожих элементов для данного item_id.
    
    Args:
        item_id: ID элемента
        k: Количество похожих элементов для возврата
        
    Returns:
        Словарь с ключами 'item_id_2' и 'track_seq', содержащими списки ID и коэффициенты похожести
    """
    logger.info(f"Received request for similar items, item_id: {item_id}, k: {k}")
    
    try:
        similar_items = app.state.sim_items.get(item_id, k)
        return similar_items
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving similar items"
        )