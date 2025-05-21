# Реализовать API на FastAPI для продаж по продавцам WB с возможностью
# обновления данных

import os
from datetime import timedelta
from typing import List, Optional

import httpx
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi_limiter import FastAPILimiter
from fastapi_limiter.depends import RateLimiter
from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, Float, DateTime, func
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.future import select
from sqlalchemy.orm import declarative_base, sessionmaker
from redis import asyncio as aioredis
from contextlib import asynccontextmanager

# Конфигурация
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://user:password@localhost/wb_sales")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
API_BASE_URL = "https://analitika.woysa.club/images/panel/json/download/niches.php"

# Инициализация FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Подключение к Redis для кэширования и ограничения скорости
    redis = await aioredis.from_url(REDIS_URL)
    await FastAPILimiter.init(redis)
    yield
    await FastAPILimiter.close()

app = FastAPI(lifespan=lifespan)

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Настройка базы данных
Base = declarative_base()

# Модели данных
class Seller(Base):
    __tablename__ = "sellers"

    id = Column(Integer, primary_key=True, index=True)
    seller_name = Column(String, index=True)
    seller_rating = Column(Float)
    seller_id = Column(Integer, unique=True)

class Shop(Base):
    __tablename__ = "shops"

    id = Column(Integer, primary_key=True, index=True)
    shop_name = Column(String, index=True)
    shop_id = Column(Integer, unique=True)
    brand_name = Column(String, index=True)

class SKU(Base):
    __tablename__ = "skus"

    id = Column(Integer, primary_key=True, index=True)
    sku_id = Column(Integer, unique=True, index=True)
    name = Column(String)
    price = Column(Float)
    category_id = Column(Integer, index=True)
    seller_id = Column(Integer, index=True)
    shop_id = Column(Integer, index=True)

class SKUStats(Base):
    __tablename__ = "sku_stats"

    id = Column(Integer, primary_key=True, index=True)
    sku_id = Column(Integer, index=True)
    date = Column(DateTime, index=True)
    sales_count = Column(Integer)
    revenue = Column(Float)
    feedbacks_count = Column(Integer)
    trend = Column(Float)

# Асинхронный движок и сессии
engine = create_async_engine(DATABASE_URL)
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# Схемы Pydantic
class SellerSchema(BaseModel):
    id: int
    seller_name: str
    seller_rating: float
    seller_id: int

    class Config:
        from_attributes = True

class ShopSchema(BaseModel):
    id: int
    shop_name: str
    shop_id: int
    brand_name: str

    class Config:
        from_attributes = True

class SKUSchema(BaseModel):
    id: int
    sku_id: int
    name: str
    price: float
    category_id: int

    class Config:
        from_attributes = True

class SKUStatsSchema(BaseModel):
    id: int
    sku_id: int
    date: str
    sales_count: int
    revenue: float
    feedbacks_count: int
    trend: float

    class Config:
        from_attributes = True

class CategoryResponse(BaseModel):
    sellers: List[SellerSchema]
    shops: List[ShopSchema]
    skus: List[SKUSchema]
    sku_stats: List[SKUStatsSchema]

# Вспомогательные функции
async def get_db():
    async with async_session() as session:
        yield session

async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# API Endpoints
@app.get("/category/{category_id}", response_model=CategoryResponse)
async def get_category_data(
        category_id: int,
        skip: int = Query(0, ge=0),
        limit: int = Query(100, le=1000),
        db: AsyncSession = Depends(get_db),
        rate_limiter: RateLimiter = Depends(RateLimiter(times=10, seconds=60))
):
    """
    Получить данные по продажам для указанной категории
    """
    # Проверяем наличие данных в кэше
    cache_key = f"category_{category_id}_{skip}_{limit}"
    cached_data = await FastAPILimiter.redis.get(cache_key)
    if cached_data:
        return JSONResponse(content=cached_data)

    # Получаем данные из БД
    try:
        # Получаем продавцов
        sellers = await db.execute(
            select(Seller).where(
                Seller.id.in_(
                    select(SKU.seller_id).where(SKU.category_id == category_id)
                )
            ).offset(skip).limit(limit)
        )
        sellers = sellers.scalars().all()

        # Получаем магазины
        shops = await db.execute(
            select(Shop).where(
                Shop.id.in_(
                    select(SKU.shop_id).where(SKU.category_id == category_id)
                )
            ).offset(skip).limit(limit)
        )
        shops = shops.scalars().all()

        # Получаем SKU
        skus = await db.execute(
            select(SKU).where(SKU.category_id == category_id)
            .offset(skip).limit(limit)
        )
        skus = skus.scalars().all()

        # Получаем статистику по SKU
        sku_stats = await db.execute(
            select(SKUStats).where(
                SKUStats.sku_id.in_([sku.sku_id for sku in skus])
            )
        )
        sku_stats = sku_stats.scalars().all()

        response_data = CategoryResponse(
            sellers=sellers,
            shops=shops,
            skus=skus,
            sku_stats=sku_stats
        )

        # Кэшируем результат на 5 минут
        await FastAPILimiter.redis.setex(
            cache_key,
            timedelta(minutes=5),
            response_data.json()
        )

        return response_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/update_data/{category_id}")
async def update_category_data(
        category_id: int,
        price_min: int = 0,
        price_max: int = 1060225,
        up_vy_min: int = 0,
        up_vy_max: int = 108682515,
        up_vy_pr_min: int = 0,
        up_vy_pr_max: int = 2900,
        sum_min: int = 1000,
        sum_max: int = 82432725,
        feedbacks_min: int = 0,
        feedbacks_max: int = 32767,
        trend: bool = False,
        db: AsyncSession = Depends(get_db),
        rate_limiter: RateLimiter = Depends(RateLimiter(times=2, seconds=60))
):
    """
    Обновить данные по категории из внешнего сервиса
    """
    try:
        # Создаем параметры запроса
        params = {
            "price_min": price_min,
            "price_max": price_max,
            "up_vy_min": up_vy_min,
            "up_vy_max": up_vy_max,
            "up_vy_pr_min": up_vy_pr_min,
            "up_vy_pr_max": up_vy_pr_max,
            "sum_min": sum_min,
            "sum_max": sum_max,
            "feedbacks_min": feedbacks_min,
            "feedbacks_max": feedbacks_max,
            "trend": "true" if trend else "false",
            "sort": "sum_sale",
            "sort_dir": "-1",
            "id_cat": category_id
        }

        # Получаем данные с внешнего сервиса
        async with httpx.AsyncClient() as client:
            response = await client.get(API_BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()

            # Обрабатываем и сохраняем данные
            await process_and_save_data(data, category_id, db)

            return {"status": "success", "message": f"Data for category {category_id} updated successfully"}

    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def process_and_save_data(data: dict, category_id: int, db: AsyncSession):
    """
    Обрабатывает и сохраняет данные в БД
    """
    try:
        # Обработка продавцов
        sellers = {}
        for item in data.get("sellers", []):
            seller = Seller(
                seller_name=item.get("name"),
                seller_rating=item.get("rating"),
                seller_id=item.get("id")
            )
            sellers[item["id"]] = seller

        # Обработка магазинов
        shops = {}
        for item in data.get("shops", []):
            shop = Shop(
                shop_name=item.get("name"),
                shop_id=item.get("id"),
                brand_name=item.get("brand")
            )
            shops[item["id"]] = shop

        # Обработка SKU
        skus = []
        sku_stats = []
        for item in data.get("products", []):
            sku = SKU(
                sku_id=item.get("id"),
                name=item.get("name"),
                price=item.get("price"),
                category_id=category_id,
                seller_id=item.get("seller_id"),
                shop_id=item.get("shop_id")
            )
            skus.append(sku)

            # Статистика по SKU
            stats = SKUStats(
                sku_id=item.get("id"),
                date=func.now(),
                sales_count=item.get("sales_count", 0),
                revenue=item.get("revenue", 0),
                feedbacks_count=item.get("feedbacks", 0),
                trend=item.get("trend", 0)
            )
            sku_stats.append(stats)

        # Сохраняем в БД
        async with db.begin():
            # Удаляем старые данные по этой категории
            await db.execute(
                SKU.__table__.delete().where(SKU.category_id == category_id)
            )

            # Добавляем новые данные
            for seller in sellers.values():
                db.add(seller)
            for shop in shops.values():
                db.add(shop)
            for sku in skus:
                db.add(sku)
            for stats in sku_stats:
                db.add(stats)

        await db.commit()

    except Exception as e:
        await db.rollback()
        raise e

@app.on_event("startup")
async def startup_event():
    await create_tables()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)