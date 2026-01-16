from fastapi import FastAPI, Query, HTTPException
import uvicorn

from dblib.querydb import (
    # já existentes
    get_latest_prices,
    get_timeseries,

    # novos
    ping_db,
    get_fuel_types,
    get_cities,
    search_stations,
    get_station_detail,
    get_prices_nearby,
    get_best_prices,
    get_prices_compare,
    get_stats_summary,
    get_timeseries_city,
    get_timeseries_station,
    get_price_drop_alerts,
    get_anomalies,
)

app = FastAPI(title="Simple FastAPI with Databricks")


@app.get("/")
def root():
    return {"message": "Hello Databricks"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/health/db")
def health_db():
    try:
        return ping_db()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/add/{num1}/{num2}")
def add(num1: int, num2: int):
    return {"total": num1 + num2}


# -----------------------------
# 1) META
# -----------------------------
@app.get("/meta/fuel-types")
def meta_fuel_types():
    try:
        return {"items": get_fuel_types()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/meta/cities")
def meta_cities(
    uf: str | None = Query(None, min_length=2, max_length=2),
    limit: int = Query(500, ge=1, le=5000),
):
    try:
        return {"items": get_cities(uf=uf, limit=limit)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------
# 2) STATIONS
# -----------------------------
@app.get("/stations/search")
def stations_search(
    q: str | None = Query(None, description="Busca por station_name (contém)"),
    uf: str | None = Query(None, min_length=2, max_length=2),
    city: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
):
    try:
        return {"items": search_stations(q=q, uf=uf, city=city, limit=limit)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stations/{cnpj}")
def station_detail(cnpj: int):
    try:
        data = get_station_detail(cnpj)
        if not data:
            raise HTTPException(status_code=404, detail="Posto não encontrado")
        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------
# 3) PRICES (LATEST, NEARBY, BEST, COMPARE)
# -----------------------------
@app.get("/prices/latest")
def prices_latest_alias(
    fuel_type: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=200),
    order_by: str = Query("price_unit"),
    order_dir: str = Query("asc"),
):
    try:
        return get_latest_prices(fuel_type, page, page_size, order_by, order_dir)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/latest")
def latest(
    fuel_type: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=200),
    order_by: str = Query("price_unit"),
    order_dir: str = Query("asc"),
):
    try:
        return get_latest_prices(fuel_type, page, page_size, order_by, order_dir)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/prices/nearby")
def prices_nearby(
    lat: float = Query(...),
    lng: float = Query(...),
    radius_km: float = Query(5.0, gt=0, le=200),
    fuel_type: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    order_by: str = Query("price_unit", description="price_unit | distance_km | price_ts"),
    order_dir: str = Query("asc", description="asc | desc"),
):
    try:
        return get_prices_nearby(lat, lng, radius_km, fuel_type, limit, order_by, order_dir)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/prices/best")
def prices_best(
    fuel_type: str = Query(...),
    uf: str | None = Query(None, min_length=2, max_length=2),
    city: str | None = Query(None),
    limit: int = Query(20, ge=1, le=200),
):
    try:
        return {"items": get_best_prices(fuel_type=fuel_type, uf=uf, city=city, limit=limit)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/prices/compare")
def prices_compare(
    fuel_type: str = Query(...),
    uf: str | None = Query(None, min_length=2, max_length=2),
    city: str | None = Query(None),
):
    try:
        return get_prices_compare(fuel_type=fuel_type, uf=uf, city=city)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------
# 4) TIMESERIES
# -----------------------------
@app.get("/timeseries")
def timeseries(
    fuel_type: str | None = Query(None),
    date_from: str | None = Query(None),  # "2026-01-10 00:00:00"
    date_to: str | None = Query(None),    # "2026-01-16 23:59:59"
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
):
    try:
        return get_timeseries(fuel_type, date_from, date_to, page, page_size)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/timeseries/city")
def timeseries_city(
    fuel_type: str = Query(...),
    uf: str | None = Query(None, min_length=2, max_length=2),
    city: str | None = Query(None),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    try:
        return get_timeseries_city(fuel_type, uf, city, date_from, date_to, page, page_size)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/timeseries/station")
def timeseries_station(
    cnpj: int = Query(...),
    fuel_type: str | None = Query(None),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=2000),
):
    try:
        return get_timeseries_station(cnpj, fuel_type, date_from, date_to, page, page_size)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------
# 5) STATS / ALERTS / ANOMALIES
# -----------------------------
@app.get("/stats/summary")
def stats_summary(
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
    uf: str | None = Query(None, min_length=2, max_length=2),
    city: str | None = Query(None),
    fuel_type: str | None = Query(None),
):
    try:
        return get_stats_summary(date_from, date_to, uf, city, fuel_type)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/alerts/price-drop")
def alerts_price_drop(
    fuel_type: str = Query(...),
    uf: str | None = Query(None, min_length=2, max_length=2),
    city: str | None = Query(None),
    hours: int = Query(24, ge=1, le=168),
    pct_drop: float = Query(3.0, gt=0, le=50.0),
    limit: int = Query(50, ge=1, le=500),
):
    try:
        return {"items": get_price_drop_alerts(fuel_type, uf, city, hours, pct_drop, limit)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/anomalies")
def anomalies(
    fuel_type: str = Query(...),
    uf: str | None = Query(None, min_length=2, max_length=2),
    city: str | None = Query(None),
    hours: int = Query(24, ge=1, le=168),
    z: float = Query(3.0, gt=0.5, le=10.0),
    limit: int = Query(100, ge=1, le=2000),
):
    try:
        return {"items": get_anomalies(fuel_type, uf, city, hours, z, limit)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run("fastapi_app:app", host="0.0.0.0", port=8000, reload=True)
