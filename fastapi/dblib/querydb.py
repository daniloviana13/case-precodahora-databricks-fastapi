import os
from databricks import sql
from dotenv import load_dotenv

load_dotenv()


def _db_cfg():
    return {
        "server_hostname": os.environ["DATABRICKS_SERVER_HOSTNAME"],
        "http_path": os.environ["DATABRICKS_HTTP_PATH"],
        "access_token": os.environ["DATABRICKS_TOKEN"],
        "catalog": os.getenv("DATABRICKS_CATALOG", "precodahora"),
        "gold_table": os.getenv("DATABRICKS_GOLD_TABLE", "gold_latest"),
        "silver_table": os.getenv("DATABRICKS_SILVER_TABLE", "silver_prices"),
    }


def _connect():
    cfg = _db_cfg()
    return sql.connect(
        server_hostname=cfg["server_hostname"],
        http_path=cfg["http_path"],
        access_token=cfg["access_token"],
    )


def querydb(sql_text: str, params=None, as_df: bool = False, **kwargs):
    """
    Executa SQL no Databricks SQL Warehouse.

    Aceita:
      - params=...
      - parameters=... (alias)
    """
    if params is None:
        params = kwargs.get("parameters")

    params = params or ()

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(sql_text, params)

        cols = [c[0] for c in cur.description] if cur.description else []
        rows = cur.fetchall()

        if as_df:
            import pandas as pd
            return pd.DataFrame(rows, columns=cols)

        # lista de dicts
        return [dict(zip(cols, r)) for r in rows] if cols else rows
    finally:
        try:
            conn.close()
        except Exception:
            pass


# --------------------------------------------------------------------------------------
# EXISTING APIs (você já tinha)
# --------------------------------------------------------------------------------------
def get_latest_prices(
    fuel_type: str | None,
    page: int,
    page_size: int,
    order_by: str,
    order_dir: str,
):
    cfg = _db_cfg()
    table = f"{cfg['catalog']}.{cfg['gold_table']}"

    allowed_order_by = {
        "price_unit", "price_net", "price_gross", "discount", "price_ts",
        "city", "uf", "station_name", "distance_km"
    }
    if order_by not in allowed_order_by:
        raise ValueError(f"order_by inválido. Use um destes: {sorted(allowed_order_by)}")

    order_dir = order_dir.lower().strip()
    if order_dir not in {"asc", "desc"}:
        raise ValueError("order_dir inválido. Use: asc ou desc")

    where = []
    params = []

    if fuel_type:
        where.append("fuel_type = ?")
        params.append(fuel_type)

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    offset = (page - 1) * page_size

    q_total = f"SELECT COUNT(*) AS total FROM {table}{where_sql}"
    total_rows = querydb(q_total, params=params, as_df=False)
    total = int(total_rows[0]["total"]) if total_rows else 0

    q_data = f"""
        SELECT
            fuel_type, uf, city, station_name, cnpj,
            product_desc, unit,
            price_ts, price_unit, price_net, price_gross, discount,
            lat, lng, distance_km
        FROM {table}
        {where_sql}
        ORDER BY {order_by} {order_dir}
        LIMIT ? OFFSET ?
    """
    data_params = list(params) + [page_size, offset]
    items = querydb(q_data, params=data_params, as_df=False)

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "items": items,
    }


def get_timeseries(
    fuel_type: str | None,
    date_from: str | None,
    date_to: str | None,
    page: int,
    page_size: int,
):
    """
    Timeseries agregada por dia (média de price_unit).
    date_from/date_to em formato string: "YYYY-MM-DD HH:MM:SS"
    """
    cfg = _db_cfg()
    table = f"{cfg['catalog']}.{cfg['silver_table']}"

    where = []
    params = []

    if fuel_type:
        where.append("fuel_type = ?")
        params.append(fuel_type)

    if date_from:
        where.append("price_ts >= TIMESTAMP(?)")
        params.append(date_from)

    if date_to:
        where.append("price_ts <= TIMESTAMP(?)")
        params.append(date_to)

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    offset = (page - 1) * page_size

    q_total = f"""
        SELECT COUNT(*) AS total
        FROM (
            SELECT date_trunc('day', price_ts) AS day
            FROM {table}
            {where_sql}
            GROUP BY date_trunc('day', price_ts)
        ) t
    """
    total_rows = querydb(q_total, params=params, as_df=False)
    total = int(total_rows[0]["total"]) if total_rows else 0

    q_data = f"""
        SELECT
            date_trunc('day', price_ts) AS day,
            AVG(price_unit) AS avg_price_unit,
            MIN(price_unit) AS min_price_unit,
            MAX(price_unit) AS max_price_unit,
            COUNT(*) AS samples
        FROM {table}
        {where_sql}
        GROUP BY date_trunc('day', price_ts)
        ORDER BY day ASC
        LIMIT ? OFFSET ?
    """
    data_params = list(params) + [page_size, offset]
    items = querydb(q_data, params=data_params, as_df=False)

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "items": items,
    }


# --------------------------------------------------------------------------------------
# NEW APIs helpers
# --------------------------------------------------------------------------------------
def ping_db():
    rows = querydb("SELECT 1 AS ok", params=(), as_df=False)
    return {"ok": True, "db": rows[0] if rows else {"ok": 1}}


def get_fuel_types():
    cfg = _db_cfg()
    table = f"{cfg['catalog']}.{cfg['gold_table']}"
    q = f"""
        SELECT DISTINCT fuel_type
        FROM {table}
        WHERE fuel_type IS NOT NULL
        ORDER BY fuel_type
    """
    rows = querydb(q, params=(), as_df=False)
    return [r["fuel_type"] for r in rows]


def get_cities(uf: str | None = None, limit: int = 500):
    cfg = _db_cfg()
    table = f"{cfg['catalog']}.{cfg['gold_table']}"

    where = []
    params = []
    if uf:
        where.append("uf = ?")
        params.append(uf)

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    q = f"""
        SELECT DISTINCT uf, city
        FROM {table}
        {where_sql}
        AND city IS NOT NULL
        ORDER BY uf, city
        LIMIT ?
    """
    params.append(limit)
    return querydb(q, params=params, as_df=False)


def search_stations(q: str | None, uf: str | None, city: str | None, limit: int = 50):
    cfg = _db_cfg()
    table = f"{cfg['catalog']}.{cfg['gold_table']}"

    where = []
    params = []

    if q:
        where.append("LOWER(station_name) LIKE ?")
        params.append(f"%{q.lower()}%")
    if uf:
        where.append("uf = ?")
        params.append(uf)
    if city:
        where.append("city = ?")
        params.append(city)

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    qsql = f"""
        SELECT DISTINCT
            cnpj, station_name, city, uf, district, street, number, zip_code, lat, lng
        FROM {table}
        {where_sql}
        ORDER BY uf, city, station_name
        LIMIT ?
    """
    params.append(limit)
    return querydb(qsql, params=params, as_df=False)


def get_station_detail(cnpj: int):
    cfg = _db_cfg()
    table = f"{cfg['catalog']}.{cfg['gold_table']}"

    qsql = f"""
        SELECT
            cnpj, station_name, city, uf, district, street, number, zip_code, lat, lng,
            fuel_type, product_desc, unit,
            price_ts, price_unit, price_net, price_gross, discount
        FROM {table}
        WHERE cnpj = ?
        ORDER BY price_ts DESC
    """
    rows = querydb(qsql, params=(cnpj,), as_df=False)
    if not rows:
        return None

    base = rows[0].copy()
    fuels = []
    for r in rows:
        fuels.append({
            "fuel_type": r.get("fuel_type"),
            "product_desc": r.get("product_desc"),
            "unit": r.get("unit"),
            "price_ts": r.get("price_ts"),
            "price_unit": r.get("price_unit"),
            "price_net": r.get("price_net"),
            "price_gross": r.get("price_gross"),
            "discount": r.get("discount"),
        })

    return {
        "cnpj": base.get("cnpj"),
        "station_name": base.get("station_name"),
        "city": base.get("city"),
        "uf": base.get("uf"),
        "district": base.get("district"),
        "street": base.get("street"),
        "number": base.get("number"),
        "zip_code": base.get("zip_code"),
        "lat": base.get("lat"),
        "lng": base.get("lng"),
        "fuels": fuels,
    }


def get_prices_nearby(
    lat: float,
    lng: float,
    radius_km: float,
    fuel_type: str | None,
    limit: int,
    order_by: str,
    order_dir: str,
):
    """
    Nearby prices with computed distance using Haversine.
    Works even if your table doesn't have distance_km.
    """
    cfg = _db_cfg()
    table = f"{cfg['catalog']}.{cfg['gold_table']}"

    allowed_order_by = {"price_unit", "distance_km", "price_ts"}
    if order_by not in allowed_order_by:
        raise ValueError(f"order_by inválido. Use: {sorted(allowed_order_by)}")

    order_dir = order_dir.lower().strip()
    if order_dir not in {"asc", "desc"}:
        raise ValueError("order_dir inválido. Use: asc ou desc")

    where = ["lat IS NOT NULL", "lng IS NOT NULL"]
    params = []

    # Haversine in km
    distance_expr = """
        (6371 * 2 * ASIN(
            SQRT(
                POW(SIN(RADIANS(lat - ?) / 2), 2) +
                COS(RADIANS(?)) * COS(RADIANS(lat)) *
                POW(SIN(RADIANS(lng - ?) / 2), 2)
            )
        ))
    """

    # parameters for distance_expr: lat, lat, lng
    params.extend([lat, lat, lng])

    if fuel_type:
        where.append("fuel_type = ?")
        params.append(fuel_type)

    where_sql = " WHERE " + " AND ".join(where)

    qsql = f"""
        SELECT
            fuel_type, uf, city, station_name, cnpj,
            product_desc, unit,
            price_ts, price_unit, price_net, price_gross, discount,
            lat, lng,
            {distance_expr} AS distance_km
        FROM {table}
        {where_sql}
        QUALIFY distance_km <= ?
        ORDER BY {order_by} {order_dir}
        LIMIT ?
    """
    params.append(radius_km)
    params.append(limit)

    return {"items": querydb(qsql, params=params, as_df=False)}


def get_best_prices(fuel_type: str, uf: str | None, city: str | None, limit: int):
    cfg = _db_cfg()
    table = f"{cfg['catalog']}.{cfg['gold_table']}"

    where = ["fuel_type = ?"]
    params = [fuel_type]

    if uf:
        where.append("uf = ?")
        params.append(uf)
    if city:
        where.append("city = ?")
        params.append(city)

    where_sql = " WHERE " + " AND ".join(where)

    qsql = f"""
        SELECT
            fuel_type, uf, city, station_name, cnpj,
            price_ts, price_unit, price_net, price_gross, discount,
            lat, lng
        FROM {table}
        {where_sql}
        ORDER BY price_unit ASC, price_ts DESC
        LIMIT ?
    """
    params.append(limit)
    return querydb(qsql, params=params, as_df=False)


def get_prices_compare(fuel_type: str, uf: str | None, city: str | None):
    cfg = _db_cfg()
    table = f"{cfg['catalog']}.{cfg['gold_table']}"

    where = ["fuel_type = ?"]
    params = [fuel_type]

    if uf:
        where.append("uf = ?")
        params.append(uf)
    if city:
        where.append("city = ?")
        params.append(city)

    where_sql = " WHERE " + " AND ".join(where)

    qsql = f"""
        SELECT
            fuel_type,
            COALESCE(uf, '') AS uf,
            COALESCE(city, '') AS city,
            AVG(price_unit) AS avg_price_unit,
            MIN(price_unit) AS min_price_unit,
            MAX(price_unit) AS max_price_unit,
            COUNT(*) AS stations,
            MAX(price_ts) AS last_price_ts
        FROM {table}
        {where_sql}
        GROUP BY fuel_type, COALESCE(uf, ''), COALESCE(city, '')
    """
    rows = querydb(qsql, params=params, as_df=False)
    return rows[0] if rows else {"fuel_type": fuel_type, "stations": 0}


def get_stats_summary(date_from: str | None, date_to: str | None, uf: str | None, city: str | None, fuel_type: str | None):
    cfg = _db_cfg()
    table = f"{cfg['catalog']}.{cfg['silver_table']}"

    where = []
    params = []

    if date_from:
        where.append("price_ts >= TIMESTAMP(?)")
        params.append(date_from)
    if date_to:
        where.append("price_ts <= TIMESTAMP(?)")
        params.append(date_to)
    if uf:
        where.append("uf = ?")
        params.append(uf)
    if city:
        where.append("city = ?")
        params.append(city)
    if fuel_type:
        where.append("fuel_type = ?")
        params.append(fuel_type)

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""

    qsql = f"""
        SELECT
            COUNT(*) AS rows,
            COUNT(DISTINCT cnpj) AS stations,
            COUNT(DISTINCT city) AS cities,
            COUNT(DISTINCT uf) AS ufs,
            MIN(price_ts) AS min_price_ts,
            MAX(price_ts) AS max_price_ts,
            AVG(price_unit) AS avg_price_unit
        FROM {table}
        {where_sql}
    """
    rows = querydb(qsql, params=params, as_df=False)
    return rows[0] if rows else {}


def get_timeseries_city(
    fuel_type: str,
    uf: str | None,
    city: str | None,
    date_from: str | None,
    date_to: str | None,
    page: int,
    page_size: int,
):
    cfg = _db_cfg()
    table = f"{cfg['catalog']}.{cfg['silver_table']}"

    where = ["fuel_type = ?"]
    params = [fuel_type]

    if uf:
        where.append("uf = ?")
        params.append(uf)
    if city:
        where.append("city = ?")
        params.append(city)
    if date_from:
        where.append("price_ts >= TIMESTAMP(?)")
        params.append(date_from)
    if date_to:
        where.append("price_ts <= TIMESTAMP(?)")
        params.append(date_to)

    where_sql = " WHERE " + " AND ".join(where)
    offset = (page - 1) * page_size

    q_total = f"""
        SELECT COUNT(*) AS total
        FROM (
            SELECT date_trunc('day', price_ts) AS day
            FROM {table}
            {where_sql}
            GROUP BY date_trunc('day', price_ts)
        ) t
    """
    total_rows = querydb(q_total, params=params, as_df=False)
    total = int(total_rows[0]["total"]) if total_rows else 0

    q_data = f"""
        SELECT
            date_trunc('day', price_ts) AS day,
            AVG(price_unit) AS avg_price_unit,
            MIN(price_unit) AS min_price_unit,
            MAX(price_unit) AS max_price_unit,
            COUNT(*) AS samples
        FROM {table}
        {where_sql}
        GROUP BY date_trunc('day', price_ts)
        ORDER BY day ASC
        LIMIT ? OFFSET ?
    """
    data_params = list(params) + [page_size, offset]
    items = querydb(q_data, params=data_params, as_df=False)

    return {"page": page, "page_size": page_size, "total": total, "items": items}


def get_timeseries_station(
    cnpj: int,
    fuel_type: str | None,
    date_from: str | None,
    date_to: str | None,
    page: int,
    page_size: int,
):
    cfg = _db_cfg()
    table = f"{cfg['catalog']}.{cfg['silver_table']}"

    where = ["cnpj = ?"]
    params = [cnpj]

    if fuel_type:
        where.append("fuel_type = ?")
        params.append(fuel_type)
    if date_from:
        where.append("price_ts >= TIMESTAMP(?)")
        params.append(date_from)
    if date_to:
        where.append("price_ts <= TIMESTAMP(?)")
        params.append(date_to)

    where_sql = " WHERE " + " AND ".join(where)
    offset = (page - 1) * page_size

    q_total = f"SELECT COUNT(*) AS total FROM {table}{where_sql}"
    total_rows = querydb(q_total, params=params, as_df=False)
    total = int(total_rows[0]["total"]) if total_rows else 0

    q_data = f"""
        SELECT
            price_ts, fuel_type, price_unit, price_net, price_gross, discount,
            station_name, city, uf, lat, lng
        FROM {table}
        {where_sql}
        ORDER BY price_ts DESC
        LIMIT ? OFFSET ?
    """
    data_params = list(params) + [page_size, offset]
    items = querydb(q_data, params=data_params, as_df=False)

    return {"page": page, "page_size": page_size, "total": total, "items": items}


def get_price_drop_alerts(
    fuel_type: str,
    uf: str | None,
    city: str | None,
    hours: int,
    pct_drop: float,
    limit: int,
):
    """
    Queda percentual do preço médio do posto nas últimas N horas vs N horas anteriores.
    """
    cfg = _db_cfg()
    table = f"{cfg['catalog']}.{cfg['silver_table']}"

    where = ["fuel_type = ?"]
    params = [fuel_type]

    if uf:
        where.append("uf = ?")
        params.append(uf)
    if city:
        where.append("city = ?")
        params.append(city)

    where_sql = " WHERE " + " AND ".join(where)

    qsql = f"""
        WITH base AS (
          SELECT
            cnpj,
            station_name,
            city,
            uf,
            price_ts,
            price_unit
          FROM {table}
          {where_sql}
        ),
        last_window AS (
          SELECT
            cnpj,
            AVG(price_unit) AS avg_last
          FROM base
          WHERE price_ts >= current_timestamp() - INTERVAL {hours} HOURS
          GROUP BY cnpj
        ),
        prev_window AS (
          SELECT
            cnpj,
            AVG(price_unit) AS avg_prev
          FROM base
          WHERE price_ts <  current_timestamp() - INTERVAL {hours} HOURS
            AND price_ts >= current_timestamp() - INTERVAL {hours * 2} HOURS
          GROUP BY cnpj
        )
        SELECT
          b.cnpj,
          MAX(b.station_name) AS station_name,
          MAX(b.city) AS city,
          MAX(b.uf) AS uf,
          l.avg_last,
          p.avg_prev,
          ((p.avg_prev - l.avg_last) / p.avg_prev) * 100 AS pct_drop
        FROM base b
        JOIN last_window l ON b.cnpj = l.cnpj
        JOIN prev_window p ON b.cnpj = p.cnpj
        GROUP BY b.cnpj, l.avg_last, p.avg_prev
        HAVING p.avg_prev IS NOT NULL AND l.avg_last IS NOT NULL
           AND ((p.avg_prev - l.avg_last) / p.avg_prev) * 100 >= ?
        ORDER BY pct_drop DESC
        LIMIT ?
    """
    params2 = list(params) + [pct_drop, limit]
    return querydb(qsql, params=params2, as_df=False)


def get_anomalies(
    fuel_type: str,
    uf: str | None,
    city: str | None,
    hours: int,
    z: float,
    limit: int,
):
    """
    Anomalias por z-score nas últimas N horas (escopo por filtro uf/city).
    """
    cfg = _db_cfg()
    table = f"{cfg['catalog']}.{cfg['silver_table']}"

    where = ["fuel_type = ?"]
    params = [fuel_type]

    if uf:
        where.append("uf = ?")
        params.append(uf)
    if city:
        where.append("city = ?")
        params.append(city)

    where_sql = " WHERE " + " AND ".join(where)

    qsql = f"""
        WITH recent AS (
          SELECT *
          FROM {table}
          {where_sql}
          AND price_ts >= current_timestamp() - INTERVAL {hours} HOURS
          AND price_unit IS NOT NULL
        ),
        stats AS (
          SELECT
            AVG(price_unit) AS mu,
            STDDEV_SAMP(price_unit) AS sigma
          FROM recent
        )
        SELECT
          r.price_ts, r.cnpj, r.station_name, r.city, r.uf,
          r.fuel_type, r.price_unit,
          s.mu, s.sigma,
          CASE WHEN s.sigma = 0 OR s.sigma IS NULL THEN NULL
               ELSE (r.price_unit - s.mu) / s.sigma
          END AS zscore
        FROM recent r
        CROSS JOIN stats s
        WHERE s.sigma IS NOT NULL AND s.sigma > 0
          AND ABS((r.price_unit - s.mu) / s.sigma) >= ?
        ORDER BY ABS((r.price_unit - s.mu) / s.sigma) DESC
        LIMIT ?
    """
    params2 = list(params) + [z, limit]
    return querydb(qsql, params=params2, as_df=False)
