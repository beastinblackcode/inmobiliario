# Modelo de Datos - Madrid Real Estate Tracker

## Esquema de Base de Datos

El proyecto utiliza SQLite con dos tablas principales: `listings` y `price_history`.

---

## Tabla: `listings`

Almacena información de todas las propiedades scrapeadas de Idealista.

### Estructura

```sql
CREATE TABLE listings (
    listing_id TEXT PRIMARY KEY,           -- ID único de Idealista
    title TEXT,                            -- Título del anuncio
    url TEXT,                              -- URL completa del anuncio
    price INTEGER,                         -- Precio actual en euros
    distrito TEXT,                         -- Distrito de Madrid
    barrio TEXT,                           -- Barrio específico
    rooms INTEGER,                         -- Número de habitaciones
    size_sqm REAL,                         -- Tamaño en metros cuadrados
    floor TEXT,                            -- Planta (ej: "1ª planta", "Bajo")
    orientation TEXT,                      -- Orientación (Interior/Exterior)
    seller_type TEXT,                      -- Tipo de vendedor (Particular/Agencia)
    is_new_development BOOLEAN,            -- Si es obra nueva
    description TEXT,                      -- Descripción del anuncio
    first_seen_date TEXT,                  -- Primera vez que se vio (YYYY-MM-DD)
    last_seen_date TEXT,                   -- Última vez que se vio (YYYY-MM-DD)
    status TEXT DEFAULT 'active'           -- Estado: 'active' o 'sold_removed'
);
```

### Índices

```sql
CREATE INDEX idx_listings_status ON listings(status);
CREATE INDEX idx_listings_distrito ON listings(distrito);
CREATE INDEX idx_listings_barrio ON listings(barrio);
CREATE INDEX idx_listings_price ON listings(price);
CREATE INDEX idx_listings_last_seen ON listings(last_seen_date);
```

### Campos Clave

- **listing_id**: Clave primaria, extraída del atributo `data-element-id` de Idealista
- **status**: 
  - `'active'`: Propiedad activa en el mercado
  - `'sold_removed'`: Propiedad vendida o retirada (no vista en 7+ días)
- **first_seen_date**: Fecha de descubrimiento de la propiedad
- **last_seen_date**: Actualizada cada vez que el scraper ve la propiedad

### Valores Típicos

- **distrito**: Uno de los 21 distritos de Madrid (ej: "Centro", "Salamanca", "Chamberí")
- **barrio**: Uno de los ~147 barrios scrapeados
- **seller_type**: "Particular" o "Agencia"
- **orientation**: "Interior", "Exterior", o NULL
- **is_new_development**: 0 (False) o 1 (True)

---

## Tabla: `price_history`

Registra todos los cambios de precio detectados para cada propiedad.

### Estructura

```sql
CREATE TABLE price_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,  -- ID autoincremental
    listing_id TEXT NOT NULL,              -- Referencia a listings.listing_id
    price INTEGER NOT NULL,                -- Nuevo precio
    change_amount INTEGER,                 -- Cambio en euros (puede ser negativo)
    change_percent REAL,                   -- Cambio en porcentaje
    date_recorded TEXT NOT NULL,           -- Fecha del cambio (YYYY-MM-DD)
    FOREIGN KEY (listing_id) REFERENCES listings(listing_id)
);
```

### Índices

```sql
CREATE INDEX idx_price_history_listing ON price_history(listing_id);
CREATE INDEX idx_price_history_date ON price_history(date_recorded);
CREATE INDEX idx_price_history_change ON price_history(change_percent);
```

### Campos Clave

- **change_amount**: Diferencia en euros (negativo = bajada, positivo = subida)
- **change_percent**: Porcentaje de cambio (negativo = bajada, positivo = subida)
- **date_recorded**: Fecha en que se detectó el cambio

### Ejemplo de Registro

```
id: 1
listing_id: "12345678"
price: 285000
change_amount: -15000
change_percent: -5.0
date_recorded: "2026-02-09"
```

Esto indica que el precio bajó de €300,000 a €285,000 (bajada de €15,000 o -5%).

---

## Relaciones

```
listings (1) ----< (N) price_history
```

- Una propiedad (`listing`) puede tener múltiples cambios de precio (`price_history`)
- Cada cambio de precio está asociado a exactamente una propiedad

---

## Lógica de Negocio

### Detección de Cambios de Precio

Cuando el scraper encuentra una propiedad:

1. **Si es nueva** (`listing_id` no existe):
   - Inserta en `listings` con `status='active'`
   - NO crea registro en `price_history` (es el precio inicial)

2. **Si ya existe**:
   - Actualiza `last_seen_date` a hoy
   - Si el precio cambió:
     - Actualiza `price` en `listings`
     - Inserta registro en `price_history` con el cambio

### Detección de Ventas

Ejecutado después de cada scrape:

```python
mark_stale_as_sold(days_threshold=7)
```

- Marca como `status='sold_removed'` las propiedades con `last_seen_date` > 7 días
- Previene falsos positivos de scrapes incompletos

---

## Estadísticas Actuales

### Propiedades por Estado

```sql
SELECT status, COUNT(*) 
FROM listings 
GROUP BY status;
```

### Bajadas de Precio Recientes

```sql
SELECT 
    date_recorded,
    COUNT(*) as bajadas,
    AVG(change_percent) as bajada_promedio,
    MIN(change_percent) as mayor_bajada
FROM price_history 
WHERE change_percent < 0
GROUP BY date_recorded
ORDER BY date_recorded DESC
LIMIT 10;
```

### Propiedades por Distrito

```sql
SELECT 
    distrito,
    COUNT(*) as total,
    AVG(price) as precio_medio,
    AVG(price/size_sqm) as precio_m2_medio
FROM listings
WHERE status = 'active' AND size_sqm > 0
GROUP BY distrito
ORDER BY precio_medio DESC;
```

---

## Notas de Implementación

1. **Fechas**: Todas las fechas se almacenan como TEXT en formato ISO 8601 (`YYYY-MM-DD`)
2. **Precios**: Almacenados como INTEGER (euros sin decimales)
3. **Porcentajes**: Almacenados como REAL (con decimales)
4. **Booleanos**: SQLite no tiene tipo BOOLEAN, se usa INTEGER (0/1)
5. **Claves foráneas**: Habilitadas con `PRAGMA foreign_keys = ON`

---

## Consultas Útiles

### Propiedades con más bajadas de precio

```sql
SELECT 
    l.listing_id,
    l.title,
    l.distrito,
    COUNT(ph.id) as num_bajadas,
    SUM(ph.change_amount) as bajada_total
FROM listings l
JOIN price_history ph ON l.listing_id = ph.listing_id
WHERE ph.change_percent < 0
GROUP BY l.listing_id
ORDER BY num_bajadas DESC
LIMIT 10;
```

### Evolución de precio de una propiedad

```sql
SELECT 
    date_recorded,
    price,
    change_amount,
    change_percent
FROM price_history
WHERE listing_id = 'XXXXXXXX'
ORDER BY date_recorded;
```

### Barrios con más bajadas de precio

```sql
SELECT 
    l.barrio,
    COUNT(DISTINCT ph.listing_id) as propiedades_con_bajadas,
    AVG(ph.change_percent) as bajada_promedio
FROM listings l
JOIN price_history ph ON l.listing_id = ph.listing_id
WHERE ph.change_percent < 0
  AND ph.date_recorded >= date('now', '-30 days')
GROUP BY l.barrio
ORDER BY propiedades_con_bajadas DESC
LIMIT 20;
```
