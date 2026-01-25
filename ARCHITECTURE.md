# Arquitectura del Sistema: Madrid Real Estate Tracker

## 📋 Índice

1. [Visión General](#visión-general)
2. [Componentes del Sistema](#componentes-del-sistema)
3. [Arquitectura de Datos](#arquitectura-de-datos)
4. [Flujo de Operación](#flujo-de-operación)
5. [Despliegue](#despliegue)
6. [Seguridad](#seguridad)
7. [Costes y Escalabilidad](#costes-y-escalabilidad)

---

## Visión General

### Propósito

Sistema de monitorización del mercado inmobiliario de Madrid que:
- Rastrea diariamente ~184 barrios de Madrid
- Detecta nuevas propiedades, cambios de precio y ventas
- Visualiza tendencias y métricas del mercado
- Proporciona acceso web seguro a los datos

### Arquitectura General

```
┌─────────────────────────────────────────────────────────────┐
│                    USUARIO (Navegador)                       │
└────────────────────────┬────────────────────────────────────┘
                         │ HTTPS
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              STREAMLIT CLOUD (Dashboard Web)                 │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  app.py (Streamlit Dashboard)                        │  │
│  │  - Autenticación multi-usuario                       │  │
│  │  - Visualización de datos                            │  │
│  │  - Filtros y análisis                                │  │
│  └────────────────────┬─────────────────────────────────┘  │
│                       │                                      │
│  ┌────────────────────▼─────────────────────────────────┐  │
│  │  database.py (Capa de Datos)                         │  │
│  │  - Descarga DB desde Google Drive                    │  │
│  │  - Consultas SQL                                     │  │
│  └────────────────────┬─────────────────────────────────┘  │
└───────────────────────┼──────────────────────────────────────┘
                        │
                        ▼
              ┌─────────────────────┐
              │   GOOGLE DRIVE      │
              │  real_estate.db     │
              │  (6 MB SQLite)      │
              └─────────────────────┘
                        ▲
                        │ Upload manual
                        │
┌───────────────────────┴──────────────────────────────────────┐
│              MÁQUINA LOCAL (Scraping)                         │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  scraper.py                                          │   │
│  │  - Scraping de Idealista                            │   │
│  │  - Detección de cambios                             │   │
│  │  - Actualización de DB local                        │   │
│  └────────────────────┬─────────────────────────────────┘   │
│                       │                                       │
│  ┌────────────────────▼─────────────────────────────────┐   │
│  │  database.py (Gestión DB)                            │   │
│  │  - Inserciones/actualizaciones                       │   │
│  │  - Marcado de vendidos                               │   │
│  └────────────────────┬─────────────────────────────────┘   │
│                       │                                       │
│  ┌────────────────────▼─────────────────────────────────┐   │
│  │  real_estate.db (SQLite local)                       │   │
│  └──────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────┘
                        │
                        ▼
              ┌─────────────────────┐
              │   BRIGHT DATA       │
              │  (Web Unlocker)     │
              │  Proxy + Anti-bot   │
              └─────────────────────┘
                        │
                        ▼
              ┌─────────────────────┐
              │   IDEALISTA.COM     │
              │  (Fuente de datos)  │
              └─────────────────────┘
```

---

## Componentes del Sistema

### 1. Scraper (Ejecución Local)

**Archivo:** `scraper.py`

**Responsabilidades:**
- Scraping de 184 barrios de Madrid
- Extracción de datos de propiedades
- Detección de cambios (nuevas, actualizadas, vendidas)
- Actualización de base de datos local

**Tecnologías:**
- **Python 3.x**
- **BeautifulSoup4** - Parsing HTML
- **Requests** - HTTP requests
- **Bright Data Web Unlocker** - Proxy anti-bot
- **SQLite** - Base de datos local

**Datos Extraídos:**
```python
{
    'listing_id': str,           # ID único de Idealista
    'title': str,                # Título del anuncio
    'url': str,                  # URL completa
    'price': int,                # Precio en €
    'distrito': str,             # Distrito de Madrid
    'barrio': str,               # Barrio específico
    'rooms': int,                # Número de habitaciones
    'size_sqm': float,           # Superficie en m²
    'floor': str,                # Planta
    'orientation': str,          # Interior/Exterior
    'seller_type': str,          # Particular/Agencia
    'is_new_development': bool,  # Obra nueva
    'description': str,          # Descripción parcial
}
```

**Frecuencia de Ejecución:**
- Manual o programada (cron/scheduler)
- Recomendado: Diario

---

### 2. Base de Datos (SQLite)

**Archivo:** `real_estate.db`

**Esquema:**

```sql
CREATE TABLE listings (
    listing_id TEXT PRIMARY KEY,
    title TEXT,
    url TEXT,
    price INTEGER,
    distrito TEXT,
    barrio TEXT,
    rooms INTEGER,
    size_sqm REAL,
    floor TEXT,
    orientation TEXT,
    seller_type TEXT,
    is_new_development BOOLEAN,
    description TEXT,
    first_seen_date TEXT,      -- Fecha primera vez visto
    last_seen_date TEXT,       -- Fecha última vez visto
    status TEXT DEFAULT 'active'  -- active/sold
);

-- Índices para optimizar consultas
CREATE INDEX idx_status ON listings(status);
CREATE INDEX idx_distrito ON listings(distrito);
CREATE INDEX idx_last_seen ON listings(last_seen_date);
CREATE INDEX idx_price ON listings(price);
```

**Tamaño:** ~6 MB (varía según número de listings)

**Ubicación:**
- **Local:** `/Users/luisnuno/Downloads/workspace/inmobiliario/real_estate.db`
- **Cloud:** Google Drive (compartido públicamente)

---

### 3. Dashboard Web (Streamlit Cloud)

**Archivo:** `app.py`

**Responsabilidades:**
- Interfaz web de visualización
- Autenticación de usuarios
- Análisis y filtros de datos
- Exportación de datos

**Características:**

#### Autenticación
- Multi-usuario con contraseñas individuales
- Sesión persistente
- Credenciales en Streamlit Secrets

#### Visualizaciones
- **Métricas principales:** Total activos, nuevos, vendidos, precio medio
- **Gráficos:** Distribución de precios, tendencias temporales
- **Tablas:** Listados detallados con filtros
- **Mapas:** Distribución por distrito/barrio

#### Filtros
- Precio (mín/máx)
- Distrito/Barrio
- Tipo de vendedor
- Estado (activo/vendido)
- Fecha

---

### 4. Capa de Datos (database.py)

**Archivo:** `database.py`

**Funciones Principales:**

```python
# Inicialización
init_database()

# Descarga desde Google Drive (solo en cloud)
download_database_from_cloud()

# Operaciones CRUD
insert_listing(data: Dict) -> bool
update_listing(listing_id: str, data: Dict) -> bool
mark_as_sold(listing_ids: Set[str]) -> int

# Consultas
get_active_listing_ids() -> Set[str]
get_listings(status, distrito, barrio, ...) -> List[Dict]
get_price_statistics() -> Dict
get_sold_last_n_days(days: int) -> int
```

**Detección de Entorno:**
```python
def is_streamlit_cloud():
    # Detecta si corre en Streamlit Cloud
    return "database" in st.secrets
```

---

## Arquitectura de Datos

### Flujo de Datos

```
1. SCRAPING (Local)
   ├─ Idealista.com
   ├─ Bright Data Proxy
   ├─ BeautifulSoup parsing
   └─ SQLite local (real_estate.db)

2. UPLOAD (Manual)
   ├─ Google Drive upload
   └─ Compartir públicamente

3. DASHBOARD (Cloud)
   ├─ Download from Google Drive
   ├─ Cache en Streamlit Cloud
   └─ Visualización web
```

### Sincronización de Datos

**Problema:** Base de datos local vs cloud

**Solución Actual:** Upload manual a Google Drive

**Proceso:**
1. Ejecutar scraper localmente
2. Subir `real_estate.db` a Google Drive
3. Dashboard descarga automáticamente en próximo acceso

---

## Flujo de Operación

### 🔄 Ciclo Completo de Actualización

#### Paso 1: Scraping Local

```bash
# En tu máquina local
cd /Users/luisnuno/Downloads/workspace/inmobiliario
source venv/bin/activate
python scraper.py
```

**Duración:** ~2-4 horas (184 barrios)

**Output:**
- Base de datos actualizada: `real_estate.db`
- Logs de progreso
- Estadísticas de cambios

**Cambios Detectados:**
- ✅ **Nuevos listings:** Insertados con `first_seen_date = today`
- 🔄 **Actualizados:** `last_seen_date = today`, precio actualizado
- ❌ **Vendidos:** Marcados como `status = 'sold'`

---

#### Paso 2: Upload a Google Drive

**Opción A: Manual (Interfaz Web)**

1. Ve a [Google Drive](https://drive.google.com)
2. Busca el archivo `real_estate.db` existente
3. Click derecho → "Gestionar versiones"
4. "Subir nueva versión"
5. Selecciona `/Users/luisnuno/Downloads/workspace/inmobiliario/real_estate.db`
6. Espera a que termine la subida

**Opción B: Manual (Drag & Drop)**

1. Ve a Google Drive
2. Borra el archivo `real_estate.db` antiguo
3. Arrastra el nuevo `real_estate.db` desde tu carpeta local
4. Asegúrate que está compartido como "Cualquiera con el enlace puede ver"

**Opción C: Automatizada (gdrive CLI) - Opcional**

```bash
# Instalar gdrive (una sola vez)
brew install gdrive

# Autenticar (una sola vez)
gdrive about

# Subir archivo (reemplazar FILE_ID con tu ID)
gdrive update FILE_ID real_estate.db
```

**File ID actual:** `1ajdgLaneXwb6OWl_S727gwyYZUfrdF7p`

---

#### Paso 3: Actualización Automática del Dashboard

**Comportamiento:**
- Streamlit Cloud descarga la DB al iniciar
- Si ya existe, usa versión cacheada
- Para forzar actualización: **Reboot app** en Streamlit Cloud

**Verificación:**
1. Abre el dashboard: `inmobiliario-beastinblackcode.streamlit.app`
2. Login con tus credenciales
3. Verifica la fecha de "Última actualización"
4. Comprueba las métricas de nuevos/vendidos

---

### 📅 Frecuencia Recomendada

| Actividad | Frecuencia | Duración |
|-----------|-----------|----------|
| Scraping | Diario (noche) | 2-4h |
| Upload a Drive | Después de scraping | 2-5 min |
| Reboot dashboard | Opcional | 30s |

---

### 🔧 Troubleshooting

#### "Database file not found" en dashboard

**Causa:** DB no descargada desde Google Drive

**Solución:**
1. Verifica que el file ID es correcto en secrets
2. Verifica que el archivo está compartido públicamente
3. Reboot app en Streamlit Cloud

#### Scraper muy lento

**Causa:** Bright Data rate limiting o problemas de red

**Solución:**
1. Verifica credenciales de Bright Data
2. Reduce concurrencia (si aplicable)
3. Ejecuta en horarios de menos tráfico

#### Datos no actualizados en dashboard

**Causa:** Dashboard usando versión cacheada

**Solución:**
1. Streamlit Cloud → Settings → Reboot app
2. Espera 30 segundos
3. Refresca navegador

---

## Despliegue

### Entorno Local (Scraping)

**Requisitos:**
- Python 3.8+
- pip
- virtualenv

**Setup:**
```bash
cd /Users/luisnuno/Downloads/workspace/inmobiliario
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**Variables de Entorno (.env):**
```bash
BRIGHTDATA_USER=your_username
BRIGHTDATA_PASS=your_password
BRIGHTDATA_HOST=brd.superproxy.io:33335
```

---

### Streamlit Cloud (Dashboard)

**Configuración:**

**Repository:** `github.com/beastinblackcode/inmobiliario`
**Branch:** `main`
**Main file:** `app.py`

**Secrets:**
```toml
[database]
google_drive_file_id = "1ajdgLaneXwb6OWl_S727gwyYZUfrdF7p"

[auth.users]
admin = "ContraseñaAdmin123"
luis = "ContraseñaLuis456"
```

**Deployment:**
- Auto-deploy en cada push a `main`
- Manual reboot disponible en Settings

---

## Seguridad

### Autenticación

**Método:** Username/Password con session state

**Almacenamiento:** Streamlit Secrets (encriptado)

**Características:**
- ✅ Multi-usuario
- ✅ Contraseñas individuales
- ✅ Sesión persistente
- ✅ HTTPS automático

### Protección de Datos

**Base de Datos:**
- ✅ No contiene datos personales sensibles
- ✅ Solo información pública de Idealista
- ✅ Compartida públicamente (read-only)

**Credenciales:**
- ✅ Bright Data en `.env` (no en git)
- ✅ Streamlit secrets encriptados
- ✅ `.gitignore` configurado

### Prevención de Indexación

**robots.txt:**
```
User-agent: *
Disallow: /
```

Bloquea crawlers de búsqueda.

---

## Costes y Escalabilidad

### Costes Actuales

**Bright Data:**
- ~$4 por 1000 requests
- ~5000 requests por scraping completo
- **Coste por scraping:** ~$20
- **Mensual (diario):** ~$600

**Streamlit Cloud:**
- **Gratis** (Community tier)
- Límites: 1 app, recursos compartidos

**Google Drive:**
- **Gratis** (15 GB incluidos)
- DB actual: 6 MB

**Total mensual:** ~$600 (solo Bright Data)

---

### Optimizaciones Posibles

#### Reducir Costes de Scraping

1. **Scraping Selectivo:**
   - Solo barrios de interés
   - Reducir frecuencia (semanal vs diario)

2. **Proxy Alternativo:**
   - Proxies residenciales más baratos
   - Rotación manual de IPs

3. **Rate Limiting:**
   - Delays entre requests
   - Menos páginas por barrio

#### Escalabilidad

**Actual:** ~20,000 listings, 6 MB DB

**Límites:**
- SQLite: Hasta ~140 TB (teórico)
- Streamlit Cloud: ~1 GB RAM
- Google Drive: 15 GB gratis

**Proyección:**
- 1 año de datos: ~50 MB
- 5 años: ~250 MB
- **Conclusión:** Escalable para años

---

## Mejoras Futuras

### Automatización

**Opción 1: GitHub Actions**
```yaml
# .github/workflows/scrape.yml
name: Daily Scrape
on:
  schedule:
    - cron: '0 2 * * *'  # 2 AM diario
jobs:
  scrape:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - run: python scraper.py
      - run: gdrive update $FILE_ID real_estate.db
```

**Opción 2: Cron Job Local**
```bash
# crontab -e
0 2 * * * cd /path/to/inmobiliario && ./run_scraper.sh
```

**Opción 3: Cloud Function**
- Google Cloud Functions
- AWS Lambda
- Ejecutar scraper en cloud

---

### Base de Datos Cloud

**Migrar a PostgreSQL/MySQL:**

**Ventajas:**
- ✅ Actualización en tiempo real
- ✅ No upload manual
- ✅ Mejor concurrencia

**Desventajas:**
- ❌ Coste mensual ($10-50)
- ❌ Más complejidad

**Proveedores:**
- Supabase (PostgreSQL gratis hasta 500 MB)
- PlanetScale (MySQL gratis hasta 5 GB)
- Railway (PostgreSQL $5/mes)

---

### Notificaciones

**Alertas automáticas:**
- Nuevas propiedades en barrios favoritos
- Bajadas de precio significativas
- Propiedades vendidas

**Canales:**
- Email (SendGrid, Mailgun)
- Telegram Bot
- Slack webhook

---

## Resumen Operativo

### ✅ Checklist Diario

```
[ ] 1. Ejecutar scraper local (2-4h)
[ ] 2. Verificar logs de errores
[ ] 3. Subir real_estate.db a Google Drive (2 min)
[ ] 4. (Opcional) Reboot dashboard en Streamlit Cloud
[ ] 5. Verificar métricas en dashboard
```

### 📊 Métricas Clave

- **Listings activos:** ~20,000
- **Nuevos diarios:** ~200-500
- **Vendidos diarios:** ~100-300
- **Tiempo de scraping:** 2-4 horas
- **Tamaño DB:** 6 MB
- **Coste mensual:** ~$600

---

## Contacto y Soporte

**Repositorio:** `github.com/beastinblackcode/inmobiliario`

**Dashboard:** `inmobiliario-beastinblackcode.streamlit.app`

**Documentación:**
- `README.md` - Guía general
- `AUTH_SETUP.md` - Configuración de autenticación
- `MULTI_USER_AUTH.md` - Multi-usuario
- `walkthrough.md` - Implementaciones recientes
