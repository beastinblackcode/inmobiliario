# 🏠 Madrid Real Estate Tracker

**Monitorización diaria del mercado inmobiliario de Madrid**

Sistema completo de scraping, análisis y visualización del mercado de venta de viviendas en Madrid a través del portal Idealista.

## 📋 Características

- ✅ **Scraping Inteligente**: Recorre los 21 distritos de Madrid individualmente para evitar límites de paginación
- ✅ **Detección de Ventas**: Identifica propiedades vendidas/retiradas por desaparición de anuncios
- ✅ **Proxy Integration**: Usa Bright Data Web Unlocker API para evitar bloqueos
- ✅ **Base de Datos SQLite**: Persistencia local con historial completo
- ✅ **Dashboard Interactivo**: Visualización con Streamlit y gráficos interactivos
- ✅ **Análisis Temporal**: Seguimiento de evolución de precios y tiempo en mercado

## 🏗️ Arquitectura

```
inmobiliario/
├── database.py          # Gestión de SQLite y operaciones CRUD
├── scraper.py          # Motor de scraping con BeautifulSoup
├── app.py              # Dashboard de Streamlit
├── requirements.txt    # Dependencias Python
├── .env.example        # Plantilla de configuración
├── .env               # Credenciales (crear manualmente)
└── real_estate.db     # Base de datos (generada automáticamente)
```

## 🚀 Instalación

### 1. Clonar/Descargar el Proyecto

```bash
cd /Users/luisnuno/Downloads/workspace/inmobiliario
```

### 2. Crear Entorno Virtual (Recomendado)

```bash
python3 -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
```

### 3. Instalar Dependencias

```bash
pip install -r requirements.txt
```

### 4. Configurar Bright Data

1. Crea una cuenta en [Bright Data](https://brightdata.com)
2. Configura un Web Unlocker zone
3. Obtén tus credenciales (username, password, host)
4. Copia `.env.example` a `.env`:

```bash
cp .env.example .env
```

5. Edita `.env` con tus credenciales reales:

```env
BRIGHTDATA_USER=brd-customer-hl_xxxxx-zone-xxxxx
BRIGHTDATA_PASS=your_password_here
BRIGHTDATA_HOST=brd.superproxy.io:33335
```

## 📊 Uso

### Ejecutar el Scraper

El scraper debe ejecutarse diariamente para mantener los datos actualizados:

```bash
python scraper.py
```

**Duración estimada**: 30-60 minutos (depende de la cantidad de anuncios y velocidad del proxy)

**Proceso**:
1. Inicializa la base de datos si no existe
2. Carga IDs activos existentes
3. Recorre los 21 distritos de Madrid
4. Para cada distrito, pagina hasta el final
5. Extrae y parsea todos los anuncios
6. Actualiza la base de datos (inserta nuevos, actualiza existentes)
7. Marca como vendidos los anuncios que desaparecieron

### Lanzar el Dashboard

```bash
streamlit run app.py
```

El dashboard se abrirá automáticamente en tu navegador (por defecto: `http://localhost:8501`)

## 📈 Funcionalidades del Dashboard

### Filtros Interactivos (Sidebar)
- **Estado**: Activos / Vendidos / Todos
- **Distritos**: Selección múltiple de los 21 distritos
- **Rango de Precio**: Mínimo y máximo
- **Tipo de Vendedor**: Particular / Agencia / Todos

### Métricas Principales (KPIs)
- Precio Medio Total
- Precio Medio por m²
- Total de Inmuebles Activos
- Vendidos en los últimos 30 días

### Tablas
- Top 10 Barrios Más Caros
- Top 10 Barrios Más Baratos

### Visualizaciones
- **Histograma**: Distribución de precios
- **Gráfico de Barras**: Precio medio por distrito
- **Gráfico de Barras**: Tiempo medio en mercado (propiedades vendidas)
- **Gráfico de Línea**: Evolución temporal del precio medio

### Tabla de Datos Detallados
Vista completa de todas las propiedades con filtros aplicados

## 🗄️ Esquema de Base de Datos

```sql
CREATE TABLE listings (
    listing_id TEXT PRIMARY KEY,        -- ID único de Idealista
    title TEXT,                         -- Título del anuncio
    url TEXT,                           -- URL completa
    price INTEGER,                      -- Precio en euros
    distrito TEXT,                      -- Distrito de Madrid
    barrio TEXT,                        -- Barrio/zona
    rooms INTEGER,                      -- Número de habitaciones
    size_sqm REAL,                      -- Superficie en m²
    floor TEXT,                         -- Planta
    orientation TEXT,                   -- Interior/Exterior
    seller_type TEXT,                   -- Particular/Agencia
    is_new_development BOOLEAN,         -- Obra nueva
    first_seen_date TEXT,               -- Primera vez detectado
    last_seen_date TEXT,                -- Última vez visto
    status TEXT                         -- active/sold_removed
)
```

## 🎯 Estrategia de Scraping

### Problema: Límite de Paginación
Idealista limita los resultados a **60 páginas** (~1800 inmuebles) por búsqueda. Buscar "Madrid completo" perdería miles de propiedades.

### Solución: Scraping por Distritos
El scraper itera por los **21 distritos** de Madrid individualmente:

1. Centro
2. Arganzuela
3. Retiro
4. Salamanca
5. Chamartín
6. Tetuán
7. Chamberí
8. Fuencarral-El Pardo
9. Moncloa-Aravaca
10. Latina
11. Carabanchel
12. Usera
13. Puente de Vallecas
14. Moratalaz
15. Ciudad Lineal
16. Hortaleza
17. Villaverde
18. Villa de Vallecas
19. Vicálvaro
20. San Blas-Canillejas
21. Barajas

Cada distrito se pagina completamente, garantizando cobertura total del mercado.

## 🔍 Detección de Ventas

**Algoritmo ETL**:
1. Al iniciar, carga todos los IDs activos en memoria
2. Durante el scrape:
   - Si un ID **no existe** en BD → INSERT (nuevo anuncio)
   - Si un ID **ya existe** → UPDATE `last_seen_date` y precio
3. Al finalizar:
   - IDs que **no fueron vistos** → Marcar como `sold_removed`
   - **Métrica de venta**: `last_seen_date - first_seen_date` = días en mercado

**Nota**: "Vendido" incluye ventas reales, retiradas, y cambios a alquiler. No distingue entre estos casos.

## ⚙️ Configuración Avanzada

### Rate Limiting
El scraper incluye delays entre requests:
- 1 segundo entre páginas del mismo distrito
- 2 segundos entre distritos

Ajusta en `scraper.py` si experimentas bloqueos:
```python
time.sleep(1)  # Línea 283
time.sleep(2)  # Línea 327
```

### Timeout de Requests
Por defecto: 60 segundos. Ajusta si tienes conexión lenta:
```python
timeout=60  # Línea 95 en scraper.py
```

### Caché del Dashboard
Los datos se cachean 5 minutos. Cambia en `app.py`:
```python
@st.cache_data(ttl=300)  # 300 segundos = 5 minutos
```

## 🤖 Automatización (Opcional)

### Cron Job (Linux/Mac)

Ejecutar diariamente a las 3 AM:

```bash
crontab -e
```

Añade:
```
0 3 * * * cd /Users/luisnuno/Downloads/workspace/inmobiliario && /path/to/venv/bin/python scraper.py >> scraper.log 2>&1
```

### Task Scheduler (Windows)

1. Abre "Programador de tareas"
2. Crear tarea básica
3. Trigger: Diario a las 3:00 AM
4. Acción: Ejecutar `python.exe scraper.py`

## 📝 Campos Extraídos

| Campo | Selector CSS | Procesamiento |
|-------|-------------|---------------|
| `listing_id` | `article[data-element-id]` | Atributo directo |
| `title` | `a.item-link` | Texto |
| `url` | `a.item-link[href]` | Concatenar con BASE_URL |
| `price` | `span.item-price` | Limpiar "€" y puntos → int |
| `rooms` | `span.item-detail` (contiene "hab") | Extraer número |
| `size_sqm` | `span.item-detail` (contiene "m²") | Extraer float |
| `floor` | `span.item-detail` (contiene "Planta") | Texto completo |
| `orientation` | `span.item-detail` (interior/exterior) | Texto |
| `seller_type` | `span.logo-branding` | Existe → Agencia, No → Particular |
| `is_new_development` | `span.item-new-construction` | Boolean |

## ⚠️ Consideraciones Éticas

- **Respeta los Términos de Servicio** de Idealista
- **Rate Limiting**: No sobrecargues sus servidores
- **Uso Personal**: Este proyecto es para análisis personal/educativo
- **No Redistribuyas** datos scrapeados públicamente
- **Bright Data**: Asegúrate de cumplir con sus políticas de uso

## 🐛 Troubleshooting

### Error: "Bright Data credentials not configured"
- Verifica que `.env` existe y contiene las credenciales correctas
- Asegúrate de que `python-dotenv` está instalado

### Error: "No hay datos disponibles"
- Ejecuta primero `python scraper.py` para poblar la base de datos
- Verifica que `real_estate.db` existe en el directorio

### Dashboard no muestra gráficos
- Verifica que hay suficientes datos (al menos 10-20 propiedades)
- Revisa que los filtros no están excluyendo todos los datos

### Scraper se bloquea/falla
- Verifica credenciales de Bright Data
- Revisa el saldo de tu cuenta Bright Data
- Aumenta los delays entre requests
- Revisa logs para errores específicos

## 📚 Dependencias

- **requests**: HTTP requests con soporte de proxies
- **beautifulsoup4**: Parsing de HTML
- **streamlit**: Framework de dashboard
- **pandas**: Manipulación de datos
- **plotly**: Gráficos interactivos
- **python-dotenv**: Gestión de variables de entorno

## 📄 Licencia

Este proyecto es de código abierto para uso educativo y personal.

## 🤝 Contribuciones

Mejoras sugeridas:
- [ ] Notificaciones por email cuando se detectan nuevas propiedades
- [ ] Exportación de datos a CSV/Excel
- [ ] Análisis de cambios de precio históricos
- [ ] Integración con otros portales (Fotocasa, etc.)
- [ ] API REST para acceso programático
- [ ] Machine Learning para predicción de precios

## 📧 Soporte

Para problemas o preguntas, revisa:
1. Esta documentación
2. Comentarios en el código fuente
3. Logs de ejecución (`scraper.log` si configuraste cron)

---

**Desarrollado con ❤️ para el análisis del mercado inmobiliario de Madrid**
