import sqlite3

# Hardcoded list of configured barrios (from BARRIO_URLS)
configured_barrios = [
    ("Arganzuela", "Acacias"), ("Arganzuela", "Chopera"), ("Arganzuela", "Delicias"),
    ("Arganzuela", "Imperial"), ("Arganzuela", "Legazpi"), ("Arganzuela", "Palos de Moguer"),
    ("Barajas", "Aeropuerto"), ("Barajas", "Alameda de Osuna"), ("Barajas", "Casco Histórico de Barajas"),
    ("Barajas", "Corralejos"), ("Barajas", "Timón"), ("Carabanchel", "Abrantes"),
    ("Carabanchel", "Buenavista"), ("Carabanchel", "Comillas"), ("Carabanchel", "Opañel"),
    ("Carabanchel", "Puerta Bonita"), ("Carabanchel", "San Isidro"), ("Carabanchel", "Vista Alegre"),
    ("Carabanchel", "Vistalegre-La Chimenea"), ("Centro", "Cortes"), ("Centro", "Embajadores"),
    ("Centro", "Justicia"), ("Centro", "Palacio"), ("Centro", "Sol"), ("Centro", "Universidad"),
    ("Chamartín", "Castilla"), ("Chamartín", "Ciudad Jardín"), ("Chamartín", "El Viso"),
    ("Chamartín", "Hispanoamérica"), ("Chamartín", "Nueva España"), ("Chamartín", "Prosperidad"),
    ("Chamberí", "Almagro"), ("Chamberí", "Arapiles"), ("Chamberí", "Gaztambide"),
    ("Chamberí", "Ríos Rosas"), ("Chamberí", "Trafalgar"), ("Chamberí", "Vallehermoso"),
    ("Ciudad Lineal", "Atalaya"), ("Ciudad Lineal", "Colina"), ("Ciudad Lineal", "Concepción"),
    ("Ciudad Lineal", "Costillares"), ("Ciudad Lineal", "Pueblo Nuevo"), ("Ciudad Lineal", "Quintana"),
    ("Ciudad Lineal", "San Juan Bautista"), ("Ciudad Lineal", "San Pascual"), ("Ciudad Lineal", "Ventas"),
    ("Fuencarral-El Pardo", "Barrio del Pilar"), ("Fuencarral-El Pardo", "El Pardo"),
    ("Fuencarral-El Pardo", "Fuentelarreina"), ("Fuencarral-El Pardo", "La Paz"),
    ("Fuencarral-El Pardo", "Mirasierra"), ("Fuencarral-El Pardo", "Peñagrande"),
    ("Fuencarral-El Pardo", "Tres Olivos-Valverde"), ("Fuencarral-El Pardo", "Valverde"),
    ("Fuencarral-El Pardo", "El Goloso"), ("Hortaleza", "Apóstol Santiago"),
    ("Hortaleza", "Canillas"), ("Hortaleza", "Palomas"), ("Hortaleza", "Pinar del Rey"),
    ("Hortaleza", "Piovera"), ("Hortaleza", "Sanchinarro"), ("Hortaleza", "Valdebebas-Valdefuentes"),
    ("Hortaleza", "Virgen del Cortijo-Manoteras"), ("Hortaleza", "Pinar de Chamartín"),
    ("Latina", "Águilas"), ("Latina", "Aluche"), ("Latina", "Campamento"),
    ("Latina", "Cuatro Vientos"), ("Latina", "Los Cármenes"), ("Latina", "Lucero"),
    ("Latina", "Puerta del Ángel"), ("Latina", "Batán"), ("Moncloa-Aravaca", "Aravaca"),
    ("Moncloa-Aravaca", "Argüelles"), ("Moncloa-Aravaca", "Casa de Campo"),
    ("Moncloa-Aravaca", "Ciudad Universitaria"), ("Moncloa-Aravaca", "El Plantío"),
    ("Moncloa-Aravaca", "Valdemarín"), ("Moncloa-Aravaca", "Valdezarza"),
    ("Moncloa-Aravaca", "Dehesa de la Villa"), ("Moratalaz", "Fontarrón"),
    ("Moratalaz", "Horcajo"), ("Moratalaz", "Marroquina"), ("Moratalaz", "Media Legua"),
    ("Moratalaz", "Pavones"), ("Moratalaz", "Vinateros"), ("Moratalaz", "Arroyo del Olivar"),
    ("Puente de Vallecas", "Entrevías"), ("Puente de Vallecas", "Numancia"),
    ("Puente de Vallecas", "Palomeras Bajas"), ("Puente de Vallecas", "Palomeras Sureste"),
    ("Puente de Vallecas", "Portazgo"), ("Puente de Vallecas", "San Diego"),
    ("Puente de Vallecas", "Doña Carlota"), ("Puente de Vallecas", "Pozo del Tío Raimundo"),
    ("Retiro", "Adelfas"), ("Retiro", "Estrella"), ("Retiro", "Ibiza"),
    ("Retiro", "Jerónimos"), ("Retiro", "Niño Jesús"), ("Retiro", "Pacífico"),
    ("Salamanca", "Castellana"), ("Salamanca", "Goya"), ("Salamanca", "Guindalera"),
    ("Salamanca", "Lista"), ("Salamanca", "Recoletos"), ("Salamanca", "Fuente del Berro"),
    ("San Blas-Canillejas", "Amposta"), ("San Blas-Canillejas", "Arcos"),
    ("San Blas-Canillejas", "Canillejas"), ("San Blas-Canillejas", "Hellín"),
    ("San Blas-Canillejas", "Rejas"), ("San Blas-Canillejas", "Rosas"),
    ("San Blas-Canillejas", "Salvador"), ("San Blas-Canillejas", "Simancas"),
    ("Tetuán", "Bellas Vistas"), ("Tetuán", "Berruguete"), ("Tetuán", "Cuatro Caminos"),
    ("Tetuán", "Cuzco-Castillejos"), ("Tetuán", "Valdeacederas"), ("Tetuán", "Ventilla-Almenara"),
    ("Usera", "12 de Octubre-Orcasur"), ("Usera", "Almendrales"), ("Usera", "Moscardó"),
    ("Usera", "Orcasitas"), ("Usera", "Pradolongo"), ("Usera", "San Fermín"),
    ("Usera", "Zofío"), ("Vicálvaro", "Ambroz"), ("Vicálvaro", "Casco Histórico de Vicálvaro"),
    ("Vicálvaro", "El Cañaveral"), ("Vicálvaro", "Los Ahijones"), ("Vicálvaro", "Los Berrocales"),
    ("Vicálvaro", "Los Cerros"), ("Vicálvaro", "Valdebernardo-Valderrivas"),
    ("Villa de Vallecas", "Casco Histórico de Vallecas"), ("Villa de Vallecas", "Ensanche de Vallecas-La Gavia"),
    ("Villa de Vallecas", "Santa Eugenia"), ("Villa de Vallecas", "Valdecarros"),
    ("Villaverde", "Butarque"), ("Villaverde", "Los Ángeles"), ("Villaverde", "Los Rosales"),
    ("Villaverde", "San Cristóbal"), ("Villaverde", "Villaverde Alto")
]

# Get barrios scraped today
conn = sqlite3.connect('real_estate.db')
cursor = conn.cursor()
cursor.execute("""
    SELECT DISTINCT distrito, barrio
    FROM listings
    WHERE last_seen_date = '2026-02-11'
""")
scraped_today = set(cursor.fetchall())
conn.close()

# Find missing
configured = set(configured_barrios)
missing = configured - scraped_today

print(f"📊 ANÁLISIS DE SCRAPING (2026-02-11)")
print("=" * 80)
print(f"Barrios configurados: {len(configured)}")
print(f"Barrios scrapeados hoy: {len(scraped_today)}")
print(f"Barrios NO scrapeados: {len(missing)}")
print()

if missing:
    print("❌ BARRIOS NO SCRAPEADOS HOY:")
    print("=" * 80)
    for distrito, barrio in sorted(missing):
        print(f"  • {distrito} - {barrio}")
