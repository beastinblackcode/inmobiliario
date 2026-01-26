"""
Coordinates mapping for Madrid barrios.
Provides latitude/longitude for each barrio to enable map visualization.
"""

from typing import Tuple, Optional

# Madrid barrio coordinates (centroids)
# Format: (distrito, barrio): (latitude, longitude)
BARRIO_COORDINATES = {
    # Arganzuela
    ("Arganzuela", "Acacias"): (40.3989, -3.7024),
    ("Arganzuela", "Chopera"): (40.3965, -3.6953),
    ("Arganzuela", "Delicias"): (40.3945, -3.6897),
    ("Arganzuela", "Imperial"): (40.4012, -3.7089),
    ("Arganzuela", "Legazpi"): (40.3891, -3.6889),
    ("Arganzuela", "Palos de la Frontera"): (40.3978, -3.6998),
    
    # Barajas
    ("Barajas", "Aeropuerto"): (40.4719, -3.5606),
    ("Barajas", "Alameda de Osuna"): (40.4525, -3.6089),
    ("Barajas", "Campo de las Naciones-Corralejos"): (40.4589, -3.6156),
    ("Barajas", "Casco Histórico de Barajas"): (40.4789, -3.5778),
    ("Barajas", "Timón"): (40.4856, -3.5889),
    
    # Salamanca
    ("Salamanca", "Castellana"): (40.4378, -3.6856),
    ("Salamanca", "Fuente del Berro"): (40.4289, -3.6656),
    ("Salamanca", "Goya"): (40.4265, -3.6732),
    ("Salamanca", "Guindalera"): (40.4412, -3.6678),
    ("Salamanca", "Lista"): (40.4312, -3.6789),
    ("Salamanca", "Recoletos"): (40.4238, -3.6886),
    
    # Carabanchel
    ("Carabanchel", "Abrantes"): (40.3756, -3.7456),
    ("Carabanchel", "Buena Vista"): (40.3889, -3.7389),
    ("Carabanchel", "Comillas"): (40.3812, -3.7289),
    ("Carabanchel", "Opañel"): (40.3867, -3.7256),
    ("Carabanchel", "PAU de Carabanchel"): (40.3723, -3.7523),
    ("Carabanchel", "Puerta Bonita"): (40.3823, -3.7156),
    ("Carabanchel", "San Isidro"): (40.3912, -3.7223),
    ("Carabanchel", "Vista Alegre"): (40.3856, -3.7323),
    
    # Centro
    ("Centro", "Chueca-Justicia"): (40.4245, -3.6989),
    ("Centro", "Huertas-Cortes"): (40.4156, -3.6989),
    ("Centro", "Lavapiés-Embajadores"): (40.4089, -3.7023),
    ("Centro", "Malasaña-Universidad"): (40.4267, -3.7056),
    ("Centro", "Palacio"): (40.4189, -3.7123),
    ("Centro", "Sol"): (40.4168, -3.7038),
    
    # Chamartín
    ("Chamartín", "Bernabéu-Hispanoamérica"): (40.4523, -3.6889),
    ("Chamartín", "Castilla"): (40.4678, -3.6789),
    ("Chamartín", "Ciudad Jardín"): (40.4589, -3.6656),
    ("Chamartín", "El Viso"): (40.4467, -3.6756),
    ("Chamartín", "Nueva España"): (40.4612, -3.6723),
    ("Chamartín", "Prosperidad"): (40.4534, -3.6723),
    
    # Chamberí
    ("Chamberí", "Almagro"): (40.4334, -3.6956),
    ("Chamberí", "Arapiles"): (40.4389, -3.7023),
    ("Chamberí", "Gaztambide"): (40.4378, -3.7123),
    ("Chamberí", "Nuevos Ministerios-Ríos Rosas"): (40.4456, -3.6923),
    ("Chamberí", "Trafalgar"): (40.4312, -3.7023),
    ("Chamberí", "Vallehermoso"): (40.4423, -3.7089),
    
    # Ciudad Lineal
    ("Ciudad Lineal", "Atalaya"): (40.4523, -3.6456),
    ("Ciudad Lineal", "Colina"): (40.4589, -3.6523),
    ("Ciudad Lineal", "Concepción"): (40.4456, -3.6523),
    ("Ciudad Lineal", "Costillares"): (40.4678, -3.6389),
    ("Ciudad Lineal", "Pueblo Nuevo"): (40.4412, -3.6456),
    ("Ciudad Lineal", "Quintana"): (40.4534, -3.6389),
    ("Ciudad Lineal", "San Juan Bautista"): (40.4467, -3.6589),
    ("Ciudad Lineal", "San Pascual"): (40.4389, -3.6589),
    ("Ciudad Lineal", "Ventas"): (40.4289, -3.6523),
    
    # Fuencarral-El Pardo
    ("Fuencarral-El Pardo", "Barrio del Pilar"): (40.4789, -3.7089),
    ("Fuencarral-El Pardo", "El Goloso"): (40.5123, -3.7456),
    ("Fuencarral-El Pardo", "El Pardo"): (40.5189, -3.7723),
    ("Fuencarral-El Pardo", "Fuentelarreina"): (40.4856, -3.7023),
    ("Fuencarral-El Pardo", "La Paz"): (40.4912, -3.6856),
    ("Fuencarral-El Pardo", "Mirasierra"): (40.4989, -3.7156),
    ("Fuencarral-El Pardo", "Peñagrande"): (40.4756, -3.7189),
    ("Fuencarral-El Pardo", "Tres Olivos-Valverde"): (40.4989, -3.6923),
    
    # Hortaleza
    ("Hortaleza", "Apóstol Santiago"): (40.4756, -3.6389),
    ("Hortaleza", "Canillas"): (40.4623, -3.6389),
    ("Hortaleza", "Conde Orgaz-Piovera"): (40.4523, -3.6289),
    ("Hortaleza", "Palomas"): (40.4856, -3.6456),
    ("Hortaleza", "Pinar del Rey"): (40.4789, -3.6523),
    ("Hortaleza", "Valdefuentes"): (40.4912, -3.6289),
    
    # Latina
    ("Latina", "Aluche"): (40.3923, -3.7523),
    ("Latina", "Campamento"): (40.3856, -3.7623),
    ("Latina", "Cuatro Vientos"): (40.3723, -3.7789),
    ("Latina", "Las Águilas"): (40.3989, -3.7623),
    ("Latina", "Los Cármenes"): (40.3889, -3.7456),
    ("Latina", "Lucero"): (40.3956, -3.7389),
    ("Latina", "Puerta del Ángel"): (40.4012, -3.7289),
    
    # Moncloa-Aravaca
    ("Moncloa-Aravaca", "Aravaca"): (40.4456, -3.7789),
    ("Moncloa-Aravaca", "Argüelles"): (40.4289, -3.7189),
    ("Moncloa-Aravaca", "Casa de Campo"): (40.4189, -3.7523),
    ("Moncloa-Aravaca", "Ciudad Universitaria"): (40.4467, -3.7289),
    ("Moncloa-Aravaca", "El Plantío"): (40.4523, -3.7623),
    ("Moncloa-Aravaca", "Valdezarza"): (40.4678, -3.7356),
    
    # Puente de Vallecas
    ("Puente de Vallecas", "Entrevías"): (40.3756, -3.6523),
    ("Puente de Vallecas", "Numancia"): (40.3889, -3.6656),
    ("Puente de Vallecas", "Palomeras Bajas"): (40.3812, -3.6389),
    ("Puente de Vallecas", "Palomeras Sureste"): (40.3723, -3.6289),
    ("Puente de Vallecas", "Portazgo"): (40.3867, -3.6589),
    ("Puente de Vallecas", "San Diego"): (40.3956, -3.6723),
    
    # Retiro
    ("Retiro", "Adelfas"): (40.4089, -3.6723),
    ("Retiro", "Estrella"): (40.4156, -3.6656),
    ("Retiro", "Ibiza"): (40.4189, -3.6589),
    ("Retiro", "Jerónimos"): (40.4134, -3.6856),
    ("Retiro", "Niño Jesús"): (40.4123, -3.6756),
    ("Retiro", "Pacífico"): (40.4067, -3.6656),
    
    # San Blas-Canillejas
    ("San Blas-Canillejas", "Arcos"): (40.4289, -3.6156),
    ("San Blas-Canillejas", "Canillejas"): (40.4412, -3.6089),
    ("San Blas-Canillejas", "Hellín"): (40.4356, -3.6223),
    ("San Blas-Canillejas", "Rejas"): (40.4478, -3.5989),
    ("San Blas-Canillejas", "Rosas"): (40.4334, -3.6289),
    ("San Blas-Canillejas", "Salvador"): (40.4389, -3.6356),
    ("San Blas-Canillejas", "Simancas"): (40.4234, -3.6223),
    
    # Tetuán
    ("Tetuán", "Bellas Vistas"): (40.4589, -3.7023),
    ("Tetuán", "Berruguete"): (40.4623, -3.7089),
    ("Tetuán", "Cuatro Caminos"): (40.4523, -3.7023),
    ("Tetuán", "Cuzco-Castillejos"): (40.4656, -3.6956),
    ("Tetuán", "Almenara"): (40.4712, -3.6889),
    ("Tetuán", "Valdeacederas"): (40.4678, -3.7023),
    
    # Usera
    ("Usera", "Almendrales"): (40.3789, -3.7089),
    ("Usera", "Moscardó"): (40.3856, -3.7023),
    ("Usera", "Orcasitas"): (40.3723, -3.7023),
    ("Usera", "Orcasur"): (40.3689, -3.6956),
    ("Usera", "Pradolongo"): (40.3812, -3.7156),
    ("Usera", "San Fermín"): (40.3756, -3.7156),
    ("Usera", "Zofío"): (40.3823, -3.7089),
    
    # Vicálvaro
    ("Vicálvaro", "Ambroz"): (40.3989, -3.5989),
    ("Vicálvaro", "Casco Histórico de Vicálvaro"): (40.4023, -3.6089),
    ("Vicálvaro", "Los Ahijones"): (40.3856, -3.5856),
    ("Vicálvaro", "Los Berrocales"): (40.3789, -3.5756),
    ("Vicálvaro", "Los Cerros"): (40.3912, -3.5923),
    ("Vicálvaro", "Valdebernardo-Valderrivas"): (40.3956, -3.6023),
    
    # Villa de Vallecas
    ("Villa de Vallecas", "Casco Histórico de Vallecas"): (40.3723, -3.6089),
    ("Villa de Vallecas", "Ensanche de Vallecas"): (40.3656, -3.5956),
    ("Villa de Vallecas", "Santa Eugenia"): (40.3789, -3.6156),
    
    # Villaverde
    ("Villaverde", "Butarque"): (40.3456, -3.7089),
    ("Villaverde", "Los Ángeles"): (40.3523, -3.7023),
    ("Villaverde", "Los Rosales"): (40.3589, -3.7156),
    ("Villaverde", "San Andrés"): (40.3612, -3.7089),
    ("Villaverde", "San Cristóbal"): (40.3556, -3.6956),
    ("Villaverde", "Villaverde Alto"): (40.3489, -3.7023),
}

# Madrid center as fallback
MADRID_CENTER = (40.4168, -3.7038)


def get_barrio_coordinates(distrito: str, barrio: str) -> Tuple[float, float]:
    """
    Get latitude and longitude for a Madrid barrio.
    
    Args:
        distrito: District name
        barrio: Barrio (neighborhood) name
        
    Returns:
        Tuple of (latitude, longitude). Returns Madrid center if barrio not found.
    """
    return BARRIO_COORDINATES.get((distrito, barrio), MADRID_CENTER)


def get_all_coordinates() -> dict:
    """
    Get all barrio coordinates as a dictionary.
    
    Returns:
        Dictionary mapping (distrito, barrio) tuples to (lat, lon) tuples
    """
    return BARRIO_COORDINATES.copy()
