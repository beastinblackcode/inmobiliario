#!/usr/bin/env python3
"""
Script to analyze 404 errors and generate removal commands for scraper.py
"""

import re
from collections import defaultdict

def analyze_404_log():
    """Analyze 404_errors.log and generate removal recommendations"""
    
    try:
        with open('404_errors.log', 'r') as f:
            urls = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("❌ No se encontró 404_errors.log")
        print("   Ejecuta el scraper primero para generar el log")
        return
    
    if not urls:
        print("✅ No hay errores 404 registrados")
        return
    
    # Parse URLs and group by barrio
    barrio_errors = defaultdict(int)
    url_details = []
    
    for url in urls:
        # Extract distrito and barrio from URL
        match = re.search(r'/venta-viviendas/madrid/([^/]+)/([^/]+)/', url)
        if match:
            distrito_slug = match.group(1)
            barrio_slug = match.group(2)
            key = f"{distrito_slug}/{barrio_slug}"
            barrio_errors[key] += 1
            url_details.append((distrito_slug, barrio_slug, url))
    
    # Report
    print("=" * 80)
    print("📊 ANÁLISIS DE ERRORES 404")
    print("=" * 80)
    print(f"\nTotal de URLs con 404: {len(urls)}")
    print(f"Barrios únicos con 404: {len(barrio_errors)}")
    print()
    
    print("❌ BARRIOS CON ERRORES 404:")
    print("=" * 80)
    for (distrito_barrio, count) in sorted(barrio_errors.items(), key=lambda x: x[1], reverse=True):
        distrito, barrio = distrito_barrio.split('/')
        print(f"  {count:3d} errores - {distrito}/{barrio}")
    
    print()
    print("🗑️  URLs COMPLETAS A ELIMINAR:")
    print("=" * 80)
    seen = set()
    for distrito, barrio, url in url_details:
        key = f"{distrito}/{barrio}"
        if key not in seen:
            print(f"  {url}")
            seen.add(key)
    
    # Generate Python code to remove from scraper
    print()
    print("📝 BARRIOS A BUSCAR Y ELIMINAR EN scraper.py:")
    print("=" * 80)
    print("Busca estas líneas en BARRIO_URLS y elimínalas:")
    print()
    
    seen = set()
    for distrito, barrio, url in url_details:
        key = f"{distrito}/{barrio}"
        if key not in seen:
            # Try to guess the distrito and barrio names
            distrito_name = distrito.replace('-', ' ').title()
            barrio_name = barrio.replace('-', ' ').title()
            print(f'    # ("{distrito_name}", "{barrio_name}", "{url.replace("https://www.idealista.com", "")}"),')
            seen.add(key)

if __name__ == "__main__":
    analyze_404_log()
