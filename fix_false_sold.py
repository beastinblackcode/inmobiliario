#!/usr/bin/env python3
"""
Fix properties falsely marked as sold_removed due to scraper coverage gaps.

Analysis found:
- 4,295 properties marked "sold" on 2026-01-20 (massive spike, scraper gap)
- Other spike dates (>100 "sales"/day before 2026-02-14) also suspicious
- These result from mark_stale_as_sold(7) marking properties not seen 
  for 7 days, but the scraper failed to cover those barrios

This script:
1. Reactivates properties from the 01-20 spike
2. Reactivates properties from other anomalous spike dates
3. Reports before/after statistics
"""

import sqlite3

def main():
    conn = sqlite3.connect('real_estate.db')
    c = conn.cursor()
    
    print("=" * 70)
    print("CORRECCIÓN DE PROPIEDADES FALSAMENTE MARCADAS COMO VENDIDAS")
    print("=" * 70)
    
    # --- Before ---
    print("\n📊 ANTES:")
    c.execute("SELECT status, COUNT(*) FROM listings GROUP BY status")
    for r in c.fetchall():
        print(f"  {r[0]}: {r[1]:,}")
    
    # --- Identify spikes ---
    # Criterion 1: Spike of 01-20 (4,295 properties)
    c.execute("""
        SELECT COUNT(*) FROM listings 
        WHERE status='sold_removed' AND last_seen_date='2026-01-20'
    """)
    spike_20 = c.fetchone()[0]
    print(f"\n🔍 Pico 20/01: {spike_20:,}")
    
    # Criterion 2: Other spikes >= 100/day before 14/02
    c.execute("""
        SELECT last_seen_date, COUNT(*) as cnt
        FROM listings
        WHERE status='sold_removed' 
          AND last_seen_date < '2026-02-14' 
          AND last_seen_date != '2026-01-20'
        GROUP BY last_seen_date 
        HAVING cnt >= 100
        ORDER BY last_seen_date
    """)
    other_spikes = c.fetchall()
    for r in other_spikes:
        print(f"   Pico {r[0]}: {r[1]:,}")
    spike_dates = [r[0] for r in other_spikes]
    
    # --- Execute correction ---
    # Fix spike 01-20
    c.execute("""
        UPDATE listings 
        SET status = 'active' 
        WHERE status = 'sold_removed' AND last_seen_date = '2026-01-20'
    """)
    fixed_20 = c.rowcount
    print(f"\n✅ Reactivadas del 20/01: {fixed_20:,}")
    
    # Fix other spikes
    fixed_other = 0
    if spike_dates:
        placeholders = ",".join("?" * len(spike_dates))
        c.execute(f"""
            UPDATE listings 
            SET status = 'active' 
            WHERE status = 'sold_removed' 
              AND last_seen_date IN ({placeholders})
        """, tuple(spike_dates))
        fixed_other = c.rowcount
        print(f"✅ Reactivadas de otros picos: {fixed_other:,}")
    
    conn.commit()
    
    # --- After ---
    print("\n📊 DESPUÉS:")
    c.execute("SELECT status, COUNT(*) FROM listings GROUP BY status")
    for r in c.fetchall():
        print(f"  {r[0]}: {r[1]:,}")
    
    # Remaining sold analysis
    c.execute("""
        SELECT last_seen_date, COUNT(*) as cnt
        FROM listings WHERE status='sold_removed'
        GROUP BY last_seen_date ORDER BY cnt DESC LIMIT 10
    """)
    print("\n  Top fechas sold restantes:")
    for r in c.fetchall():
        print(f"    {r[0]}: {r[1]:,}")
    
    total_fixed = fixed_20 + fixed_other
    print(f"\n🎯 TOTAL REACTIVADAS: {total_fixed:,}")
    
    conn.close()


if __name__ == "__main__":
    main()
