
from database import get_connection
import pandas as pd
from datetime import datetime

def find_oldest_active_listing():
    with get_connection() as conn:
        # Get oldest valid listing
        query = """
            SELECT title, price, distrito, barrio, first_seen_date, url, listing_id
            FROM listings 
            WHERE status = 'active' 
            AND first_seen_date IS NOT NULL
            AND title NOT LIKE '%Test Property%'
            ORDER BY first_seen_date ASC 
            LIMIT 1
        """
        df = pd.read_sql_query(query, conn)
        
        if not df.empty:
            listing = df.iloc[0]
            print(f"🏠 Vivienda más antigua en venta:")
            print(f"  - Título: {listing['title']}")
            print(f"  - Precio: €{listing['price']:,}")
            print(f"  - Ubicación: {listing['barrio']} ({listing['distrito']})")
            print(f"  - Vista por primera vez: {listing['first_seen_date']}")
            print(f"  - Link: {listing['url']}")
            
            # Calculate days on market
            try:
                first_seen = datetime.strptime(listing['first_seen_date'], "%Y-%m-%d")
                days = (datetime.now() - first_seen).days
                print(f"  - Días en el mercado: {days} días")
            except Exception as e:
                print(f"Error parsing date: {e}")
        else:
            print("No hay propiedades activas válidas con fecha.")

if __name__ == "__main__":
    find_oldest_active_listing()
