# Streamlit Cloud Deployment Steps

## Paso Actual: Seleccionar Tipo de Despliegue

![Streamlit deployment options](/Users/luisnuno/.gemini/antigravity/brain/34cc1e68-3d6c-4ba8-bd3f-b078ee35e01f/uploaded_media_1769360515598.png)

**Selecciona:** "Deploy a public app from GitHub" (primera opción) → Click "Deploy now"

---

## Siguiente Pantalla: Configuración de la App

Después de hacer click en "Deploy now", verás un formulario con estos campos:

### 1. Repository
- **Repository:** `beastinblackcode/inmobiliario`
- **Branch:** `main`
- **Main file path:** `app.py`

### 2. App URL (opcional)
- Streamlit te asignará una URL automáticamente
- Formato: `https://[app-name]-[random-string].streamlit.app`
- Puedes personalizarla si quieres

### 3. Advanced settings (opcional)
- **Python version:** 3.11 (o la que uses)
- Puedes dejarlo en automático

---

## Después de Hacer Click en "Deploy"

1. **Verás los logs de construcción:**
   - Instalando dependencias...
   - Descargando paquetes...
   - Iniciando aplicación...

2. **Primer error esperado:**
   ```
   ❌ Database configuration missing in secrets
   ```
   
   **Esto es NORMAL** - aún no has configurado los secrets.

3. **Configurar Secrets:**
   - En la página de tu app, click en "⋮" (menú) → "Settings"
   - Ve a la pestaña "Secrets"
   - Pega esto:
   
   ```toml
   [database]
   google_drive_file_id = "TU_FILE_ID_DE_GOOGLE_DRIVE"
   ```
   
   - Click "Save"
   - Click "Reboot app"

4. **La app debería cargar correctamente:**
   - Descargará la base de datos de Google Drive
   - Mostrará el dashboard

---

## Recordatorio: Google Drive File ID

¿Ya subiste `real_estate.db` a Google Drive y obtuviste el file ID?

Si no lo has hecho:
1. Sube `real_estate.db` a Google Drive
2. Click derecho → "Compartir" → "Obtener enlace"
3. Configura: "Cualquiera con el enlace puede ver"
4. Copia el enlace: `https://drive.google.com/file/d/1a2b3c4d5e6f7g8h9i0j/view`
5. El file ID es: `1a2b3c4d5e6f7g8h9i0j`

---

## Troubleshooting

### "Repository not found"
- Verifica que el repositorio sea público o que Streamlit tenga acceso
- Ve a GitHub → Settings → Applications → Streamlit

### "No module named 'gdown'"
- Verifica que `requirements.txt` tenga `gdown>=4.7.1`
- Streamlit reinstalará automáticamente

### "Failed to download database"
- Verifica que el file ID sea correcto
- Verifica que el archivo en Google Drive sea público
- Prueba el enlace: `https://drive.google.com/uc?id=TU_FILE_ID`
