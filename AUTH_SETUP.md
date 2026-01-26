# Authentication Setup Guide

## Configurar Credenciales en Streamlit Cloud

Para activar la autenticación, necesitas añadir las credenciales a los secrets de Streamlit Cloud.

### Paso 1: Ir a Settings → Secrets

1. Abre tu app en Streamlit Cloud: `inmobiliario-beastinblackcode.streamlit.app`
2. Click en el menú "⋮" → "Settings"
3. Ve a la pestaña "Secrets"

### Paso 2: Actualizar Secrets

Reemplaza el contenido actual con esto (añadiendo la sección `[auth]`):

```toml
[database]
google_drive_file_id = "1ajdgLaneXwb6OWl_S727gwyYZUfrdF7p"

[auth]
username = "admin"
password = "TuContraseñaSegura123"
```

**Importante:**
- Cambia `"admin"` por el usuario que prefieras
- Cambia `"TuContraseñaSegura123"` por una contraseña segura
- Usa una contraseña diferente a tus otras cuentas
- Recomendación: mínimo 12 caracteres, mezcla de letras, números y símbolos

### Paso 3: Guardar y Reiniciar

1. Click en **"Save"**
2. Click en **"Reboot app"**
3. Espera ~30 segundos

### Paso 4: Probar

1. Refresca la página de tu app
2. Deberías ver la pantalla de login:
   - 🔐 Acceso al Dashboard
   - Campo "Usuario"
   - Campo "Contraseña"
   - Botón "Iniciar Sesión"

3. Introduce tus credenciales
4. Si son correctas, verás el dashboard
5. Si son incorrectas, verás: "😕 Usuario o contraseña incorrectos"

---

## Características de Seguridad

### ✅ Implementado

- **Autenticación por sesión**: No pide contraseña en cada recarga
- **Credenciales encriptadas**: Almacenadas en Streamlit secrets (encriptadas)
- **HTTPS**: Todas las comunicaciones son seguras
- **robots.txt**: Bloquea crawlers de búsqueda
- **Validación de credenciales**: Compara con secrets de forma segura

### 🔒 Recomendaciones

1. **Contraseña fuerte**: Usa un gestor de contraseñas
2. **No compartir**: Solo comparte con personas de confianza
3. **Cambiar periódicamente**: Actualiza la contraseña cada 3-6 meses
4. **Cerrar sesión**: Borra cookies del navegador si usas un ordenador compartido

---

## Cambiar Credenciales

Para cambiar usuario o contraseña:

1. Streamlit Cloud → Settings → Secrets
2. Modifica los valores en `[auth]`
3. Save → Reboot app
4. Las nuevas credenciales estarán activas inmediatamente

---

## Troubleshooting

### "Usuario o contraseña incorrectos" (pero son correctos)

**Posibles causas:**
- Espacios extra en el usuario o contraseña en secrets
- Mayúsculas/minúsculas (la contraseña es case-sensitive)
- Comillas mal cerradas en secrets

**Solución:**
```toml
# ❌ Incorrecto
username = " admin "  # espacios extra
password = TuContraseña  # falta comillas

# ✅ Correcto
username = "admin"
password = "TuContraseña"
```

### La app no pide contraseña

**Causa:** Secrets no configurados correctamente

**Solución:**
1. Verifica que la sección `[auth]` existe en secrets
2. Verifica que `username` y `password` están definidos
3. Reboot app

### Olvidé mi contraseña

**Solución:**
1. Ve a Streamlit Cloud → Settings → Secrets
2. Cambia el valor de `password`
3. Save → Reboot app

---

## Ejemplo de Configuración Completa

```toml
[database]
google_drive_file_id = "1ajdgLaneXwb6OWl_S727gwyYZUfrdF7p"

[auth]
username = "admin"
password = "Mi$uper$ecur3P@ssw0rd!"
```

---

## Seguridad Adicional

### Hacer el Repositorio Privado (Recomendado)

Si aún no lo has hecho:

1. GitHub → Tu repo → Settings
2. Scroll hasta "Danger Zone"
3. "Change visibility" → "Make private"
4. Confirma

Esto evita que alguien vea el código fuente y entienda cómo funciona la app.

### Limitar Acceso por IP (Avanzado)

Streamlit Cloud no soporta esto nativamente, pero puedes:
- Usar Cloudflare (gratis) como proxy
- Configurar reglas de acceso por país/IP
- Requiere configuración de DNS personalizado

---

## Próximos Pasos

Una vez configurado:
1. ✅ Prueba el login con credenciales correctas
2. ✅ Prueba con credenciales incorrectas
3. ✅ Verifica que la sesión persiste al recargar
4. ✅ Comparte las credenciales solo con personas autorizadas
