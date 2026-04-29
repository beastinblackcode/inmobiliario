# Multi-User Authentication Setup

## Configuración de Múltiples Usuarios

Ahora puedes configurar varios usuarios, cada uno con su propia contraseña.

### Formato de Secrets

En **Streamlit Cloud → Settings → Secrets**, usa este formato:

```toml
[database]
google_drive_file_id = "1ajdgLaneXwb6OWl_S727gwyYZUfrdF7p"

[auth.users]
admin = "ContraseñaAdmin123"
luis = "ContraseñaLuis456"
maria = "ContraseñaMaria789"
juan = "ContraseñaJuan012"
```

### Características

- ✅ Cada usuario tiene su propia contraseña
- ✅ Puedes añadir/eliminar usuarios fácilmente
- ✅ El usuario actual se muestra en el sidebar
- ✅ Compatible con el formato anterior (un solo usuario)

### Añadir un Nuevo Usuario

1. Ve a **Streamlit Cloud → Settings → Secrets**
2. Añade una nueva línea en `[auth.users]`:
   ```toml
   nuevo_usuario = "ContraseñaNueva"
   ```
3. **Save** → **Reboot app**

### Eliminar un Usuario

1. Ve a **Streamlit Cloud → Settings → Secrets**
2. Borra la línea del usuario
3. **Save** → **Reboot app**

### Cambiar Contraseña de un Usuario

1. Ve a **Streamlit Cloud → Settings → Secrets**
2. Modifica la contraseña del usuario:
   ```toml
   luis = "NuevaContraseña789"
   ```
3. **Save** → **Reboot app**

---

## Ejemplo Completo

```toml
[database]
google_drive_file_id = "1ajdgLaneXwb6OWl_S727gwyYZUfrdF7p"

[auth.users]
# Administrador principal
admin = "SuperSecureAdmin2024!"

# Equipo de análisis
luis = "AnalystPass456#"
maria = "DataTeam789$"

# Usuarios externos
cliente1 = "ClientAccess123@"
```

---

## Retrocompatibilidad

Si prefieres mantener un solo usuario, el formato antiguo sigue funcionando:

```toml
[database]
google_drive_file_id = "1ajdgLaneXwb6OWl_S727gwyYZUfrdF7p"

[auth]
username = "admin"
password = "MiContraseña123"
```

---

## Indicador de Usuario

Una vez autenticado, el sidebar mostrará:

```
---
☁️ Deployed on Streamlit Cloud
👤 Usuario: luis
```

Esto te permite saber quién está usando la app en cada momento.

---

## Seguridad

### Recomendaciones de Contraseñas

- **Mínimo 12 caracteres**
- **Mezcla de mayúsculas, minúsculas, números y símbolos**
- **Diferente para cada usuario**
- **No reutilizar contraseñas de otras cuentas**

### Ejemplo de Contraseñas Seguras

```toml
[auth.users]
admin = "Adm!n2024$ecur3Pass"
luis = "Lu!s#Analyt1cs789"
maria = "M@r1a_D@t@2024!"
```

### Generador de Contraseñas

Puedes usar herramientas como:
- 1Password
- LastPass
- Bitwarden
- O el generador de tu navegador

---

## Troubleshooting

### "Usuario o contraseña incorrectos"

**Verifica:**
1. El nombre de usuario es exacto (case-sensitive)
2. La contraseña no tiene espacios extra
3. El formato TOML es correcto (comillas bien cerradas)

### No aparece el usuario en el sidebar

**Causa:** Versión antigua del código

**Solución:** Espera a que Streamlit Cloud redesplegue (~2 minutos)

### Quiero volver al modo de un solo usuario

**Solución:** Cambia el formato de secrets:

```toml
[auth]
username = "admin"
password = "MiContraseña"
```

El código detectará automáticamente el formato y usará el modo correcto.
