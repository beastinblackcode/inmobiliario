# GitHub Authentication Fix

GitHub ya no acepta contraseñas para operaciones Git. Necesitas usar un Personal Access Token (PAT).

## Solución Rápida: Crear Personal Access Token

### 1. Crear el Token

1. Ve a GitHub: https://github.com/settings/tokens
2. Click en "Generate new token" → "Generate new token (classic)"
3. Configuración:
   - **Note:** "Inmobiliario deployment"
   - **Expiration:** 90 days (o lo que prefieras)
   - **Scopes:** Marca estas opciones:
     - ✅ `repo` (Full control of private repositories)
     - ✅ `workflow` (Update GitHub Action workflows)
4. Click "Generate token"
5. **IMPORTANTE:** Copia el token AHORA (solo se muestra una vez)
   - Formato: `ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`

### 2. Usar el Token

Cuando Git te pida la contraseña, **pega el token** en lugar de tu contraseña:

```bash
git push -u origin main
Username: beastinblackcode
Password: [PEGA_TU_TOKEN_AQUÍ]  # ghp_xxxx...
```

### 3. Guardar el Token (Opcional pero Recomendado)

Para no tener que ingresarlo cada vez:

```bash
# Configurar Git para recordar credenciales
git config --global credential.helper osxkeychain

# Ahora haz push (te pedirá el token una vez)
git push -u origin main

# Git guardará el token en el Keychain de macOS
```

---

## Alternativa: Usar SSH (Más Seguro)

Si prefieres usar SSH en lugar de HTTPS:

### 1. Generar clave SSH

```bash
# Generar nueva clave SSH
ssh-keygen -t ed25519 -C "tu_email@example.com"

# Presiona Enter para aceptar la ubicación por defecto
# Opcionalmente, añade una passphrase

# Iniciar el agente SSH
eval "$(ssh-agent -s)"

# Añadir la clave al agente
ssh-add ~/.ssh/id_ed25519
```

### 2. Añadir clave a GitHub

```bash
# Copiar la clave pública al portapapeles
pbcopy < ~/.ssh/id_ed25519.pub
```

1. Ve a GitHub: https://github.com/settings/keys
2. Click "New SSH key"
3. Title: "MacBook Air"
4. Key: Pega la clave (ya está en tu portapapeles)
5. Click "Add SSH key"

### 3. Cambiar remote a SSH

```bash
# Cambiar de HTTPS a SSH
git remote set-url origin git@github.com:beastinblackcode/inmobiliario.git

# Verificar
git remote -v

# Ahora push funcionará sin contraseña
git push -u origin main
```

---

## Recomendación

**Para este proyecto:** Usa **Personal Access Token** (Opción 1) - es más rápido.

**Para uso frecuente:** Configura **SSH** (Opción 2) - es más seguro y conveniente a largo plazo.

---

## Troubleshooting

### "Token doesn't have the required scopes"
- Asegúrate de marcar el scope `repo` al crear el token

### "Token expired"
- Los tokens expiran. Crea uno nuevo en https://github.com/settings/tokens

### "SSH key already in use"
- Cada clave SSH solo puede usarse en una cuenta
- Genera una nueva clave o usa un token en su lugar
