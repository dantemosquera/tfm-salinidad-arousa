## Configuración Inicial para Notebooks

Para asegurar que la conexión a la base de datos, las proyecciones geográficas (PROJ) y los gráficos funcionen correctamente en Windows,
**debes ejecutar este bloque de código en la PRIMERA celda** de cualquier notebook nuevo:

```python
import sys
import os

# 1. Agregar el directorio raíz del proyecto al path
# (Permite importar módulos de la carpeta 'src' desde 'notebooks')
sys.path.append(os.path.abspath(".."))

# 2. Importar la configuración maestra
# (Asegúrate de que el archivo en src se llame 'config_env.py')
from src.config_env import inicializar_entorno

# 3. Inicializar el entorno (Parches DLL, PROJ y .env)
inicializar_entorno()