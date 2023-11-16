cleanup() {
    echo "Realizando limpieza antes de detener el contenedor..."
    
    # Borra archivos temporales o realiza otras acciones necesarias
    rm -f /ruta/al/archivo/temporal
    
    echo "Limpieza completada. Deteniendo el contenedor."
    
    # Continúa con la terminación normal del contenedor
    exit "$1"
}

# Captura la señal SIGTERM para ejecutar la función cleanup antes de detener el contenedor
trap 'cleanup' SIGTERM

# Ejecuta el comando principal del contenedor
exec "$@"