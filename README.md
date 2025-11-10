# tensor_jefe_monitoreo_sistemas_2025- noviembre 2025
sistema para monitorear los equipos de tensor/vesat

## condigo lectura tablas: 
Este codigo lee las tablas del servidor centralizado(plc, horometros y pesometros,.)
si el ultimo dato es menor en tiempo a un delta(5 minutos) toma ese registro, junto con la hora de sincronizacion y luego
lee el registro del serbidor remoto de esa planta. Si se el registro remoto es actual(esta operando) y la hora
de sincronziacion es mayor a registro remoto, borra los ultimo 30 registros del centraliado para volver a sincronizar.
cuando elimina, deja log y registro en BD

## log_sincronizador
  hace una consulta a BD remotas para saber si existe conexion a las BD, si no existe deja log y registro 