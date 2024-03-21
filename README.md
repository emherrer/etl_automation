## Motivación

Importar archivos CSV a una base de datos es una tarea común en la ingenieria y análisis de datos.  El objetivo de este proyecto es crear un "pipeline" para automatizar esta tarea, a fin de mejorar la eficiencia y mitigar errores en el proceso.

Este script importa automáticamente archivos CSV a una base de datos Postgres alojada en AWS.  Simplemente coloque los archivos CSV en el mismo directorio que el notebbok y ejecútelo. El notebook limpiara automáticamente el nombre del archivo y los encabezados de las columnas, creará la tabla de base de datos y copiará el archivo a la base de datos. 

Los nombres de las tablas son los mismos nombres que los nombres de los archivos, sin embargo, todos los caracteres en mayúsculas se cambian a minúsculas, los espacios se convierten en guiones bajos y se eliminan todos los símbolos.