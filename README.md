# Uso de SparkSession, DataFrame y DataSet en Spark 2.x con Scala

En este proyecto se utiliza Spark 2.x para realizar diversas tareas de análisis de datos relacionadas con información sobre nacimientos. Se utilizarán SparkSession, DataFrame y DataSet para llevar a cabo las siguientes tareas:

## Utilizando el API DataFrame

0. Limpieza de datos

1. **Top 10 Estados con Mayor Número de Bebés Nacidos en 2003:**
   - Obtener los 10 estados donde nacieron más bebés en el año 2003.

2. **Media de Peso de los Bebés por Año y Estado:**
   - Calcular la media de peso de los bebés por año y estado.

3. **Evolución del Número de Niños y Niñas Nacidos por Año y Mes:**
   - Obtener la evolución del número de niños y niñas nacidos por año y mes, presentando el resultado en dos tablas separadas.

4. **Los Tres Meses de 2005 con Más Bebés Nacidos:**
   - Identificar los tres meses de 2005 en los que nacieron más bebés.

5. **Estados con Semanas de Gestación Superiores a la Media Nacional:**
   - Encontrar los estados donde las semanas de gestación son superiores a la media de EE. UU.

6. **Top 5 Estados con Mayor Media de Edad de las Madres:**
   - Identificar los cinco estados donde la media de edad de las madres ha sido mayor.

7. **Influencia del Parto Múltiple en el Peso del Bebé y las Semanas de Gestación:**
   - Analizar cómo influye el parto múltiple (campo plurality) en el peso del bebé y las semanas de gestación en comparación con los que no lo han tenido.

## Utilizando el Lenguaje SQL

Para cada una de las tareas anteriores, también se realizará una implementación utilizando el lenguaje SQL para proporcionar una alternativa a las consultas con API DataFrame.
