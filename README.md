# Proyectos del Informatorio: Análisis de datos.
---
## **Proyecto-1:**
Objetivo: Desarrollar una aplicación modular en Python que permita cargar al menos 2 tipos de archivos de datos (.csv, .xlsx, .json, .txt, etc) de su elección, realizar validaciones básicas y guardarlos en una base de datos relacional utilizando SQLAlchemy. El desarrollo debe demostrar claramente los cuatro pilares de la POO, una estructura modular, y ser ejecutable desde consola mediante una clase principal.

### **1. Requisitos Técnicos:**
**Lenguaje y Paradigma:**
 - Python (3.10+)
 - Paradigma: Programación Orientada a Objetos (POO)
 - Manejo de Archivos:
 - Soporte para .csv, .xlsx, .json, .txt, etc
 - Carga de datos desde archivos de su elección

**Validaciones mínimas:**
 - Validar tipos de datos (número, texto, fecha)
 - Validar campos obligatorios (no nulos)
 - Eliminar duplicados
 - Eliminar valores nulos

**Persistencia de datos:**
 - Uso de SQLAlchemy con engine
 - Una tabla por archivo cargado

**Flujo esperado de la app:**
 - El usuario ejecuta main.py
 - El sistema carga automáticamente todos los archivos del directorio files/
 - Cada archivo se valida
 - Los datos se guardan en su tabla correspondiente en la base
 - La app finaliza indicando qué se cargó exitosamente y qué falló


### **2. Requisitos POO (deben estar claramente presentes)**
 - Pilar 	Evaluación Esperada
 - Abstracción 	Separación clara entre lectura de archivos, validación y persistencia
 - Encapsulamiento 	Atributos y métodos privados o protegidos donde corresponda
 - Herencia 	Clases de modelo que heredan de una clase base (por ejemplo, Base)
 - Polimorfismo 	Métodos redefinidos para manejar distintos formatos de archivo (leer_csv, leer_json, etc.)
 
---
