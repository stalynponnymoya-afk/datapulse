# DataPulse

[Español](#español) | [English](#english)

---

## Español

### Qué es DataPulse

DataPulse es una herramienta de **monitoreo automático de calidad de datos**. Analiza archivos CSV, Excel, JSON o Parquet y genera un reporte detallado con un score de calidad de 0-100.

Se utiliza antes de usar datos en análisis, machine learning o reportes para detectar problemas comunes que pueden afectar los resultados.

### Características

- ✅ **Score de calidad 0-100** con veredicto: EXCELENTE / BUENO / REGULAR / POBRE
- ✅ **8 tipos de análisis** de calidad de datos
- ✅ **Análisis de tipos de variables** (tabla + lista por tipo)
- ✅ **Soporte multi-formato**: CSV, Excel (.xlsx), JSON, Parquet
- ✅ **API REST** con FastAPI
- ✅ **Detección de drift** entre versiones de datos

### Análisis que realiza

| Análisis | Descripción | Impacto en score |
|----------|--------------|------------------|
| Tipos de variables | Tabla con dtype, tipo inferido, cardinalidad | - |
| Nulos | Valores vacíos por columna | hasta -30 pts |
| Duplicados | Filas repetidas | hasta -20 pts |
| Outliers | Valores atípicos (método IQR) | -5 pts/col (máx -25) |
| Valores negativos | Números negativos inesperados | -5 pts/col (máx -15) |
| Texto inconsistente | Variaciones de mayúsculas, espacios | -3 pts/col (máx -15) |
| Fechas inválidas | Formatos inválidos o fuera de rango | -5 pts/col (máx -15) |
| Relaciones entre columnas | total ≠ quantity * price | -5 pts/relación |

### Análisis de Tipos de Variables

El análisis incluye:

1. **Tabla de variables**: Información detallada por columna
   - `column`: Nombre de la columna
   - `dtype`: Tipo de dato de pandas
   - `inferred_type`: Tipo inferido (integer, float, categorical, text, datetime, boolean, binary)
   - `unique_count`: Número de valores únicos
   - `unique_pct`: Porcentaje de valores únicos
   - `null_count`: Conteo de nulos
   - `null_pct`: Porcentaje de nulos
   - `sample_values`: hasta 5 valores ejemplo

2. **Lista por tipo**: Columnas agrupadas por tipo inferido

### Uso rápido

#### Como script de Python

```bash
python src/quality_checker.py data/superstore_sales.csv
```

#### Como API REST

```bash
uvicorn api.main:app --reload
```

### Ejemplo de resultado

```json
{
  "quality_score": 85,
  "verdict": "BUENO",
  "shape": {"rows": 8399, "columns": 21},
  "variable_analysis": {
    "variable_table": [
      {"column": "Row_ID", "dtype": "int64", "inferred_type": "integer", "unique_count": 8399},
      {"column": "Sales", "dtype": "float64", "inferred_type": "float", "unique_count": 4588},
      {"column": "Region", "dtype": "object", "inferred_type": "categorical", "unique_count": 4}
    ],
    "type_summary": {"integer": 1, "float": 1, "categorical": 3, "text": 16},
    "type_groups": {
      "integer": ["Row_ID"],
      "float": ["Sales", "Profit"],
      "categorical": ["Region", "Segment", "Ship_Mode"]
    }
  }
}
```

### Instalación

```bash
pip install -r requirements.txt
```

### Dependencias

- pandas
- numpy
- openpyxl
- fastapi
- uvicorn

### Licencia

MIT

---

## English

### What is DataPulse

DataPulse is an **automatic data quality monitoring tool**. It analyzes CSV, Excel, JSON, or Parquet files and generates a detailed report with a quality score from 0-100.

### Features

- ✅ Quality score 0-100 with verdict
- ✅ 8 types of data quality analysis
- ✅ Variable type analysis (table + list by type)
- ✅ Multi-format support
- ✅ REST API with FastAPI
- ✅ Drift detection

### Analysis Performed

| Analysis | Description | Score Impact |
|----------|--------------|---------------|
| Variable types | Table with dtype, inferred type, cardinality | - |
| Nulls | Empty values per column | up to -30 pts |
| Duplicates | Duplicate rows | up to -20 pts |
| Outliers | Atypical values (IQR method) | -5 pts/col |
| Negative values | Unexpected negatives | -5 pts/col |
| Inconsistent text | Case variations, spaces | -3 pts/col |
| Invalid dates | Invalid formats | -5 pts/col |
| Column relationships | total ≠ quantity * price | -5 pts |

### Variable Type Analysis

1. **Variable table**: Per column info
   - column, dtype, inferred_type, unique_count, unique_pct, null_count, null_pct, sample_values

2. **List by type**: Columns grouped by inferred type

### Quick Usage

```bash
python src/quality_checker.py data/superstore_sales.csv
```

### Installation

```bash
pip install -r requirements.txt
```

### Dependencies

- pandas
- numpy
- openpyxl
- fastapi
- uvicorn

### License

MIT
