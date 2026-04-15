# DataPulse

Monitor automático de calidad de datos. Analiza cualquier CSV o Excel y genera un reporte con score de 0-100.

## Qué detecta

- Valores nulos por columna
- Filas duplicadas
- Outliers (método IQR)
- Valores negativos
- Texto inconsistente
- Fechas inválidas
- Relaciones entre columnas

## Uso rápido

```bash
python src/quality_checker.py data/superstore_sales.csv
```

Devuelve un JSON con el análisis completo y un quality score.

## Ejemplo de resultado

| Dataset            | Filas  | Nulos | Duplicados | Outliers | Score |
|--------------------|--------|-------|------------|----------|-------|
| Superstore Sales   | 8,399  | 0.75% | 0          | 5 cols   | 70    |
| California Housing | 20,640 | 1.0%  | 0          | 6 cols   | 70    |

## Requisitos

```bash
pip install -r requirements.txt
```

## Stack

- Python + pandas + numpy
- Próximamente: FastAPI, Render.com, n8n, MCP Server

## Licencia

MIT
