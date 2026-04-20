# System Audit Report

| Campo | Valor |
| --- | --- |
| Fecha/hora | 2026-04-20T20:13:08 |
| Estado general | **WARNING** |
| Hallazgos críticos | 0 |
| Warnings | 2 |
| Checks ejecutados | 2758 |
| Reporte raw | `reports/system_audit_raw.json` |

## Resumen ejecutivo

- Estado general: **WARNING**.
- Salud de datos: 1289 activos, 96.1% usables, 0 sin precio, 48 sin m2.
- Benchmark: 99 evaluados, error mediano 14.4%.
- Radar: 1157 listings listos, 20 oportunidades generadas.
- Legal risk: Medio: 20; 0 inconsistencias legales.
- Principal warning: `listings_without_m2` - Existen listings sin m2_construidos.

## System Health

Status: **UNSTABLE**  
Score: **40 / 100**

Critical issues: 0  
High issues: 2  
Medium issues: 1  
Low issues: 2

### High

- listings_without_m2: Existen listings sin m2_construidos.
- missing m2: 48 listings sin m2_construidos

### Medium

- missing lat/lon: 1169 listings sin coordenadas

### Low

- precio_m2_outliers: Existen outliers de precio/m2.
- insufficient_segment_data_events: Se detectaron eventos insufficient_segment_data en logs del radar.

## Hallazgos críticos

Sin críticos.

## Warnings

- `listings_without_m2`: Existen listings sin m2_construidos.
- `precio_m2_outliers`: Existen outliers de precio/m2.

## Infos

- `insufficient_segment_data_events` sección=system_logs: Se detectaron eventos insufficient_segment_data en logs del radar.

## Acciones sugeridas

- Priorizar warnings repetidos en calidad de datos y radar.

## Salud de base de datos

Estado: **OK**

- Properties: 12
- Listings: 1356
- Active listings: 1289
- Inactive listings: 67
- Representatives: 1223
- Conteo por status: active: 1289, invalid_data: 49, inactive: 13, appraisal_result: 5
- Duplicados fuente+link: 0
- Duplicados fingerprint: 25
- Sin precio: 0
- Sin m2: 48
- Sin comuna: 0
- Sin lat/lon: 1169
- Precio/m2 válido: 1307
- Precio/m2 inválido: 1

## Calidad de datos

Estado: **OK**

- Listings revisados: 1356
- Listings usables: 1303
- Ratio usable: 96.1%
- Top issues: missing_basic_attributes: 930, invalid_precio_m2: 49, missing_m2: 48, invalid_m2_range: 5
- Outliers precio/m2: 1
- Cobertura de campos:
  - dormitorios: 415 (30.6%)
  - banos: 229 (16.9%)
  - estacionamientos: 86 (6.3%)
  - ano_construccion: 9 (0.7%)
  - piscina: 9 (0.7%)
  - lat_lon: 187 (13.8%)

## Cobertura por comuna

Estado: **OK**

- Mínimo de registros por comuna: 3
- Comunas incluidas: 8
- Comunas omitidas por bajo volumen: 0

| Comuna | Activos | Total | Mediana precio/m2 | Min precio/m2 | Max precio/m2 | Dorm. | Baños | Estac. | Geo |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Providencia | 312 | 334 | 3153005.7 | 1471395.3 | 5449661.0 | 31.1% | 14.4% | 5.1% | 16.8% |
| Vitacura | 264 | 267 | 3488571.4 | 1384353.7 | 6111111.1 | 23.2% | 9.4% | 7.5% | 5.2% |
| Las Condes | 261 | 292 | 3342613.6 | 1646813.6 | 40280000.0 | 34.2% | 20.9% | 7.9% | 28.4% |
| Ñuñoa | 98 | 106 | 2954218.8 | 1532857.1 | 5180000.0 | 20.8% | 15.1% | 5.7% | 12.3% |
| Macul | 93 | 93 | 2236818.2 | 899305.6 | 3418478.3 | 23.7% | 15.1% | 5.4% | 6.5% |
| Peñalolén | 93 | 93 | 2196875.0 | 800000.0 | 4096428.6 | 44.1% | 26.9% | 7.5% | 3.2% |
| La Reina | 90 | 90 | 2986668.8 | 1080531.0 | 4273239.4 | 37.8% | 23.3% | 2.2% | 3.3% |
| Santiago | 78 | 81 | 2132913.0 | 719387.8 | 4922321.4 | 37.0% | 23.5% | 7.4% | 11.1% |

## Evaluación del motor

Estado: **OK**

- Evaluados: 99
- Saltados: 1
- Error promedio: 17.2%
- Error mediano: 14.4%
- Mejores 5 grupos: comuna:La Reina med=3.9%; comuna:Las Condes med=10.3%; m2_range:small <50m2 med=11.5%; comuna:Vitacura med=12.2%; m2_range:large >100m2 med=14.2%
- Peores 5 grupos: comuna:Ñuñoa med=21.7%; comuna:Santiago med=17.9%; m2_range:medium 50-100m2 med=17.6%; comuna:Providencia med=17.5%; comuna:Macul med=14.8%
- Fuente peores casos individuales: biggest_errors

| Peor caso | Listing | Comuna | M2 | Precio real | Precio predicho | Error abs. | URL |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | --- |
| 1 | 192 | Ñuñoa | 70.0 | 115963315.0 | 236873634.7 | 104.3% | [link](https://www.yapo.cl/bienes-raices-venta-de-propiedades-apartamentos/departamento-en-avenida-americo-vespucio/29912611) |
| 2 | 961 | Providencia | 135.0 | 239524226.5 | 422902616.6 | 76.6% | [link](https://www.yapo.cl/bienes-raices-venta-de-propiedades-apartamentos/oportunidad-amplio-y-comodo-departamento/31736340) |
| 3 | 860 | Providencia | 83.0 | 178321966.2 | 291155700.6 | 63.3% | [link](https://www.yapo.cl/bienes-raices-venta-de-propiedades-apartamentos/la-experiencia-de-vivir-en-un-lugar-iconico/32065040) |
| 4 | 1267 | Las Condes | 466.0 | 2388444415.5 | 1292376535.5 | 45.9% | [link](https://www.yapo.cl/bienes-raices-venta-de-propiedades-apartamentos/departamento-valle-alegre-con-charles-hamilton/30704126) |
| 5 | 918 | Ñuñoa | 65.0 | 139955725.0 | 203806047.5 | 45.6% | [link](https://www.yapo.cl/bienes-raices-venta-de-propiedades-apartamentos/venta-departamento-2d2b1e1bod-zanartu-nunoa/32252152) |

## Auditoría de radar

Estado: **OK**

- Listings listos para radar: 1157
- Oportunidades generadas: 20
- Score inversión promedio: 56.9
- Descuento promedio: 27.3%

| Rank | Listing | Comuna | Precio | Valor estimado | Descuento | Confianza | Comparables | Inv. score | Legal risk |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 950 | Providencia | 251160545.3 | 355405097.6 | 29.3% | 88.5% | 10 | 62.0 | Medio (50.0) |
| 2 | 315 | La Reina | 311901330.0 | 442243335.6 | 29.5% | 86.8% | 10 | 61.0 | Medio (50.0) |
| 3 | 18 | Ñuñoa | 99568501.5 | 136942453.7 | 27.3% | 91.5% | 9 | 58.0 | Medio (50.0) |
| 4 | 1132 | Providencia | 331895005.0 | 471297776.1 | 29.6% | 82.6% | 9 | 58.0 | Medio (50.0) |
| 5 | 964 | Providencia | 275912715.0 | 379887354.6 | 27.4% | 89.1% | 10 | 59.0 | Medio (50.0) |
| 6 | 575 | Providencia | 204855194.0 | 285832034.8 | 28.3% | 85.0% | 10 | 59.0 | Medio (50.0) |
| 7 | 649 | Peñalolén | 60521394.6 | 84304309.1 | 28.2% | 84.8% | 7 | 53.0 | Medio (50.0) |
| 8 | 1123 | Providencia | 194533054.1 | 267144454.8 | 27.2% | 86.9% | 10 | 58.0 | Medio (50.0) |
| 9 | 1254 | Vitacura | 427864645.0 | 587255755.0 | 27.1% | 86.2% | 9 | 56.0 | Medio (50.0) |
| 10 | 259 | Las Condes | 237924732.5 | 318316360.9 | 25.3% | 92.4% | 10 | 57.0 | Medio (50.0) |
| 11 | 1219 | Vitacura | 535830490.0 | 751552911.6 | 28.7% | 81.1% | 6 | 53.0 | Medio (50.0) |
| 12 | 1084 | Vitacura | 831736880.0 | 1140243529.8 | 27.1% | 85.5% | 10 | 58.0 | Medio (50.0) |
| 13 | 1182 | Macul | 110205136.6 | 149804365.6 | 26.4% | 86.7% | 10 | 57.0 | Medio (50.0) |
| 14 | 284 | Las Condes | 249920937.5 | 343302737.3 | 27.2% | 83.5% | 10 | 61.0 | Medio (50.0) |
| 15 | 946 | Providencia | 283910185.0 | 399665951.5 | 29.0% | 77.4% | 8 | 56.0 | Medio (50.0) |
| 16 | 941 | Providencia | 179943075.0 | 236951814.1 | 24.1% | 91.7% | 10 | 55.0 | Medio (50.0) |
| 17 | 1126 | Providencia | 257918407.5 | 351843240.7 | 26.7% | 82.5% | 10 | 57.0 | Medio (50.0) |
| 18 | 665 | Peñalolén | 219930425.0 | 296347538.9 | 25.8% | 84.6% | 9 | 54.0 | Medio (50.0) |
| 19 | 309 | La Reina | 171945605.0 | 230639232.2 | 25.4% | 85.3% | 10 | 56.0 | Medio (50.0) |
| 20 | 939 | Providencia | 243922835.0 | 328104449.9 | 25.7% | 84.4% | 7 | 50.0 | Medio (50.0) |

## Verificación de contratos de services

Estado: **ok**

- Estado contrato: ok
- Errores: 0
- Warnings: 0

## Validación de flujos del sistema

Estado: **ok**

- Estado integración: ok
- Errores: 0
- Warnings: 0

## Auditoría de Logs del Sistema

Estado: **OK**

- Total líneas de log capturadas: 77265
- Total caracteres capturados: 3738021
- insufficient_segment_data: 24
- % listings afectados: 1.2%
- Promedio comparables antes: 35.1
- Promedio comparables segmentados: 30.4
- Eventos fallback: 1220
- Resultados finales radar: 444

| Evento | Conteo |
| --- | ---: |
| CLUSTER | 39376 |
| WEIGHT | 10582 |
| AGGREGATION | 1233 |

### Hallazgos automáticos

- **INFO** `insufficient_segment_data_events`: Se detectaron eventos insufficient_segment_data en logs del radar.

## Auditoría de legal risk

Estado: **OK**

- Opportunities revisadas: 20
- Conteo niveles: Medio: 20
- Top flags: Datos incompletos: 20, Buena base de comparables y alta confiabilidad: 16
- Casos altos: 0
- Inconsistencias: 0

| Nivel | Count |
| --- | ---: |
| Bajo | 0 |
| Medio | 20 |
| Alto | 0 |

| Score legal | Valor |
| --- | ---: |
| Count | 20 |
| Min | 50.0 |
| Max | 50.0 |
| Promedio | 50.0 |
| Mediana | 50.0 |

## Consistency checks

Estado: **OK**

- Opportunities revisadas: 20
- Issues detectados: 0
- Por severidad: Sin datos
- Radar ready count observado: 1157

| Severidad | Código | Listing | Mensaje |
| --- | --- | ---: | --- |

## Diagnóstico global del sistema

El sistema está operativo, pero presenta señales de riesgo que deben monitorearse antes de escalar uso comercial o automatizar decisiones.

## Top 5 acciones recomendadas

- Agrupar warnings por código y atacar los de mayor frecuencia.
- Revisar los peores casos del benchmark y validar patrones por comuna/m2.
- Revisar manualmente las top oportunidades con mayor descuento y baja confianza.
- Priorizar warnings repetidos en calidad de datos y radar.

