# -*- coding: utf-8 -*-
"""
Created on Mon Apr 13 16:30:41 2026

@author: profesional.eemm
"""

# SUB-CLUSTER Cluster 0 - clusters_final_*.csv → Excel análisis
import pandas as pd
from collections import Counter
import re
import os

analytics_dir = r'C:/Users/PROFESIONAL.EEMM/Desktop/HRC-CEMS/data/processed/analytics'

# ENCONTRAR ARCHIVO CLUSTER PRINCIPAL
archivos = [f for f in os.listdir(analytics_dir) if 'clusters_final' in f]
archivo_cluster = os.path.join(analytics_dir, archivos[0])
print(f"📂 Leyendo: {archivos[0]}")

df = pd.read_csv(archivo_cluster, low_memory=False)
cluster0 = df[df['CLUSTER'] == 0]

print(f"🔍 Cluster 0: {len(cluster0)} OT ({len(cluster0)/len(df)*100:.1f}%)")

# CONTAR PALABRAS
palabras = []
for texto in cluster0['LIMPIO']:
    words = re.findall(r'\b\w+\b', str(texto).lower())
    palabras.extend([w for w in words if len(w)>2])

top_palabras = Counter(palabras).most_common(25)

print("\n🏆 TOP 25 PROBLEMAS Cluster 0:")
for i, (palabra, count) in enumerate(top_palabras, 1):
    print(f"{i:2d}. {palabra:12s} = {count:3d}")

# CATEGORIZAR
def categorizar(texto):
    texto = str(texto).lower()
    if any(x in texto for x in ['batería','baterias']): return '🔋 BATERÍAS'
    if any(x in texto for x in ['cable','poder','enchufe']): return '🔌 CABLES'
    if any(x in texto for x in ['presión','pani','sensor']): return '📏 PRESIÓN'
    if any(x in texto for x in ['papel','imprime','impresión']): return '📄 PAPEL'
    if any(x in texto for x in ['imagen','captura','video']): return '📹 VIDEO'
    return '❓ OTROS'

cluster0['CATEGORIA'] = cluster0['LIMPIO'].apply(categorizar)
categorias = cluster0['CATEGORIA'].value_counts()

print(f"\n📈 CATEGORÍAS Cluster 0:")
for cat, count in categorias.items():
    print(f"  {cat:<20} {count:3d} ({count/len(cluster0)*100:.1f}%)")

# EXPORTAR EXCEL FINAL
with pd.ExcelWriter(os.path.join(analytics_dir, 'CLUSTER0_ANALISIS_FINAL.xlsx')) as writer:
    # Resumen
    pd.DataFrame({
        'Métrica': ['Total Cluster 0', 'Score Clustering', '% Total OT'],
        'Valor': [len(cluster0), 0.789, f"{len(cluster0)/len(df)*100:.1f}%"]
    }).to_excel(writer, 'RESUMEN', index=False)
    
    # Top palabras
    pd.DataFrame(top_palabras, columns=['Problema', 'Cantidad']).to_excel(writer, 'TOP_PROBLEMAS', index=False)
    
    # Categorías
    pd.DataFrame({'Categoría': categorias.index, 'Cantidad': categorias.values}).to_excel(writer, 'CATEGORIAS', index=False)
    
    # Detalle OT
    cluster0[['OT', 'LIMPIO', 'CATEGORIA']].to_excel(writer, 'DETALLE', index=False)

print(f"\n🎉 ¡LISTO!")
print(f"💾 CLUSTER0_ANALISIS_FINAL.xlsx (4 hojas)")
print(f"📊 Resumen | Top Problemas | Categorías | Detalle OT")