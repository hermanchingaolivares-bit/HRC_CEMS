# -*- coding: utf-8 -*-
"""
Created on Mon Apr 13 16:28:18 2026

@author: profesional.eemm
"""

# -*- coding: utf-8 -*-
"""
ANÁLISIS CLUSTER 0 - 1 CLIC
Lee OT_CLUSTERS_FINAL_score0.789.csv → Analiza Cluster 0 → Excel listo
"""
import pandas as pd
import re
from collections import Counter
import matplotlib.pyplot as plt
import os

plt.close('all')

# 📁 RUTA TU ARCHIVO (CAMBIAR SI NECESARIO)
import pandas as pd

# REPARAR TU CSV
archivo = r'C:/Users/PROFESIONAL.EEMM/Desktop/HRC-CEMS/data/processed/analytics/OT_CLUSTERS_FINAL_score0.789.csv'

# LEER CON ERROR_HANDLING
df = pd.read_csv(archivo, on_bad_lines='skip', low_memory=False)
print(f"✅ Archivo reparado: {len(df)} filas")

# GUARDAR LIMPIO
df.to_csv('OT_CLUSTERS_LIMPIO.csv', index=False)
print("💾 OT_CLUSTERS_LIMPIO.csv ← USAR ESTE")

salida_dir = r'C:/Users/PROFESIONAL.EEMM/Desktop/HRC-CEMS/data/processed/analytics'

print("🔍 ANÁLISIS CLUSTER 0 - AUTOMÁTICO")
print("=" * 50)

# 1. LEER Y FILTRAR CLUSTER 0
df = pd.read_csv(archivo)
cluster0 = df[df['CLUSTER'] == 0].copy()

print(f"📊 Cluster 0: {len(cluster0)} textos ({len(cluster0)/len(df)*100:.1f}%)")

# 2. EXTRAER Y CONTAR PALABRAS
palabras = []
for texto in cluster0['TEXTO_LIMPIO']:
    # Limpiar y dividir
    clean = re.sub(r'[^\w\s]', ' ', str(texto).lower())
    words = clean.split()
    palabras.extend([w for w in words if len(w) > 2])

top_palabras = Counter(palabras).most_common(30)
print("\n🏆 TOP 30 PALABRAS Cluster 0:")
for i, (palabra, count) in enumerate(top_palabras, 1):
    pct = count / len(palabras) * 100
    print(f"{i:2d}. {palabra:12s} → {count:4d} ({pct:5.1f}%)")

# 3. SUB-CLUSTERS (Agrupar por palabras clave)
def clasificar_problema(texto):
    texto = str(texto).lower()
    
    if any(pal in texto for pal in ['batería', 'baterias']):
        return '🔋 BATERÍAS'
    elif any(pal in texto for pal in ['cable', 'poder', 'enchufe', 'cortado']):
        return '🔌 CABLES/ELÉCTRICO'
    elif any(pal in texto for pal in ['presión', 'pani', 'sensor']):
        return '📏 SENSORES/PRESIÓN'
    elif any(pal in texto for pal in ['papel', 'impresión', 'imprime']):
        return '📄 PAPEL/IMPRESIÓN'
    elif any(pal in texto for pal in ['imagen', 'captura', 'video']):
        return '📹 IMÁGENES/VIDEO'
    elif any(pal in texto for pal in ['luz', 'lámpara', 'enciende']):
        return '💡 LÚMINES'
    else:
        return '❓ OTROS'

cluster0['SUB_CLUSTER'] = cluster0['TEXTO_LIMPIO'].apply(clasificar_problema)
sub_stats = cluster0['SUB_CLUSTER'].value_counts()

print(f"\n📈 SUB-CLUSTERS Cluster 0:")
for problema, count in sub_stats.items():
    pct = count / len(cluster0) * 100
    print(f"  {problema:20s} → {count:3d} ({pct:5.1f}%)")

# 4. EXPORTAR EXCEL PROFESIONAL
with pd.ExcelWriter(os.path.join(salida_dir, 'CLUSTER0_ANALISIS_COMPLETO.xlsx')) as writer:
    
    # Hoja 1: Resumen
    resumen = pd.DataFrame({
        'Métrica': ['Total Cluster 0', 'Score General', '% del Total', 'Top Problema'],
        'Valor': [len(cluster0), 0.789, f"{len(cluster0)/len(df)*100:.1f}%", top_palabras[0][0]]
    })
    resumen.to_excel(writer, sheet_name='RESUMEN', index=False)
    
    # Hoja 2: Top Palabras
    top_df = pd.DataFrame(top_palabras, columns=['Palabra', 'Cantidad'])
    top_df['%'] = top_df['Cantidad'] / top_df['Cantidad'].sum() * 100
    top_df.to_excel(writer, sheet_name='TOP_PALABRAS', index=False)
    
    # Hoja 3: Sub-clusters
    sub_df = pd.DataFrame({
        'SUB_CLUSTER': sub_stats.index,
        'CANTIDAD': sub_stats.values,
        '%': [f"{x/len(cluster0)*100:.1f}%" for x in sub_stats.values]
    })
    sub_df.to_excel(writer, sheet_name='SUB_CLUSTERS', index=False)
    
    # Hoja 4: Detalle OT
    cluster0[['OT_ORIGINAL', 'TEXTO_LIMPIO', 'SUB_CLUSTER']].to_excel(
        writer, sheet_name='DETALLE_OT', index=False
    )

print(f"\n💾 EXCEL COMPLETO:")
print(f"  ✅ CLUSTER0_ANALISIS_COMPLETO.xlsx")
print(f"  📊 4 hojas: Resumen | Top Palabras | Sub-clusters | Detalle")

# 5. GRÁFICO RÁPIDO
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

# Top 10 palabras
top10 = pd.DataFrame(top_palabras[:10], columns=['Palabra', 'Cantidad'])
ax1.barh(range(10), top10['Cantidad'], color='skyblue')
ax1.set_yticks(range(10))
ax1.set_yticklabels(top10['Palabra'])
ax1.set_title('Top 10 Problemas Cluster 0')
ax1.set_xlabel('Cantidad')

# Sub-clusters
sub_colors = plt.cm.Set3(range(len(sub_stats)))
ax2.pie(sub_stats.values, labels=sub_stats.index, autopct='%1.1f%%', colors=sub_colors)
ax2.set_title('Distribución Sub-Clusters')

plt.tight_layout()
plt.savefig(os.path.join(salida_dir, 'CLUSTER0_GRAFICOS.png'), dpi=300)
plt.show()

print("\n🎉 ¡ANÁLISIS CLUSTER 0 COMPLETO!")
print("📁 Todo en: data/processed/analytics/")
print("✅ Listo para Jefatura/PowerBI")