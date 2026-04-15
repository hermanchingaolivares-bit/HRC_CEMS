# -*- coding: utf-8 -*-
"""
Created on Tue Apr 14 23:50:14 2026

@author: herma
"""

# visualizar_problemas_eemm.py
import os, pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud
from collections import Counter
import numpy as np
plt.close('all')

def get_latest_problemas():
    """🔍 Último PROBLEMAS_EEMM"""
    proyecto_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    clusters_dir = os.path.join(proyecto_raiz, 'data', 'processed', 'clusters')
    
    archivos = [f for f in os.listdir(clusters_dir) if f.startswith('PROBLEMAS_EEMM')]
    if not archivos:
        raise FileNotFoundError("❌ Ejecuta problemas_eemm.py primero")
    
    ultimo = max(archivos, key=lambda x: float(x.split('score')[1].split('.csv')[0]))
    filepath = os.path.join(clusters_dir, ultimo)
    print(f"🎯 {ultimo}")
    return filepath

# 🚀 INICIO
print("🔍 Buscando problemas EEMM...")
problemas_file = get_latest_problemas()
df = pd.read_csv(problemas_file)
df['LEN'] = df['PROBLEMA'].str.len()

print(f"\n📊 {len(df)} problemas | {df['CLUSTER'].nunique()} categorías")

# 1️⃣ GRÁFICO PRINCIPAL
fig, axes = plt.subplots(2, 2, figsize=(16,12))
fig.suptitle('🔍 PROBLEMAS EEMM - Prioridades Acción', fontsize=16, fontweight='bold')

# BARRAS CANTIDAD
counts = df['CLUSTER'].value_counts().sort_index()
colors = plt.cm.Set3(np.linspace(0,1,len(counts)))
bars = axes[0,0].bar(counts.index, counts.values, color=colors, alpha=0.8)
axes[0,0].set_title('📊 Cantidad por Problema', fontweight='bold')
for bar, count in zip(bars, counts.values):
    axes[0,0].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, 
                   str(count), ha='center', va='bottom')

# WORDCLOUD GENERAL
words = ' '.join(df['PROBLEMA']).split()
wc_global = WordCloud(width=400, height=300, background_color='white', 
                     colormap='viridis').generate(' '.join(words[:300]))
axes[0,1].imshow(wc_global, interpolation='bilinear')
axes[0,1].set_title('☁️ Todos los Problemas')
axes[0,1].axis('off')

# BOXPLOT LONGITUD
sns.boxplot(data=df, x='CLUSTER', y='LEN', ax=axes[1,0], palette='Set2')
axes[1,0].set_title('📏 Complejidad Problemas')
axes[1,0].tick_params(axis='x', rotation=45)

# TOP 5 CLUSTERS
top5 = counts.head(5)
axes[1,1].pie(top5.values, labels=top5.index, autopct='%1.1f%%', 
              colors=colors[:5], startangle=90)
axes[1,1].set_title('🥧 TOP 5 Problemas')

plt.tight_layout()
plt.savefig(os.path.join(os.path.dirname(problemas_file), 'visual_problemas.png'), 
            dpi=300, bbox_inches='tight')
plt.show()

# 2️⃣ WORDCLOUDS INDIVIDUALES
print("\n☁️  Wordclouds por problema...")
fig, axes = plt.subplots(3, 4, figsize=(20,15))
axes = axes.ravel()

for i, c in enumerate(sorted(df['CLUSTER'].unique())):
    if i < 12:  # Máx 12
        cluster_data = df[df['CLUSTER']==c]['PROBLEMA']
        wc = WordCloud(width=300, height=200, background_color='white').generate(' '.join(cluster_data))
        axes[i].imshow(wc, interpolation='bilinear')
        axes[i].set_title(f'C{c} ({len(cluster_data)} casos)')
        axes[i].axis('off')

for j in range(i+1, 12):
    axes[j].axis('off')

plt.suptitle('☁️ PROBLEMAS POR CATEGORÍA', fontsize=16)
plt.tight_layout()
plt.savefig(os.path.join(os.path.dirname(problemas_file), 'wordclouds_problemas.png'), 
            dpi=300, bbox_inches='tight')
plt.show()

# 3️⃣ TABLA EJECUTIVA
print("\n📋 PRIORIDADES EEMM (Top 10)")
resumen = []
for c in df['CLUSTER'].value_counts().head(10).index:
    data = df[df['CLUSTER']==c]
    palabras = Counter(' '.join(data['PROBLEMA']).split()).most_common(4)
    print(f"🔴 C{c}: {len(data)} casos | {', '.join([w[0] for w in palabras])}")
    
    resumen.append({
        'Prioridad': len(data),
        f'Cluster_{c}': ', '.join([w[0] for w in palabras]),
        'Acción': f"Revisar {palabras[0][0]} ({len(data)} casos)"
    })

print("\n💾 visual_problemas.png + wordclouds_problemas.png")
print("✅ ¡VISUALIZACIÓN LISTA!")