# -*- coding: utf-8 -*-
"""
Created on Tue Apr 14 23:34:05 2026

@author: herma
"""

# -*- coding: utf-8 -*-
"""
VISUALIZAR CLUSTERS v1.0
Lee automáticamente el último archivo de data/processed/clusters/
"""
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud
from collections import Counter
from sklearn.manifold import TSNE
plt.close('all')
plt.style.use('default')

def get_latest_cluster_file():
    """🔍 Encuentra OT_CLUSTERS_FINAL_scoreX.csv más reciente"""
    proyecto_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    clusters_dir = os.path.join(proyecto_raiz, 'data', 'processed', 'clusters')
    
    if not os.path.exists(clusters_dir):
        raise FileNotFoundError(f"❌ {clusters_dir}\nEjecuta clustering primero")
    
    archivos = [f for f in os.listdir(clusters_dir) if f.startswith('OT_CLUSTERS')]
    if not archivos:
        raise FileNotFoundError("❌ No hay archivos de clusters")
    
    # El de mayor score
    ultimo = max(archivos, key=lambda x: float(x.split('score')[1].replace('.csv','')))
    filepath = os.path.join(clusters_dir, ultimo)
    
    print(f"🎯 Último archivo: {ultimo}")
    print(f"📊 Score: {ultimo.split('score')[1].replace('.csv','')}")
    return filepath

# 🚀 INICIO
print("🔍 Buscando clusters...")
clusters_file = get_latest_cluster_file()
df = pd.read_csv(clusters_file)
df['LEN'] = df['LIMPIO'].str.len()

print(f"\n📊 {len(df)} OTs | {df['CLUSTER'].nunique()} clusters")
print("🎨 Generando visualizaciones...")

# 1️⃣ t-SNE 2D
print("📈 t-SNE 2D...")
tsne = TSNE(n_components=2, random_state=42, perplexity=min(30, len(df)//4))
X_2d = tsne.fit_transform(df['LEN'].values.reshape(-1,1))  # Simple embedding

# 2️⃣ GRÁFICOS
fig, axes = plt.subplots(2, 2, figsize=(15,12))
fig.suptitle(f'🔍 CLUSTERS HRC-CEMS\n{clusters_file.split("/")[-1]}', fontsize=16)

# Gráfico principal
scatter = axes[0,0].scatter(X_2d[:,0], X_2d[:,1], c=df['CLUSTER'], 
                           cmap='tab10', s=60, alpha=0.7)
axes[0,0].set_title('Clusters 2D (t-SNE)')
plt.colorbar(scatter, ax=axes[0,0])

# Distribución clusters
counts = df['CLUSTER'].value_counts().sort_index()
axes[0,1].bar(counts.index, counts.values, color='skyblue', alpha=0.8)
axes[0,1].set_title('📊 Tamaño Clusters')
for i, v in counts.items():
    axes[0,1].text(i, v+1, str(v), ha='center')

# Boxplot longitud
sns.boxplot(data=df, x='CLUSTER', y='LEN', ax=axes[1,0])
axes[1,0].set_title('📏 Longitud por Cluster')

# Wordcloud general
words = ' '.join(df['LIMPIO']).split()
wc = WordCloud(width=400, height=400, background_color='white').generate(' '.join(words[:200]))
axes[1,1].imshow(wc, interpolation='bilinear')
axes[1,1].axis('off')
axes[1,1].set_title('☁️ Palabras frecuentes')

plt.tight_layout()
plt.savefig(os.path.join(os.path.dirname(clusters_file), 'visualizacion.png'), dpi=300, bbox_inches='tight')
plt.show()

# 3️⃣ RESUMEN AUTOMÁTICO
print("\n📋 RESUMEN EJECUTIVO")
resumen = []
for c in sorted(df['CLUSTER'].unique()):
    cluster_data = df[df['CLUSTER']==c]
    palabras = Counter(' '.join(cluster_data['LIMPIO']).split()).most_common(5)
    top_palabras = ', '.join([w[0] for w in palabras])
    
    print(f"\n🔍 CLUSTER {c}: {len(cluster_data)} OTs")
    print(f"   Palabras: {top_palabras}")
    print(f"   Ejemplo: {cluster_data['OT'].iloc[0][:120]}...")
    
    resumen.append({
        'Cluster': c, 'Cantidad': len(cluster_data), 
        'Palabras': top_palabras,
        'Ejemplo': str(cluster_data['OT'].iloc[0])[:100]
    })

resumen_df = pd.DataFrame(resumen)
resumen_file = os.path.join(os.path.dirname(clusters_file), 'resumen.csv')
resumen_df.to_csv(resumen_file, index=False)

print(f"\n💾 Guardados en {os.path.dirname(clusters_file)}/")
print("   📈 visualizacion.png")
print("   📋 resumen.csv")
print("\n🎉 ¡ANÁLISIS COMPLETO!")