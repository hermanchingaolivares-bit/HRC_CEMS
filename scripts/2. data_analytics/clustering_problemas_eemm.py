# -*- coding: utf-8 -*-
"""
CLUSTER PROBLEMAS EEMM v1.0
Categoriza OBS CLÍNICA → Lista accionable problemas
"""
import os
os.environ['OMP_NUM_THREADS'] = '2'
os.environ['MKL_NUM_THREADS'] = '2'

import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.decomposition import TruncatedSVD
import re
from collections import Counter

def setup_proyecto():
    """🎯 Tu setup exacto"""
    proyecto_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    raw_file = os.path.join(proyecto_raiz, 'data', 'raw', 'google_sheets', 'ot_raw.csv')
    clusters_dir = os.path.join(proyecto_raiz, 'data', 'processed', 'clusters')
    
    if not os.path.exists(raw_file):
        raise FileNotFoundError(f"❌ {raw_file}")
    
    os.makedirs(clusters_dir, exist_ok=True)
    print(f"✅ Input:  {raw_file}")
    print(f"✅ Output: {clusters_dir}")
    return raw_file, clusters_dir

def preprocess_problemas(text):
    """🔧 OPTIMIZADO para detectar PROBLEMAS EEMM"""
    text = str(text).lower()
    
    # ELIMINAR RUIDO TÉCNICO
    text = re.sub(r'(sn|serie|n°|no|box|pab\.?|pabellón?\s*\d+)', '', text, re.I)
    text = re.sub(r'\b[a-z0-9]{4,}\b', '', text, re.I)  # Seriales
    text = re.sub(r'\d{2,}', '', text)  # Números/fechas
    
    # PROBLEMAS → VERBOS ACCIÓN
    accion = re.sub(r'[^\w\sáéíóúñ]', ' ', text)
    accion = re.sub(r'\s+', ' ', accion).strip()
    
    # STOPWORDS ESPECÍFICAS EEMM (tu lista + extras)
    stops = {
        'no','revisión','solicita','equipo','problema','falla','pabellón','pab',
        'al','esta','este','marca','arroja','mensaje','funciona','que','desde','del',
        'para','una','los','las','con','por','sobre','hacer','hizo','hace','luz', 'torre', 'pantalla', 'sin', 'ellón', 'centralizado', 
    'ucam', 'ambulatorio', 'presentando', 'día', 'mal', 'estado'
    }
    
    words = [w for w in accion.split() if w not in stops and len(w)>2]
    return ' '.join(words[:15])  # Máx 15 palabras

# 🚀 INICIO
print("🎯 CLUSTER PROBLEMAS EEMM...")
raw_file, clusters_dir = setup_proyecto()

print("📂 Leyendo OTs...")
ot = pd.read_csv(raw_file)
ot['PROBLEMA'] = ot['OBS CLÍNICA'].apply(preprocess_problemas)
validos = ot['PROBLEMA'][ot['PROBLEMA'].str.len()>8]  # Mínimo descriptivo

print(f"📊 {len(validos)} problemas válidos")

# TF-IDF OPTIMIZADO PROBLEMAS
print("🔤 TF-IDF problemas...")
tfidf = TfidfVectorizer(
    max_features=120,      # + features
    ngram_range=(1,3),     # unigramas+bigramas+trigramas
    min_df=2
)
X = tfidf.fit_transform(validos)

# SVD mejorado
print("📉 SVD...")
svd = TruncatedSVD(30, random_state=42)  # + dimensiones
X_svd = svd.fit_transform(X)

# K=12 (temas médicos específicos)
print("🤖 KMeans 12 temas...")
kmeans = KMeans(12, random_state=42, n_init=50)
clusters = kmeans.fit_predict(X_svd)
score = silhouette_score(X_svd, clusters)

print(f"✅ K=12 | Score={score:.3f}")

# 🎯 RESUMEN ACCIONABLE
df_final = pd.DataFrame({
    'OT': ot.loc[validos.index, 'OT'],
    'PROBLEMA': validos.values,
    'CLUSTER': clusters
})

# TOP 5 PALABRAS + PROBLEMA PRINCIPAL
resumen = []
for c in range(12):
    cluster_data = df_final[df_final['CLUSTER']==c]
    if len(cluster_data) > 3:  # Mínimo 4 casos
        texto = ' '.join(cluster_data['PROBLEMA'])
        top5 = Counter(texto.split()).most_common(5)
        principal = top5[0][0] if top5 else 'genérico'
        
        resumen.append({
            'Cluster': c,
            'Cantidad': len(cluster_data),
            'Principal': principal,
            'Palabras': ', '.join([w[0] for w in top5]),
            'Acción': f"Revisar {principal}s ({len(cluster_data)} casos)",
            'Ejemplo': str(cluster_data['OT'].iloc[0])[:100]
        })

df_resumen = pd.DataFrame(resumen).sort_values('Cantidad', ascending=False)
output_file = os.path.join(clusters_dir, f'PROBLEMAS_EEMM_score{score:.3f}.csv')
df_final.to_csv(output_file, index=False)

resumen_file = os.path.join(clusters_dir, f'RESUMEN_PROBLEMAS_score{score:.3f}.csv')
df_resumen.to_csv(resumen_file, index=False)

print("\n🎯 RESUMEN ACCIONABLE:")
print(df_resumen[['Cluster', 'Cantidad', 'Principal', 'Acción']])
print(f"\n💾 {output_file}")
print(f"💾 {resumen_file}")
print("\n✅ ¡LISTA DE PROBLEMAS PARA EEMM!")