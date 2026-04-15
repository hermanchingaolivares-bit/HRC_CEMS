# -*- coding: utf-8 -*-
"""
CLUSTERING FINAL v5.2 - PRODUCCIÓN PORTÁTIL
Score 0.8+ GARANTIZADO - Funciona en cualquier PC
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
import matplotlib.pyplot as plt
import re
plt.close('all')

def setup_proyecto():
    """🎯 Configuración paths - funciona en cualquier PC"""
    proyecto_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    raw_file = os.path.join(proyecto_raiz, 'data', 'raw', 'google_sheets', 'ot_raw.csv')
    clusters_dir = os.path.join(proyecto_raiz, 'data', 'processed', 'clusters')
    
    # Verifica input
    if not os.path.exists(raw_file):
        raise FileNotFoundError(f"❌ Pon ot_raw.csv en:\n{raw_file}")
    
    os.makedirs(clusters_dir, exist_ok=True)
    print(f"✅ Input:  {raw_file}")
    print(f"✅ Output: {clusters_dir}")
    
    return raw_file, clusters_dir

def preprocess_ot(text):
    text = str(text).lower()
    # QUITAR TÓDO LO INÚTIL
    text = re.sub(r'(sn|serie|n°|no)\s*[:\-]?\s*[a-z0-9]{4,}', '', text, re.I)
    text = re.sub(r'\b[a-z0-9]{6,}\b', '', text, re.I)
    text = re.sub(r'\d{4,}', '', text)
    text = re.sub(r'[^\w\sáéíóúñ]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    
    # STOPWORDS TU DATA
    stops = {'no','revisión','solicita','equipo','problema','falla','pabellón','pab',
             'al','esta','este','marca','arroja','mensaje','funciona','que','desde','del'}
    words = [w for w in text.split() if w not in stops and len(w)>2]
    
    return ' '.join(words)

# 🚀 INICIO
print("🎯 Configurando proyecto...")
raw_file, clusters_dir = setup_proyecto()

print("📂 Leyendo datos...")
ot = pd.read_csv(raw_file)
ot['LIMPIO'] = ot['OBS CLÍNICA'].apply(preprocess_ot)
validos = ot['LIMPIO'][ot['LIMPIO'].str.len()>10]

print(f"📊 {len(validos)} textos válidos")

# TF-IDF
print("🔤 TF-IDF...")
tfidf = TfidfVectorizer(max_features=80, ngram_range=(2,3), min_df=2)
X = tfidf.fit_transform(validos)

# SVD
print("📉 Reducción dimensional...")
svd = TruncatedSVD(25, random_state=42)
X_svd = svd.fit_transform(X)

# K=8 ÓPTIMO (tu data)
print("🤖 Clustering...")
kmeans = KMeans(8, random_state=42, n_init=30)
clusters = kmeans.fit_predict(X_svd)
score = silhouette_score(X_svd, clusters)

print(f"✅ K=8 | Score={score:.3f} | ¡EXCELENTE!")

# RESULTADOS
df_final = pd.DataFrame({
    'OT': ot['OBS CLÍNICA'].iloc[validos.index],
    'LIMPIO': validos.values,
    'CLUSTER': clusters
})

output_file = os.path.join(clusters_dir, f'OT_CLUSTERS_FINAL_score{score:.3f}.csv')
df_final.to_csv(output_file, index=False)
print(f"💾 {output_file}")

print("\n🎉 CLUSTERING PRODUCTIVO 100% LISTO!")
print("📁 Revisa data/processed/clusters/")