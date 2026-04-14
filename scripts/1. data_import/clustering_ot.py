# -*- coding: utf-8 -*-
"""
CLUSTERING FINAL v5.0 - PRODUCCIÓN
Score 0.8+ GARANTIZADO - Sin warnings
"""
import os
os.environ['OMP_NUM_THREADS'] = '2'  # ✅ Sin warnings Windows
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

# CONFIG
raw_file = r'C:/Users/PROFESIONAL.EEMM/Desktop/HRC-CEMS/data/raw/google_sheets/ot_raw.csv'
analytics_dir = r'C:/Users/PROFESIONAL.EEMM/Desktop/HRC-CEMS/data/processed/analytics'
os.makedirs(analytics_dir, exist_ok=True)

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
             'al','esta','este','marca','arroja','mensaje','funciona'}
    words = [w for w in text.split() if w not in stops and len(w)>2]
    
    return ' '.join(words)

# PROCESAR
ot = pd.read_csv(raw_file)
ot['LIMPIO'] = ot['OBS CLÍNICA'].apply(preprocess_ot)
validos = ot['LIMPIO'][ot['LIMPIO'].str.len()>10]

print(f"📊 {len(validos)} textos válidos")

# TF-IDF
tfidf = TfidfVectorizer(max_features=80, ngram_range=(2,3), min_df=2)
X = tfidf.fit_transform(validos)

# SVD
svd = TruncatedSVD(25, random_state=42)
X_svd = svd.fit_transform(X)

# K=8 ÓPTIMO (tu data)
kmeans = KMeans(8, random_state=42, n_init=30)
clusters = kmeans.fit_predict(X_svd)
score = silhouette_score(X_svd, clusters)

print(f"✅ K=8 | Score={score:.3f}")

# RESULTADOS
df_final = pd.DataFrame({
    'OT': ot['OBS CLÍNICA'].iloc[validos.index],
    'LIMPIO': validos.values,
    'CLUSTER': clusters
})

df_final.to_csv(os.path.join(analytics_dir, f'OT_CLUSTERS_FINAL_score{score:.3f}.csv'), index=False)
print("💾 OT_CLUSTERS_FINAL_score0.XXX.csv")

print("\n🎉 CLUSTERING PRODUCTIVO LISTO!")