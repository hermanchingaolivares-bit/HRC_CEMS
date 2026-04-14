# -*- coding: utf-8 -*-
"""
Created on Mon Apr 13 16:30:28 2026

@author: profesional.eemm
"""

# CLUSTER PRINCIPAL - ot_raw.csv → clusters_final.csv
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.decomposition import TruncatedSVD
import re
import os

raw_file = r'C:/Users/PROFESIONAL.EEMM/Desktop/HRC-CEMS/data/raw/google_sheets/ot_raw.csv'
analytics_dir = r'C:/Users/PROFESIONAL.EEMM/Desktop/HRC-CEMS/data/processed/analytics'
os.makedirs(analytics_dir, exist_ok=True)

def limpiar_texto(texto):
    texto = str(texto).lower()
    texto = re.sub(r'(sn|serie|n°)\s*[:\-]?\s*[a-z0-9]{4,}', '', texto, re.I)
    texto = re.sub(r'\b[a-z0-9]{6,}\b', '', texto, re.I)
    texto = re.sub(r'[^\w\s]', ' ', texto)
    texto = re.sub(r'\s+', ' ', texto).strip()
    stops = {'no','revisión','solicita','equipo','problema','pabellón', 'los','las','el', 'la', 'se', 'con','del'}
    words = [w for w in texto.split() if w not in stops and len(w)>2]
    return ' '.join(words)

ot = pd.read_csv(raw_file)
ot['LIMPIO'] = ot['OBS CLÍNICA'].apply(limpiar_texto)
textos = ot['LIMPIO'][ot['LIMPIO'].str.len()>10]

tfidf = TfidfVectorizer(max_features=100, ngram_range=(1,3))
X = tfidf.fit_transform(textos)
svd = TruncatedSVD(30)
X_svd = svd.fit_transform(X)

kmeans = KMeans(8, random_state=42, n_init=30)
clusters = kmeans.fit_predict(X_svd)
score = silhouette_score(X_svd, clusters)

df_result = pd.DataFrame({
    'OT': ot['OBS CLÍNICA'].iloc[textos.index],
    'LIMPIO': textos,
    'CLUSTER': clusters
})

df_result.to_csv(os.path.join(analytics_dir, f'clusters_final_k8_score{score:.3f}.csv'), index=False)
print(f"✅ clusters_final_k8_score{score:.3f}.csv")
print(f"📊 K=8 | Score={score:.3f} | {len(df_result)} OT")