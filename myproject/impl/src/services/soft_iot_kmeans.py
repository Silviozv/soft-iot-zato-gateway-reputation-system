# -*- coding: utf-8 -*-
from zato.server.service import Service
import numpy as np
from sklearn.cluster import KMeans
import time

class KMeansCredibilityService(Service):
    """
    Serviço que executa o algoritmo K-Means sobre uma lista de dados,
    retornando os pontos do cluster que contém o maior valor.
    Equivalente ao KMeansServiceCredibility do Java.
    """
    name = 'soft-iot.kmeans.credibility'

    def handle(self):
        start_time = time.time()

        # Recebe os dados do payload da requisição JSON
        # Esperado: {"data": [10.5, 20.1, 5.0, ...]}
        raw_data = self.request.payload.get('data', [])
        
        if not raw_data:
            self.response.payload = {"error": "A lista 'data' não foi fornecida."}
            self.response.status_code = 400
            return

        k = 4
        max_iterations = 100

        # Copia a lista para evitar mutações indesejadas
        data = list(raw_data)

        # Preenche com -1.0f 
        while len(data) < k:
            data.append(-1.0)

        # Prepara os dados e executa o K-Means
        # O scikit-learn exige uma matriz 2D para as features (n_samples, n_features)
        X = np.array(data).reshape(-1, 1)
        
        # Instancia e treina o modelo
        kmeans = KMeans(n_clusters=k, max_iter=max_iterations, n_init=10, random_state=42)
        clusters_of_points = kmeans.fit_predict(X)

        # Organiza pontos por cluster para os logs 
        cluster_map = {}
        for i, cluster_id in enumerate(clusters_of_points):
            cluster_map.setdefault(cluster_id, []).append(data[i])

        self.logger.info("Clusters designados:")
        for c_id, points in cluster_map.items():
            self.logger.info(f"Cluster {c_id}: {points}")

        # Encontra o cluster que contém o maior valor original
        # Achamos o maior valor ignorando os -1.0 que injetamos
        valid_data = [val for val in data if val != -1.0]
        max_value = max(valid_data) if valid_data else -1.0
        
        # Pega o índice desse maior valor na lista original para descobrir seu cluster
        max_index = data.index(max_value)
        target_cluster = clusters_of_points[max_index]

        # Filtra os valores únicos desse cluster 
        # O uso de set() garante que não haverá valores repetidos
        result_set = set()
        for i, val in enumerate(data):
            if clusters_of_points[i] == target_cluster and val != -1.0:
                result_set.add(val)

        points_in_target_cluster = list(result_set)
        
        self.logger.info(f"Pontos no cluster com maior valor: {points_in_target_cluster}")

        duration = time.time() - start_time
        self.logger.info(f"Tempo de execução: {duration:.6f} segundos")

        self.response.payload = points_in_target_cluster