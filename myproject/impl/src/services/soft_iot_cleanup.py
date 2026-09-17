# -*- coding: utf-8 -*-

from zato.server.service import Service
import sqlite3
import os
from datetime import datetime, timedelta

# Configuração do Banco de Dados
DB_FILENAME = "/opt/zato/env/soft_iot_data.db"

# Padrão: 60 segundos (1 minuto) se não houver configuração
DEFAULT_RETENTION_SECONDS = 60 

class CleanupOldData(Service):
    """
    Serviço de manutenção que remove dados antigos do banco (baseado em SEGUNDOS).
    """
    name = 'soft-iot.cleanup.service'

    def handle(self):
        # Configuração da Retenção (Agora em Segundos)
        try:
            # Procura pela variável DATA_RETENTION_SECONDS
            retention_seconds = int(os.environ.get('Zato_DATA_RETENTION_SECONDS', DEFAULT_RETENTION_SECONDS))
            self.logger.info(
                f"TEMPO LIDO PELA VARIAVEL DE AMBIENTE NO CLEANUP: {os.environ.get('Zato_DATA_RETENTION_SECONDS')}"
            )
        except ValueError:
            retention_seconds = DEFAULT_RETENTION_SECONDS

        # Calcula a data de corte (Agora - Segundos de Retenção)
        cutoff_date = datetime.now() - timedelta(seconds=retention_seconds)
        
        # Formata para o padrão do SQLite (String ISO)
        cutoff_str = cutoff_date.strftime("%Y-%m-%d %H:%M:%S")

        self.logger.info(f"INICIANDO LIMPEZA RAPIDA: Removendo registros anteriores a {cutoff_str} ({retention_seconds}s de retenção)")

        conn = None
        try:
            if not os.path.exists(DB_FILENAME):
                self.logger.warning("Banco de dados não encontrado. Pulei a limpeza.")
                return

            conn = sqlite3.connect(DB_FILENAME)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout = 5000;")
            cursor = conn.cursor()

            # Executa a Deleção
            query = """
                DELETE FROM sensor_data 
                WHERE end_datetime <= ? 
                AND aggregation_status = 2
            """
            
            cursor.execute(query, (cutoff_str,))
            deleted_count = cursor.rowcount
            conn.commit()

            self.logger.info(f"LIMPEZA CONCLUÍDA: {deleted_count} registros removidos com sucesso.")

        except Exception as e:
            self.logger.error(f"ERRO CRÍTICO na limpeza de dados: {e}")
        finally:
            if conn:
                conn.close()