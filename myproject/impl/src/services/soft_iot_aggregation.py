# -*- coding: utf-8 -*-

from zato.server.service import Service
import sqlite3
import os

DB_FILENAME = "/opt/zato/env/soft_iot_data.db"

class AggregateSensorData(Service):
    """
    Serviço que consolida dados brutos dinamicamente baseado na variável AGGREGATION_WINDOW_MINUTES.
    """
    name = 'soft-iot.aggregation.service'

    def handle(self):
        # Lê a variável de ambiente (Padrão: 60 minutos se não existir)
        try:
            self.window_minutes = int(os.environ.get('Zato_AGGREGATION_WINDOW_MINUTES', 60))
        except ValueError:
            self.window_minutes = 60
            
        self.window_seconds = self.window_minutes * 60

        self.logger.info(f"INICIANDO AGREGAÇÃO (Janela configurada: {self.window_minutes} minutos)...")
        
        conn = None
        try:
            if not os.path.exists(DB_FILENAME):
                return

            conn = sqlite3.connect(DB_FILENAME)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout = 5000;")
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("SELECT DISTINCT device_id, sensor_id FROM sensor_data")
            sensors = cursor.fetchall()

            for row in sensors:
                self._aggregate_sensor(cursor, row['device_id'], row['sensor_id'])
            
            conn.commit()
            self.logger.info("AGREGAÇÃO FINALIZADA.")

        except Exception as e:
            self.logger.error(f"Erro na agregação: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

    def _aggregate_sensor(self, cursor, device_id, sensor_id):
        cursor.execute("""
            SELECT last_time FROM aggregation_registered_last_time_sensors 
            WHERE device_id = ? AND sensor_id = ?
        """, (device_id, sensor_id))
        
        last_run_row = cursor.fetchone()
        
        if last_run_row and last_run_row[0]:
            last_time = last_run_row[0]
        else:
            last_time = "1970-01-01 00:00:00"
            if not last_run_row:
                cursor.execute("""
                    INSERT INTO aggregation_registered_last_time_sensors (device_id, sensor_id, last_time)
                    VALUES (?, ?, ?)
                """, (device_id, sensor_id, last_time))

        # Agrupa criando blocos dinâmicos do tamanho de window_seconds
        # Só pega blocos que já fecharam (menor que o bloco atual do relógio)
        query = """
            SELECT 
                datetime((CAST(strftime('%s', start_datetime) AS INTEGER) / ?) * ?, 'unixepoch') as bucket_start,
                AVG(CAST(data_value AS FLOAT)) as avg_val,
                MAX(end_datetime) as last_entry_in_bucket
            FROM sensor_data
            WHERE device_id = ? 
              AND sensor_id = ? 
              AND aggregation_status = 0
              AND start_datetime > ?
              AND start_datetime < datetime((CAST(strftime('%s', 'now') AS INTEGER) / ?) * ?, 'unixepoch')
            GROUP BY bucket_start
            ORDER BY bucket_start ASC
        """
        
        # Passando window_seconds várias vezes para completar a fórmula matemática da query
        cursor.execute(query, (self.window_seconds, self.window_seconds, device_id, sensor_id, last_time, self.window_seconds, self.window_seconds))
        aggregates = cursor.fetchall()
        
        if not aggregates:
            return

        self.logger.info(f"Agregando {len(aggregates)} blocos de {self.window_minutes} min para {device_id}/{sensor_id}")

        last_processed_time = last_time

        for agg in aggregates:
            bucket_start = agg['bucket_start']
            avg_value = round(agg['avg_val'], 2)
            last_entry = agg['last_entry_in_bucket']
            
            insert_sql = """
                INSERT INTO sensor_data 
                (sensor_id, device_id, data_value, start_datetime, end_datetime, aggregation_status)
                VALUES (?, ?, ?, ?, ?, 1)
            """
            cursor.execute(insert_sql, (sensor_id, device_id, str(avg_value), bucket_start, bucket_start))

            # Marca como "já agregado" (status=2) todos os registros brutos (status=0)
            # que pertencem ao bucket processado. Isso evita que o Cleanup remova dados
            # antes da agregação e também impede reprocessamento.
            update_raw_sql = """
                UPDATE sensor_data
                SET aggregation_status = 2
                WHERE device_id = ?
                  AND sensor_id = ?
                  AND aggregation_status = 0
                  AND start_datetime >= ?
                  AND start_datetime < datetime(?, '+' || ? || ' seconds')
            """
            cursor.execute(update_raw_sql, (device_id, sensor_id, bucket_start, bucket_start, self.window_seconds))
            last_processed_time = last_entry

        cursor.execute("""
            UPDATE aggregation_registered_last_time_sensors
            SET last_time = ?
            WHERE device_id = ? AND sensor_id = ?
        """, (last_processed_time, device_id, sensor_id))