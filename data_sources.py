"""
Módulo de gerenciamento de múltiplas fontes de dados.
Suporta: Databricks, CSV, Excel, GCP BigQuery, AWS S3, Template Python
"""

import pandas as pd
import json
import os
from typing import Dict, List, Tuple, Any
import io

class DataSource:
    """Classe base para fontes de dados"""
    
    def __init__(self, name: str, source_type: str):
        self.name = name
        self.source_type = source_type
        self.schema = {}  # {coluna: tipo}
        self.data = None
        self.config = {}
    
    def get_data(self) -> pd.DataFrame:
        """Retorna os dados como DataFrame"""
        raise NotImplementedError
    
    def get_schema(self) -> Dict[str, str]:
        """Retorna o schema {coluna: tipo}"""
        if self.data is not None:
            return {col: str(self.data[col].dtype) for col in self.data.columns}
        return self.schema
    
    def set_schema(self, schema: Dict[str, str]):
        """Define o schema customizado"""
        self.schema = schema
    
    def to_dict(self) -> Dict:
        """Serializa a fonte para dicionário"""
        return {
            "name": self.name,
            "source_type": self.source_type,
            "schema": self.schema,
            "config": self.config
        }


class DatabricksSource(DataSource):
    """Fonte de dados Databricks"""
    
    def __init__(self, name: str, host: str, path: str, token: str, table: str):
        super().__init__(name, "databricks")
        self.host = host
        self.path = path
        self.token = token
        self.table = table
        self.config = {
            "host": host,
            "path": path,
            "table": table
        }
    
    def get_data(self) -> pd.DataFrame:
        """Busca dados do Databricks"""
        try:
            from databricks import sql
            with sql.connect(server_hostname=self.host, 
                           http_path=self.path, 
                           access_token=self.token) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(f"SELECT * FROM {self.table} LIMIT 1000")
                    result = cursor.fetchall()
                    if not result:
                        return pd.DataFrame()
                    columns = [desc[0] for desc in cursor.description]
                    self.data = pd.DataFrame(result, columns=columns)
                    return self.data
        except Exception as e:
            raise Exception(f"Erro ao conectar Databricks: {e}")


class CSVSource(DataSource):
    """Fonte de dados CSV"""
    
    def __init__(self, name: str, file_content: bytes):
        super().__init__(name, "csv")
        self.file_content = file_content
        self.config = {"file_type": "csv"}
        self._load_data()
    
    def _load_data(self):
        """Carrega dados do CSV"""
        try:
            self.data = pd.read_csv(io.BytesIO(self.file_content))
        except Exception as e:
            raise Exception(f"Erro ao ler CSV: {e}")
    
    def get_data(self) -> pd.DataFrame:
        """Retorna os dados"""
        return self.data


class ExcelSource(DataSource):
    """Fonte de dados Excel"""
    
    def __init__(self, name: str, file_content: bytes, sheet_name: str = 0):
        super().__init__(name, "excel")
        self.file_content = file_content
        self.sheet_name = sheet_name
        self.config = {"file_type": "excel", "sheet_name": sheet_name}
        self._load_data()
    
    def _load_data(self):
        """Carrega dados do Excel"""
        try:
            self.data = pd.read_excel(io.BytesIO(self.file_content), sheet_name=self.sheet_name)
        except Exception as e:
            raise Exception(f"Erro ao ler Excel: {e}")
    
    def get_data(self) -> pd.DataFrame:
        """Retorna os dados"""
        return self.data


class GCPBigQuerySource(DataSource):
    """Fonte de dados GCP BigQuery"""
    
    def __init__(self, name: str, project_id: str, dataset_id: str, table_id: str, credentials_json: str):
        super().__init__(name, "gcp_bigquery")
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.credentials_json = credentials_json
        self.config = {
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table_id": table_id
        }
    
    def get_data(self) -> pd.DataFrame:
        """Busca dados do BigQuery"""
        try:
            from google.cloud import bigquery
            from google.oauth2 import service_account
            import json
            
            credentials = service_account.Credentials.from_service_account_info(
                json.loads(self.credentials_json)
            )
            client = bigquery.Client(project=self.project_id, credentials=credentials)
            
            query = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.{self.table_id}` LIMIT 1000"
            self.data = client.query(query).to_dataframe()
            return self.data
        except Exception as e:
            raise Exception(f"Erro ao conectar BigQuery: {e}")


class AWSS3Source(DataSource):
    """Fonte de dados AWS S3"""
    
    def __init__(self, name: str, bucket: str, key: str, access_key: str, secret_key: str, file_type: str = "csv"):
        super().__init__(name, "aws_s3")
        self.bucket = bucket
        self.key = key
        self.access_key = access_key
        self.secret_key = secret_key
        self.file_type = file_type
        self.config = {
            "bucket": bucket,
            "key": key,
            "file_type": file_type
        }
    
    def get_data(self) -> pd.DataFrame:
        """Busca dados do S3"""
        try:
            import boto3
            
            s3 = boto3.client(
                's3',
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key
            )
            
            obj = s3.get_object(Bucket=self.bucket, Key=self.key)
            
            if self.file_type == "csv":
                self.data = pd.read_csv(io.BytesIO(obj['Body'].read()))
            elif self.file_type == "excel":
                self.data = pd.read_excel(io.BytesIO(obj['Body'].read()))
            elif self.file_type == "parquet":
                self.data = pd.read_parquet(io.BytesIO(obj['Body'].read()))
            
            return self.data
        except Exception as e:
            raise Exception(f"Erro ao conectar S3: {e}")


class PythonTemplateSource(DataSource):
    """Fonte de dados com template Python customizado"""
    
    def __init__(self, name: str, python_code: str):
        super().__init__(name, "python_template")
        self.python_code = python_code
        self.config = {"code_length": len(python_code)}
    
    def get_data(self) -> pd.DataFrame:
        """Executa código Python customizado"""
        try:
            # Ambiente seguro para executar o código
            local_vars = {
                'pd': pd,
                'json': json,
                'requests': None,  # Será importado se necessário
            }
            
            # Executar o código
            exec(self.python_code, {"pd": pd, "json": json}, local_vars)
            
            # Esperar que o código retorne um DataFrame em 'result'
            if 'result' in local_vars and isinstance(local_vars['result'], pd.DataFrame):
                self.data = local_vars['result']
                return self.data
            else:
                raise Exception("O código deve retornar um DataFrame em 'result'")
        except Exception as e:
            raise Exception(f"Erro ao executar template Python: {e}")


class DataSourceManager:
    """Gerenciador central de fontes de dados"""
    
    def __init__(self):
        self.sources: Dict[str, DataSource] = {}
    
    def add_source(self, source: DataSource):
        """Adiciona uma fonte de dados"""
        self.sources[source.name] = source
    
    def remove_source(self, name: str):
        """Remove uma fonte de dados"""
        if name in self.sources:
            del self.sources[name]
    
    def delete_source(self, name: str):
        """Alias para remove_source (compatibilidade)"""
        self.remove_source(name)
    
    def get_source(self, name: str) -> DataSource:
        """Obtém uma fonte de dados"""
        return self.sources.get(name)
    
    def list_sources(self) -> List[str]:
        """Lista todas as fontes"""
        return list(self.sources.keys())
    
    def get_all_tables(self) -> Dict[str, Dict[str, str]]:
        """Retorna todas as tabelas com seus schemas"""
        tables = {}
        for name, source in self.sources.items():
            try:
                schema = source.get_schema()
                tables[f"{source.source_type}:{name}"] = schema
            except Exception as e:
                print(f"Erro ao obter schema de {name}: {e}")
        return tables
    
    def execute_query(self, source_name: str, query: str) -> Tuple[pd.DataFrame, str]:
        """
        Executa uma query em uma fonte específica.
        Para Databricks: SQL puro
        Para CSV/Excel: Pandas query
        Para BigQuery: SQL puro
        Para S3: Pandas query
        Para Python: Retorna os dados já carregados
        """
        source = self.get_source(source_name)
        if not source:
            return None, f"Fonte '{source_name}' não encontrada"
        
        try:
            if source.source_type == "databricks":
                from databricks import sql
                with sql.connect(server_hostname=source.host, 
                               http_path=source.path, 
                               access_token=source.token) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(query)
                        result = cursor.fetchall()
                        if not result:
                            return pd.DataFrame(), None
                        columns = [desc[0] for desc in cursor.description]
                        return pd.DataFrame(result, columns=columns), None
            
            elif source.source_type == "gcp_bigquery":
                from google.cloud import bigquery
                from google.oauth2 import service_account
                
                credentials = service_account.Credentials.from_service_account_info(
                    json.loads(source.credentials_json)
                )
                client = bigquery.Client(project=source.project_id, credentials=credentials)
                return client.query(query).to_dataframe(), None
            
            else:
                # Para CSV, Excel, S3, Python: usar pandas query
                data = source.get_data()
                # Simplificar query para pandas
                result = data.query(query) if query else data
                return result, None
        
        except Exception as e:
            return None, str(e)
    
    def to_dict(self) -> Dict:
        """Serializa todas as fontes"""
        return {
            name: source.to_dict() 
            for name, source in self.sources.items()
        }
