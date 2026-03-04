"""
Módulo de editor de schema para tabelas customizadas.
Permite editar nomes de colunas, tipos de dados, adicionar/remover colunas.
"""

import pandas as pd
from typing import Dict, List, Tuple
import json


class SchemaEditor:
    """Editor de schema para tabelas"""
    
    # Tipos de dados suportados
    SUPPORTED_TYPES = [
        "string",
        "int",
        "float",
        "bool",
        "datetime",
        "date",
        "time",
        "decimal",
        "binary",
        "array",
        "struct",
        "null"
    ]
    
    def __init__(self, table_name: str, df: pd.DataFrame = None, schema: Dict[str, str] = None):
        self.table_name = table_name
        self.df = df
        self.original_schema = schema or {}
        self.current_schema = self._infer_schema(df) if df is not None else schema or {}
        self.column_mappings = {}  # {old_name: new_name}
        self.deleted_columns = []
    
    def _infer_schema(self, df: pd.DataFrame) -> Dict[str, str]:
        """Infere o schema a partir do DataFrame"""
        schema = {}
        for col in df.columns:
            dtype = str(df[col].dtype)
            # Mapear tipos pandas para tipos mais legíveis
            if "int" in dtype:
                schema[col] = "int"
            elif "float" in dtype:
                schema[col] = "float"
            elif "bool" in dtype:
                schema[col] = "bool"
            elif "datetime" in dtype:
                schema[col] = "datetime"
            elif "object" in dtype:
                schema[col] = "string"
            else:
                schema[col] = dtype
        return schema
    
    def rename_column(self, old_name: str, new_name: str) -> Tuple[bool, str]:
        """Renomeia uma coluna"""
        if old_name not in self.current_schema:
            return False, f"Coluna '{old_name}' não encontrada"
        
        if new_name in self.current_schema and new_name != old_name:
            return False, f"Coluna '{new_name}' já existe"
        
        # Atualizar schema
        self.current_schema[new_name] = self.current_schema.pop(old_name)
        self.column_mappings[old_name] = new_name
        
        return True, f"Coluna renomeada de '{old_name}' para '{new_name}'"
    
    def change_column_type(self, column_name: str, new_type: str) -> Tuple[bool, str]:
        """Altera o tipo de uma coluna"""
        if column_name not in self.current_schema:
            return False, f"Coluna '{column_name}' não encontrada"
        
        if new_type not in self.SUPPORTED_TYPES:
            return False, f"Tipo '{new_type}' não suportado. Tipos: {', '.join(self.SUPPORTED_TYPES)}"
        
        self.current_schema[column_name] = new_type
        return True, f"Tipo da coluna '{column_name}' alterado para '{new_type}'"
    
    def add_column(self, column_name: str, column_type: str = "string") -> Tuple[bool, str]:
        """Adiciona uma nova coluna"""
        if column_name in self.current_schema:
            return False, f"Coluna '{column_name}' já existe"
        
        if column_type not in self.SUPPORTED_TYPES:
            return False, f"Tipo '{column_type}' não suportado"
        
        self.current_schema[column_name] = column_type
        return True, f"Coluna '{column_name}' adicionada com tipo '{column_type}'"
    
    def delete_column(self, column_name: str) -> Tuple[bool, str]:
        """Remove uma coluna"""
        if column_name not in self.current_schema:
            return False, f"Coluna '{column_name}' não encontrada"
        
        del self.current_schema[column_name]
        self.deleted_columns.append(column_name)
        return True, f"Coluna '{column_name}' removida"
    
    def get_schema(self) -> Dict[str, str]:
        """Retorna o schema atual"""
        return self.current_schema.copy()
    
    def get_columns(self) -> List[str]:
        """Retorna lista de colunas"""
        return list(self.current_schema.keys())
    
    def get_column_type(self, column_name: str) -> str:
        """Retorna o tipo de uma coluna"""
        return self.current_schema.get(column_name, "unknown")
    
    def apply_schema_to_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica o schema editado ao DataFrame"""
        # Renomear colunas
        for old_name, new_name in self.column_mappings.items():
            if old_name in df.columns:
                df = df.rename(columns={old_name: new_name})
        
        # Remover colunas deletadas
        df = df.drop(columns=[col for col in self.deleted_columns if col in df.columns])
        
        # Converter tipos (simplificado)
        for col, dtype in self.current_schema.items():
            if col in df.columns:
                try:
                    if dtype == "int":
                        df[col] = df[col].astype("int64")
                    elif dtype == "float":
                        df[col] = df[col].astype("float64")
                    elif dtype == "bool":
                        df[col] = df[col].astype("bool")
                    elif dtype == "datetime":
                        df[col] = pd.to_datetime(df[col])
                    elif dtype == "date":
                        df[col] = pd.to_datetime(df[col]).dt.date
                    elif dtype == "string":
                        df[col] = df[col].astype("str")
                except Exception as e:
                    print(f"Aviso: Não foi possível converter '{col}' para '{dtype}': {e}")
        
        return df
    
    def get_changes_summary(self) -> Dict:
        """Retorna um resumo das mudanças"""
        return {
            "renamed_columns": self.column_mappings,
            "deleted_columns": self.deleted_columns,
            "current_schema": self.current_schema,
            "total_columns": len(self.current_schema)
        }
    
    def reset(self):
        """Reseta as mudanças"""
        self.current_schema = self.original_schema.copy()
        self.column_mappings = {}
        self.deleted_columns = []
    
    def to_dict(self) -> Dict:
        """Serializa o editor"""
        return {
            "table_name": self.table_name,
            "schema": self.current_schema,
            "mappings": self.column_mappings,
            "deleted": self.deleted_columns
        }
    
    @staticmethod
    def from_dict(data: Dict) -> "SchemaEditor":
        """Desserializa um editor"""
        editor = SchemaEditor(data["table_name"])
        editor.current_schema = data.get("schema", {})
        editor.column_mappings = data.get("mappings", {})
        editor.deleted_columns = data.get("deleted", [])
        return editor


class SchemaRegistry:
    """Registro central de schemas customizados"""
    
    def __init__(self):
        self.schemas: Dict[str, SchemaEditor] = {}
    
    def register_schema(self, table_name: str, editor: SchemaEditor):
        """Registra um schema"""
        self.schemas[table_name] = editor
    
    def get_schema(self, table_name: str) -> SchemaEditor:
        """Obtém um schema"""
        return self.schemas.get(table_name)
    
    def list_schemas(self) -> List[str]:
        """Lista todos os schemas registrados"""
        return list(self.schemas.keys())
    
    def delete_schema(self, table_name: str):
        """Remove um schema"""
        if table_name in self.schemas:
            del self.schemas[table_name]
    
    def export_schemas(self) -> Dict:
        """Exporta todos os schemas"""
        return {
            name: editor.to_dict()
            for name, editor in self.schemas.items()
        }
    
    def import_schemas(self, data: Dict):
        """Importa schemas"""
        for table_name, schema_data in data.items():
            editor = SchemaEditor.from_dict(schema_data)
            self.register_schema(table_name, editor)
