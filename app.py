import streamlit as st
import google.generativeai as genai
from databricks import sql
import pandas as pd
import json
from data_sources import (
    DataSourceManager, CSVSource, ExcelSource,
    GCPBigQuerySource, AWSS3Source, PythonTemplateSource
)
from schema_editor import SchemaRegistry, SchemaEditor

# --- CONFIGURAÇÕES ---
st.set_page_config(
    page_title="AI Assistant para Análise de Dados 🤖 v2.5",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
    .main { background-color: #F8F9FA; }
    .stSidebar { background: linear-gradient(135deg, #2962FF 0%, #1a3fa0 100%); }
    [data-testid="stSidebar"] { color: white; }
    .stSidebar [data-testid="stMarkdownContainer"] p { color: white; }
    button[kind="icon"] { background: transparent !important; border: none !important; }
    button[kind="icon"]:hover { background: transparent !important; }
    </style>
    """, unsafe_allow_html=True)

# --- SECRETS ---
try:
    GEMINI_KEY = st.secrets["GEMINI_API_KEY"]
except:
    GEMINI_KEY = ""

try:
    DB_SERVER = st.secrets["DB_SERVER"]
except:
    DB_SERVER = ""

try:
    DB_HTTP_PATH = st.secrets["DB_HTTP_PATH"]
except:
    DB_HTTP_PATH = ""

try:
    DB_TOKEN = st.secrets["DB_TOKEN"]
except:
    DB_TOKEN = ""

DB_SCHEMA = "workspace.viny_dev"

# --- SESSION STATE ---
if "data_source_manager" not in st.session_state:
    st.session_state.data_source_manager = DataSourceManager()

if "schema_registry" not in st.session_state:
    st.session_state.schema_registry = SchemaRegistry()

if "catalog" not in st.session_state:
    st.session_state.catalog = {}

if "selected_sources" not in st.session_state:
    st.session_state.selected_sources = {}

if "messages" not in st.session_state:
    st.session_state.messages = []

if "show_modal" not in st.session_state:
    st.session_state.show_modal = False

if "modal_type" not in st.session_state:
    st.session_state.modal_type = None

if "editing_source" not in st.session_state:
    st.session_state.editing_source = None

if "show_preview" not in st.session_state:
    st.session_state.show_preview = False

if "preview_data" not in st.session_state:
    st.session_state.preview_data = None

if "gemini_key" not in st.session_state:
    st.session_state.gemini_key = GEMINI_KEY

if "db_host" not in st.session_state:
    st.session_state.db_host = DB_SERVER

if "db_path" not in st.session_state:
    st.session_state.db_path = DB_HTTP_PATH

if "db_token" not in st.session_state:
    st.session_state.db_token = DB_TOKEN

if "selected_model" not in st.session_state:
    st.session_state.selected_model = "models/gemini-2.5-flash"

# --- FUNÇÕES ---

def get_database_catalog(host, path, token, schema):
    try:
        with sql.connect(server_hostname=host, http_path=path, access_token=token) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"SHOW TABLES IN {schema}")
                tables = cursor.fetchall()
                
                catalog = {}
                for table in tables:
                    table_name = f"{schema}.{table[1]}"
                    cursor.execute(f"DESCRIBE TABLE {table_name}")
                    columns = cursor.fetchall()
                    
                    schema_info = {}
                    for col in columns:
                        col_name = col[0]
                        col_type = col[1]
                        schema_info[col_name] = col_type
                    
                    catalog[table_name] = schema_info
                
                return catalog
    except Exception as e:
        st.error(f"Erro: {e}")
        return {}

def get_table_preview(host, path, token, table_name, limit=10):
    """Executa SELECT * FROM table LIMIT X para preview"""
    try:
        with sql.connect(server_hostname=host, http_path=path, access_token=token) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
                result = cursor.fetchall()
                if not result:
                    return pd.DataFrame()
                columns = [desc[0] for desc in cursor.description]
                return pd.DataFrame(result, columns=columns)
    except Exception as e:
        st.error(f"Erro ao carregar preview: {e}")
        return None

def generate_sql_query(pergunta, selected_tables, model_name, api_key):
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(model_name)
        
        schema_context = "Tabelas disponíveis:\n"
        for table_name, schema_info in selected_tables.items():
            schema_context += f"\n{table_name}:\n"
            for col_name, col_type in schema_info.items():
                schema_context += f"  - {col_name}: {col_type}\n"
        
        prompt = f"""
        Você é um especialista em SQL Databricks.
        {schema_context}
        
        Pergunta do usuário: "{pergunta}"
        
        Gere uma query SQL válida para responder a pergunta.
        Retorne APENAS a query SQL, sem explicações.
        """
        
        response = model.generate_content(prompt)
        sql_text = response.text.strip()
        # Remover prefixo ```sql se existir
        if sql_text.lower().startswith('```sql'):
            sql_text = sql_text[6:].strip()
        # Remover prefixo "sql" se existir
        elif sql_text.lower().startswith('sql'):
            sql_text = sql_text[3:].strip()
        # Remover ``` no final se existir
        if sql_text.endswith('```'):
            sql_text = sql_text[:-3].strip()
        # Remover quebras de linha no início
        sql_text = sql_text.lstrip()
        return sql_text
    except:
        return None

def split_and_clean_queries(sql_text):
    queries = sql_text.split(';')
    cleaned_queries = []
    for query in queries:
        query = query.strip()
        if query:
            cleaned_queries.append(query)
    return cleaned_queries

def execute_multiple_queries(host, path, token, queries):
    results = []
    try:
        with sql.connect(server_hostname=host, http_path=path, access_token=token) as connection:
            with connection.cursor() as cursor:
                for query in queries:
                    try:
                        cursor.execute(query)
                        result = cursor.fetchall()
                        columns = [desc[0] for desc in cursor.description]
                        df = pd.DataFrame(result, columns=columns)
                        results.append({
                            'query': query,
                            'df': df,
                            'success': True,
                            'error': None
                        })
                    except Exception as e:
                        results.append({
                            'query': query,
                            'df': None,
                            'success': False,
                            'error': str(e)
                        })
        return results
    except Exception as e:
        return [{
            'query': 'connection',
            'df': None,
            'success': False,
            'error': f"Erro de conexao: {str(e)}"
        }]

def interpret_results(pergunta, df, model_name, api_key):
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(model_name)
        
        data_summary = df.to_string(index=False)
        
        prompt_interpret = f"""
        Você é um Analista de Negócios especializado em análise de dados.
        O usuário perguntou: "{pergunta}"
        Os dados retornados foram:
        {data_summary}
        
        Resuma os resultados em linguagem de negócios clara e profissional. 
        Destaque os pontos principais. Seja conciso.
        """
        
        response = model.generate_content(prompt_interpret)
        return response.text
    except Exception as e:
        return f"Erro: {e}"

# --- SIDEBAR ---
with st.sidebar:
    st.title("⚙️ Configurações")
    
    tab1, tab2, tab3 = st.tabs(["🔌 Databricks", "📊 Fontes", "🔑 API"])
    
    # TAB 1: DATABRICKS
    with tab1:
        st.subheader("Databricks")
        
        if st.session_state.db_host and st.session_state.db_path and st.session_state.db_token:
            st.success("✅ Configurado")
            db_host = st.session_state.db_host
            db_path = st.session_state.db_path
            db_token = st.session_state.db_token
        else:
            st.warning("⚠️ Configure nos secrets")
            db_host = st.text_input("Host", value=st.session_state.db_host)
            db_path = st.text_input("Path", value=st.session_state.db_path)
            db_token = st.text_input("Token", type="password", value=st.session_state.db_token)
            st.session_state.db_host = db_host
            st.session_state.db_path = db_path
            st.session_state.db_token = db_token
        
        if st.button("🔄 Carregar Catalog", use_container_width=True):
            if not db_host or not db_path or not db_token:
                st.error("❌ Configure credenciais!")
            else:
                with st.spinner("Descobrindo tabelas..."):
                    st.session_state.catalog = get_database_catalog(db_host, db_path, db_token, DB_SCHEMA)
                    if st.session_state.catalog:
                        for table in st.session_state.catalog.keys():
                            if table not in st.session_state.selected_sources:
                                st.session_state.selected_sources[table] = True
                        st.success(f"✅ {len(st.session_state.catalog)} tabelas!")
        
        # Exibir tabelas
        if st.session_state.catalog:
            st.divider()
            st.subheader("📋 Tabelas")
            
            # CSS para remover botão branco dos ícones
            st.markdown("""
            <style>
            button[kind="secondary"] { background: transparent; border: none; padding: 0; }
            button[kind="secondary"]:hover { background: transparent; }
            </style>
            """, unsafe_allow_html=True)
            
            table_types = {}
            for table_name in sorted(st.session_state.catalog.keys()):
                table_short = table_name.split(".")[-1]
                if "gold" in table_short:
                    tipo = "🥇 Gold"
                elif "silver" in table_short:
                    tipo = "🥈 Silver"
                elif "dim_" in table_short:
                    tipo = "📐 Dimensão"
                elif "fact_" in table_short:
                    tipo = "📊 Fato"
                else:
                    tipo = "📋 Outras"
                
                if tipo not in table_types:
                    table_types[tipo] = []
                table_types[tipo].append(table_name)
            
            for tipo in sorted(table_types.keys()):
                with st.expander(tipo, expanded=True):
                    for table_name in table_types[tipo]:
                        table_short = table_name.split(".")[-1]
                        
                        # Layout: checkbox + nome + ícones (sem quebra de linha)
                        # Usar proporções fixas que não quebram
                        col_check, col_name, col_icons = st.columns(
                            [0.5, 5, 0.8], 
                            gap="small",
                            vertical_alignment="center"
                        )
                        
                        with col_check:
                            is_selected = st.session_state.selected_sources.get(table_name, True)
                            st.session_state.selected_sources[table_name] = st.checkbox(
                                "",
                                value=is_selected,
                                key=f"db_{table_name}",
                                label_visibility="collapsed"
                            )
                        
                        with col_name:
                            st.caption(table_short)
                        
                        with col_icons:
                            # Usar colunas internas para os ícones (sem quebra)
                            icon_col1, icon_col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                            
                            with icon_col1:
                                if st.button("⚙️", key=f"edit_db_{table_name}", help="Editar", use_container_width=False):
                                    st.session_state.editing_source = table_name
                                    st.session_state.show_preview = False
                                    st.rerun()
                            
                            with icon_col2:
                                if st.button("🗑️", key=f"del_db_{table_name}", help="Remover", use_container_width=False):
                                    if table_name in st.session_state.selected_sources:
                                        del st.session_state.selected_sources[table_name]
                                    st.rerun()
    
    # TAB 2: FONTES CUSTOMIZADAS
    with tab2:
        st.subheader("Fontes de Dados")
        
        if st.button("➕ Novas Fontes", use_container_width=True, key="btn_new_source"):
            st.session_state.show_modal = True
            st.rerun()
        
        st.divider()
        sources = st.session_state.data_source_manager.list_sources()
        
        if sources:
            st.write(f"**{len(sources)} fonte(s)**")
            
            for source_name in sources:
                source = st.session_state.data_source_manager.get_source(source_name)
                
                # Layout: checkbox + nome + ícones (sem quebra de linha)
                col_check, col_name, col_icons = st.columns(
                    [0.5, 4, 0.8], 
                    gap="small",
                    vertical_alignment="center"
                )
                
                with col_check:
                    is_selected = st.session_state.selected_sources.get(source_name, True)
                    st.session_state.selected_sources[source_name] = st.checkbox(
                        "",
                        value=is_selected,
                        key=f"custom_{source_name}",
                        label_visibility="collapsed"
                    )
                
                with col_name:
                    st.caption(f"📊 {source_name} ({source.source_type})")
                
                with col_icons:
                    # Usar colunas internas para os ícones (sem quebra)
                    icon_col1, icon_col2 = st.columns([1, 1], gap="small", vertical_alignment="center")
                    
                    with icon_col1:
                        if st.button("⚙️", key=f"edit_custom_{source_name}", help="Editar", use_container_width=False):
                            st.session_state.editing_source = source_name
                            st.session_state.show_preview = False
                            st.rerun()
                    
                    with icon_col2:
                        if st.button("🗑️", key=f"del_custom_{source_name}", help="Remover", use_container_width=False):
                            st.session_state.data_source_manager.delete_source(source_name)
                            if source_name in st.session_state.selected_sources:
                                del st.session_state.selected_sources[source_name]
                            st.rerun()
        else:
            st.info("Nenhuma fonte customizada adicionada")
    
    # TAB 3: API
    with tab3:
        st.subheader("Google Gemini")
        
        if st.session_state.gemini_key:
            st.success("✅ Configurado")
        else:
            st.warning("⚠️ Configure nos secrets")
        
        gemini_key = st.text_input("API Key", type="password", value=st.session_state.gemini_key)
        st.session_state.gemini_key = gemini_key
        
        st.divider()
        st.write("**Modelo:**")
        st.session_state.selected_model = st.selectbox(
            "Selecione o modelo",
            ["models/gemini-2.5-flash", "models/gemini-2.0-flash", "models/gemini-1.5-pro"],
            index=0
        )

# --- MODAL: EDITOR DE SCHEMA COM ST.DIALOG ---
@st.dialog("✏️ Editar Schema", width="large")
def show_schema_editor():
    if not st.session_state.editing_source:
        return
    
    editing_name = st.session_state.editing_source
    table_short = editing_name.split(".")[-1]
    
    st.markdown(f"### {table_short}")
    st.divider()
    
    try:
        # Preparar editor
        if editing_name in st.session_state.catalog:
            source_data = st.session_state.catalog[editing_name]
            editor = SchemaEditor(editing_name, schema=source_data)
        else:
            source = st.session_state.data_source_manager.get_source(editing_name)
            data = source.get_data()
            editor = SchemaEditor(editing_name, data)
        
        st.session_state.schema_registry.register_schema(editing_name, editor)
        
        # Botão para carregar preview (lazy loading)
        if st.button("👁️ Carregar Preview", use_container_width=True):
            st.session_state.show_preview = True
        
        # Mostrar preview apenas se solicitado
        if st.session_state.show_preview:
            st.divider()
            with st.spinner("Carregando dados..."):
                if editing_name in st.session_state.catalog:
                    # Databricks
                    preview_data = get_table_preview(
                        st.session_state.db_host,
                        st.session_state.db_path,
                        st.session_state.db_token,
                        editing_name,
                        limit=10
                    )
                else:
                    # Fonte customizada
                    source = st.session_state.data_source_manager.get_source(editing_name)
                    preview_data = source.get_data().head(10)
                
                if preview_data is not None and not preview_data.empty:
                    st.write("**👁️ Preview (10 primeiras linhas):**")
                    st.dataframe(preview_data, use_container_width=True)
                else:
                    st.warning("Sem dados para preview")
        
        st.divider()
        
        st.write("**✏️ Editar Colunas:**")
        columns = editor.get_columns()
        
        for col_name in columns:
            col1, col2, col3 = st.columns([2, 2, 1])
            
            with col1:
                new_name = st.text_input(f"Nome", value=col_name, key=f"col_name_{col_name}")
                if new_name != col_name:
                    editor.rename_column(col_name, new_name)
            
            with col2:
                current_type = editor.get_column_type(col_name)
                new_type = st.selectbox(
                    f"Tipo",
                    ["string", "int", "float", "bool", "datetime", "date", "time", "decimal", "binary", "array", "struct", "null"],
                    index=["string", "int", "float", "bool", "datetime", "date", "time", "decimal", "binary", "array", "struct", "null"].index(current_type) if current_type in ["string", "int", "float", "bool", "datetime", "date", "time", "decimal", "binary", "array", "struct", "null"] else 0,
                    key=f"col_type_{col_name}"
                )
                if new_type != current_type:
                    editor.change_column_type(col_name, new_type)
            
            with col3:
                if st.button("🗑️", key=f"del_col_{col_name}"):
                    editor.delete_column(col_name)
                    st.rerun()
        
        st.divider()
        st.write("**➕ Adicionar Coluna:**")
        col1, col2 = st.columns(2)
        with col1:
            new_col_name = st.text_input("Nome", key="new_col_name")
        with col2:
            new_col_type = st.selectbox("Tipo", ["string", "int", "float", "bool", "datetime", "date", "time", "decimal", "binary", "array", "struct", "null"], key="new_col_type")
        
        if st.button("➕ Adicionar", use_container_width=True):
            if new_col_name:
                editor.add_column(new_col_name, new_col_type)
                st.success(f"✅ Adicionado!")
                st.rerun()
        
        st.divider()
        col_save, col_cancel = st.columns(2)
        with col_save:
            if st.button("💾 Salvar", use_container_width=True):
                st.session_state.schema_registry.register_schema(editing_name, editor)
                st.success("✅ Salvo!")
                st.session_state.editing_source = None
                st.rerun()
        
        with col_cancel:
            if st.button("❌ Fechar", use_container_width=True):
                st.session_state.editing_source = None
                st.rerun()
    
    except Exception as e:
        st.error(f"Erro: {e}")

# --- MODAL: NOVAS FONTES COM ST.DIALOG ---
@st.dialog("➕ Adicionar Nova Fonte")
def show_add_source_modal():
    # Inicializar estado do selectbox
    if "source_type_selected" not in st.session_state:
        st.session_state.source_type_selected = "CSV"
    
    source_type = st.selectbox(
        "Tipo de Fonte", 
        ["CSV", "Excel", "GCP BigQuery", "AWS S3", "Template Python"],
        index=["CSV", "Excel", "GCP BigQuery", "AWS S3", "Template Python"].index(st.session_state.source_type_selected),
        key="source_type_select"
    )
    st.session_state.source_type_selected = source_type
    
    source_name = ""
    
    if source_type == "CSV":
        st.write("**📄 Upload CSV**")
        csv_file = st.file_uploader("Arquivo", type="csv", key="csv_upload_modal")
        if csv_file:
            source_name = st.text_input("Nome", value=csv_file.name.replace(".csv", ""), key="csv_name_modal")
            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("✅ Adicionar", use_container_width=True, key="csv_add_btn"):
                    try:
                        csv_source = CSVSource(source_name, csv_file.getvalue())
                        st.session_state.data_source_manager.add_source(csv_source)
                        st.session_state.selected_sources[source_name] = True
                        st.success(f"✅ Adicionado!")
                        st.session_state.show_modal = False
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro: {e}")
            with col_b:
                if st.button("❌ Cancelar", use_container_width=True, key="csv_cancel_btn"):
                    st.session_state.show_modal = False
                    st.rerun()
    
    elif source_type == "Excel":
        st.write("**📊 Upload Excel**")
        excel_file = st.file_uploader("Arquivo", type=["xlsx", "xls"], key="excel_upload_modal")
        if excel_file:
            source_name = st.text_input("Nome", value=excel_file.name.split(".")[0], key="excel_name_modal")
            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("✅ Adicionar", use_container_width=True, key="excel_add_btn"):
                    try:
                        excel_source = ExcelSource(source_name, excel_file.getvalue())
                        st.session_state.data_source_manager.add_source(excel_source)
                        st.session_state.selected_sources[source_name] = True
                        st.success(f"✅ Adicionado!")
                        st.session_state.show_modal = False
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro: {e}")
            with col_b:
                if st.button("❌ Cancelar", use_container_width=True, key="excel_cancel_btn"):
                    st.session_state.show_modal = False
                    st.rerun()
    
    elif source_type == "GCP BigQuery":
        st.write("**☁️ Google Cloud BigQuery**")
        st.info("Configure suas credenciais do GCP")
        project_id = st.text_input("Project ID", key="gcp_project_modal")
        dataset_id = st.text_input("Dataset ID", key="gcp_dataset_modal")
        credentials_json = st.text_area("Credenciais JSON (opcional)", height=100, key="gcp_creds_modal",
                                       help="Cole o conteúdo do arquivo JSON de credenciais do GCP")
        if project_id and dataset_id:
            source_name = st.text_input("Nome da Fonte", value=f"bigquery_{dataset_id}", key="gcp_name_modal")
            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("✅ Adicionar", use_container_width=True, key="gcp_add_btn"):
                    try:
                        gcp_source = GCPBigQuerySource(source_name, project_id, dataset_id)
                        if credentials_json:
                            gcp_source.config["credentials"] = credentials_json
                        st.session_state.data_source_manager.add_source(gcp_source)
                        st.session_state.selected_sources[source_name] = True
                        st.success(f"✅ Adicionado!")
                        st.session_state.show_modal = False
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro: {e}")
            with col_b:
                if st.button("❌ Cancelar", use_container_width=True, key="gcp_cancel_btn"):
                    st.session_state.show_modal = False
                    st.rerun()
    
    elif source_type == "AWS S3":
        st.write("**☁️ Amazon S3**")
        st.info("Configure suas credenciais da AWS")
        bucket_name = st.text_input("Bucket Name", key="aws_bucket_modal")
        aws_access_key = st.text_input("AWS Access Key ID", key="aws_access_key_modal", type="password")
        aws_secret_key = st.text_input("AWS Secret Access Key", key="aws_secret_key_modal", type="password")
        aws_region = st.text_input("AWS Region", value="us-east-1", key="aws_region_modal")
        if bucket_name and aws_access_key and aws_secret_key:
            source_name = st.text_input("Nome da Fonte", value=f"s3_{bucket_name}", key="aws_name_modal")
            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("✅ Adicionar", use_container_width=True, key="aws_add_btn"):
                    try:
                        aws_source = AWSS3Source(source_name, bucket_name)
                        aws_source.config["access_key"] = aws_access_key
                        aws_source.config["secret_key"] = aws_secret_key
                        aws_source.config["region"] = aws_region
                        st.session_state.data_source_manager.add_source(aws_source)
                        st.session_state.selected_sources[source_name] = True
                        st.success(f"✅ Adicionado!")
                        st.session_state.show_modal = False
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro: {e}")
            with col_b:
                if st.button("❌ Cancelar", use_container_width=True, key="aws_cancel_btn"):
                    st.session_state.show_modal = False
                    st.rerun()
    
    elif source_type == "Template Python":
        st.write("**🐍 Template Python Customizado**")
        st.info("Escreva código Python que retorne um DataFrame")
        python_code = st.text_area("Código Python", height=200, key="python_code_modal", 
                                   value="import pandas as pd\nresult = pd.DataFrame({'col1': [1, 2, 3]})")
        if python_code:
            source_name = st.text_input("Nome da Fonte", value="custom_python", key="python_name_modal")
            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("✅ Adicionar", use_container_width=True, key="python_add_btn"):
                    try:
                        python_source = PythonTemplateSource(source_name, python_code)
                        st.session_state.data_source_manager.add_source(python_source)
                        st.session_state.selected_sources[source_name] = True
                        st.success(f"✅ Adicionado!")
                        st.session_state.show_modal = False
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro: {e}")
            with col_b:
                if st.button("❌ Cancelar", use_container_width=True, key="python_cancel_btn"):
                    st.session_state.show_modal = False
                    st.rerun()

# --- RENDERIZAR MODAIS ---
# Garantir que apenas um modal seja aberto por vez
if st.session_state.editing_source:
    show_schema_editor()
if st.session_state.show_modal:
    show_add_source_modal()

# --- INTERFACE PRINCIPAL ---
st.title("AI Assistant para Análise de Dados 🤖")
st.subheader("Seu Analista de Dados Inteligente")

selected_db_tables = [t for t, selected in st.session_state.selected_sources.items() if selected and t in st.session_state.catalog]
selected_custom_sources = [s for s in st.session_state.data_source_manager.list_sources() if st.session_state.selected_sources.get(s, False)]

if not selected_db_tables and not selected_custom_sources:
    st.warning("⚠️ Selecione fontes na sidebar para começar.")
else:
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
            if "df" in message:
                with st.expander("Ver dados brutos"):
                    st.dataframe(message["df"])
    
    user_input = st.chat_input("Faça uma pergunta sobre seus dados...")
    
    if user_input:
        st.session_state.messages.append({"role": "user", "content": user_input})
        
        with st.chat_message("user"):
            st.markdown(user_input)
        
        with st.chat_message("assistant"):
            with st.spinner("Analisando..."):
                # Preparar contexto de tabelas
                selected_tables = {}
                for table_name in selected_db_tables:
                    if table_name in st.session_state.catalog:
                        selected_tables[table_name] = st.session_state.catalog[table_name]
                
                # Gerar SQL
                sql_query = generate_sql_query(
                    user_input,
                    selected_tables,
                    st.session_state.selected_model,
                    st.session_state.gemini_key
                )
                
                if sql_query:
                    with st.expander("📝 SQL Gerado", expanded=True):
                        st.code(sql_query, language="sql")
                    
                    # Executar query(ies)
                    try:
                        queries = split_and_clean_queries(sql_query)
                        
                        if len(queries) > 1:
                            st.info(f"📊 Executando {len(queries)} queries...")
                            results = execute_multiple_queries(
                                st.session_state.db_host,
                                st.session_state.db_path,
                                st.session_state.db_token,
                                queries
                            )
                            
                            all_responses = []
                            combined_df = None
                            
                            for i, result in enumerate(results, 1):
                                if result['success']:
                                    st.success(f"✅ Query {i} executada com sucesso")
                                    st.dataframe(result['df'], use_container_width=True)
                                    
                                    response = interpret_results(
                                        user_input,
                                        result['df'],
                                        st.session_state.selected_model,
                                        st.session_state.gemini_key
                                    )
                                    all_responses.append(response)
                                    
                                    if combined_df is None:
                                        combined_df = result['df']
                                else:
                                    st.error(f"❌ Query {i} falhou: {result['error']}")
                            
                            final_response = "\n\n".join(all_responses) if all_responses else "Nenhuma query foi executada com sucesso."
                            st.markdown(final_response)

                            # Expander para dados brutos
                            with st.expander("📋 Ver dados brutos", expanded=False):
                                st.dataframe(combined_df, use_container_width=True)
                            st.session_state.messages.append({
                                "role": "assistant",
                                "content": final_response,
                                "df": combined_df
                            })
                        else:
                            with sql.connect(
                                server_hostname=st.session_state.db_host,
                                http_path=st.session_state.db_path,
                                access_token=st.session_state.db_token
                            ) as connection:
                                with connection.cursor() as cursor:
                                    cursor.execute(sql_query)
                                    result = cursor.fetchall()
                                    columns = [desc[0] for desc in cursor.description]
                                    df = pd.DataFrame(result, columns=columns)
                            
                            response = interpret_results(
                                user_input,
                                df,
                                st.session_state.selected_model,
                                st.session_state.gemini_key
                            )
                            
                            st.markdown(response)
                            
                            # Expander para dados brutos
                            with st.expander("📋 Ver dados brutos", expanded=False):
                                st.dataframe(df, use_container_width=True)
                            
                            st.session_state.messages.append({
                                "role": "assistant",
                                "content": response,
                                "df": df
                            })
                    except Exception as e:
                        st.error(f"Erro ao executar query: {e}")
                else:
                    st.error("Não consegui gerar uma query válida")
