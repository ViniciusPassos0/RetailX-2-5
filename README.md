# AI Assistant para Análise de Dados 🤖 v2.5

Um assistente inteligente de análise de dados desenvolvido com Streamlit, Google Gemini e Databricks.

## 👨‍💻 Desenvolvido por

**Carlos Passos**

---

## 🚀 Como executar a aplicação

1. **Certifique-se de ter o Python instalado.**
2. **Instale as dependências:**
   ```bash
   pip install -r requirements.txt
   ```
3. **Execute o Streamlit:**
   ```bash
   streamlit run app.py
   ```

---

## ✨ Funcionalidades

- **Conversão de Linguagem Natural para SQL:** Utiliza o Google Gemini para transformar perguntas em Spark SQL
- **Suporte a Múltiplas Fontes de Dados:**
  - Databricks
  - CSV
  - Excel
  - Google BigQuery
  - Amazon S3
  - Templates Python
- **Interpretação Inteligente de Dados:** A IA analisa os resultados e fornece um resumo executivo
- **Editor de Schema:** Gerencie e customize o schema das suas tabelas
- **Interface Profissional:** Design limpo e intuitivo com modo chat
- **Expanders Colapsáveis:** SQL e dados brutos organizados em expanders para melhor UX
- **Suporte a Múltiplas Queries:** Perguntas complexas que geram múltiplas queries funcionam perfeitamente

---

## 🔑 Configuração de Credenciais

### Localmente
As credenciais são carregadas de um arquivo `.streamlit/secrets.toml`:

```toml
DATABRICKS_HOST = "seu-host"
DATABRICKS_PATH = "seu-path"
DATABRICKS_TOKEN = "seu-token"
GEMINI_API_KEY = "sua-chave"
```

### No Streamlit Cloud
Configure as secrets em **Settings → Secrets** com as mesmas chaves acima.

---

## 📊 Versão

**v2.5** - Refinement & Optimization
- SQL em expander colapsável
- Dados brutos em expander
- Suporte a múltiplas queries
- SQL parsing automático
- Interface limpa e profissional

---

## 🛠️ Tecnologias

- **Frontend:** Streamlit
- **IA:** Google Gemini API
- **Database:** Databricks
- **Linguagem:** Python 3.8+

---

## 📝 Licença

Desenvolvido por Carlos Passos - 2026

---

## 🤝 Suporte

Para dúvidas ou problemas, consulte a documentação de deployment ou os guias inclusos no projeto.
