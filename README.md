# iFood ABTest Data Pipeline

Este projeto realiza o download, processamento e salvamento dos dados de um experimento de teste A/B do iFood. O pipeline executa a extração de dados brutos (JSON/CSV/TAR), faz pré-processamento, joins e exporta tudo em formato Parquet para análises avançadas.

Além do script de download e processamento dos arquivos, o projeto inclui um **notebook Jupyter (`ifood_abtest.ipynb`)** com uma análise objetiva e visual dos dados, trazendo métricas e gráficos sobre o resultado do experimento.

---

## Tecnologias e Bibliotecas Utilizadas

- Python 3.8+
- PySpark
- tqdm
- requests
- pandas
- matplotlib

---

## Instalação

### 1. Clone o repositório

```bash
git clone git@github.com:jeacfontes/ifood_case.git
cd ifood_case/
```

### 2. Crie um ambiente virtual (opcional, mas recomendado)

```bash
python3 -m venv .venv
source .venv/bin/activate   # Linux/macOS
# .venv\Scripts\activate    # Windows
```

### 3. Instale as dependências

```bash
pip install -r requirements.txt
```

---

## 🚀 Como rodar localmente pelo VS Code

1. **Abra a pasta do projeto no Visual Studio Code**
2. Certifique-se de que seu interpretador Python está apontando para o ambiente virtual criado.
3. Tenha o Java instalado e o JAVA_HOME configurado (PySpark exige Java; recomenda-se Java 8 ou 11).
4. Execute o script principal:
   - No terminal integrado, rode:
     ```bash
     python3 ifood_abtest_data_pipeline.py
     ```
   - Ou clique com o botão direito no arquivo e escolha "Run Python File in Terminal".
5. Os arquivos processados serão salvos na pasta `trusted_data/` em formato `.parquet`.

---

## Análise dos Dados

Após a geração dos arquivos processados, você pode abrir o notebook **`ifood_abtest.ipynb`** para executar uma análise exploratória, obter métricas dos grupos de teste/controle e visualizar gráficos comparativos das principais métricas.

Recomenda-se o uso do **Jupyter Notebook** ou do próprio **VS Code** (com a extensão Python/Jupyter) para navegar e executar as células do notebook.

---

## Resumo do Processo Realizado

1. **Download de Dados:**  
   O script baixa automaticamente os datasets necessários de fontes públicas do iFood.

2. **Extração:**  
   Arquivos compactados (TAR.GZ) são extraídos para o diretório local.

3. **Leitura e Parsing:**  
   Os dados são lidos em DataFrames do PySpark com schemas definidos para garantir a qualidade.

4. **Pré-processamento:**  
   - Limpeza de registros inválidos.
   - Explosão de arrays de itens dos pedidos.

5. **Exportação:**  
   Os DataFrames tratados são salvos em formato Parquet na pasta `trusted_data/`, prontos para análises de BI ou exploração em notebooks.

6. **Análise dos Dados:**  
   O notebook `ifood_abtest.ipynb` traz análise exploratória, resumo das métricas principais do teste A/B, gráficos e insights objetivos para tomada de decisão.

---

##  Observações

- Certifique-se de ter Java instalado na máquina (PySpark depende disso).
- Os arquivos de dados são baixados automaticamente na primeira execução.
- A pasta `trusted_data/` é criada se não existir.

---

##  Autor

Zé Eduardo

---
