# 🏆 Sports Betting Prediction & Recommendation Platform

> Plataforma inteligente de análise e recomendação de apostas esportivas baseada em Machine Learning, dados históricos e agentes LLM.

---

## 📌 Sumário

- [Visão Geral](#-visão-geral)
- [Arquitetura](#-arquitetura-do-sistema)
- [Camada de Dados](#-camada-de-dados)
- [Ingestão de Dados](#-ingestão-de-dados)
- [Feature Engineering](#-feature-engineering)
- [Machine Learning](#-machine-learning)
- [Recommendation Engine](#-recommendation-engine)
- [Coupon Service](#-coupon-service)
- [Backend (FastAPI)](#-backend-fastapi)
- [Banco de Dados](#-banco-de-dados)
- [Agentes LLM](#-agentes-langchain--langgraph)
- [Frontend](#-frontend)
- [Deploy](#-deploy)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [Problemas Conhecidos](#-problemas-conhecidos)
- [Roadmap](#-roadmap)

---

## 🧭 Visão Geral

Esta plataforma combina engenharia de dados, modelos de ML e agentes inteligentes para recomendar apostas esportivas com base em:

- 📊 **Dados históricos** — StatsBomb + FBref
- 🤖 **Modelos de Machine Learning** — pré-jogo e in-play
- ⚙️ **Backend robusto** — API REST com FastAPI
- 🎯 **Recomendação personalizada** — baseada em perfil de risco do usuário
- 🧠 **Agentes LLM** *(opcional)* — automação e interação via LangChain/LangGraph

---

## 🏛 Arquitetura do Sistema

```
[Data Sources]          ← StatsBomb + FBref
       ↓
[Ingestion Layer]       ← Download, leitura e normalização
       ↓
[Raw Schema - Postgres] ← Dados brutos
       ↓
[Feature Engineering]   ← Transformação e criação de features
       ↓
[Feature Store]         ← Armazenamento otimizado para ML
       ↓
[ML Models]             ← Treinamento e predição
       ↓
[Backend API (FastAPI)] ← Exposição dos dados e recomendações
       ↓
[Recommendation Engine] ← Score baseado em probabilidade × odd
       ↓
[Frontend / Agents]     ← Streamlit + LangChain Agents
```

---

## 🗄 Camada de Dados

### Fontes

| Fonte | Dados Disponíveis |
|-------|-------------------|
| **StatsBomb** | Matches, Events, Lineups |
| **FBref** | Estatísticas agregadas de jogadores |

### Schemas

#### `raw` — Dados Brutos

| Tabela | Descrição |
|--------|-----------|
| `raw.statsbomb_matches` | Partidas brutas |
| `raw.statsbomb_events` | Eventos de jogo |
| `raw.statsbomb_lineups` | Escalações |
| `raw.fbref_players` | Estatísticas de jogadores |

#### `silver` — Dados Tratados

| Tabela | Descrição |
|--------|-----------|
| `silver.matches` | Partidas normalizadas e enriquecidas |

#### `feature_store` — Features para ML

| Tabela | Descrição |
|--------|-----------|
| `feature_store.match_pre_game_features` | Features calculadas antes da partida |
| `feature_store.match_in_game_features` | Features calculadas durante a partida |

---

## ⚙️ Ingestão de Dados

### Pipeline

1. **Download** — Kaggle
2. **Leitura** — JSON / CSV
3. **Normalização** — Padronização de schemas
4. **Inserção** — Banco de dados PostgreSQL

### Roteamento de Tabelas

```python
def resolve_target_table(file_path: Path) -> str | None:
    parts = file_path.parts
    if "matches" in parts:
        return "raw.statsbomb_matches"
    if "lineups" in parts:
        return "raw.statsbomb_lineups"
    # ...
```

---

## 🧮 Feature Engineering

### Pré-jogo

| Feature | Descrição |
|---------|-----------|
| `home_matches_played_before` | Partidas disputadas pelo mandante |
| `away_matches_played_before` | Partidas disputadas pelo visitante |
| `home_last5_win_rate` | Taxa de vitórias (últimas 5 partidas) — mandante |
| `away_last5_win_rate` | Taxa de vitórias (últimas 5 partidas) — visitante |
| `home_last5_draw_rate` | Taxa de empates — mandante |
| `home_last5_loss_rate` | Taxa de derrotas — mandante |
| `home_home_last5_goals_for_avg` | Média de gols marcados em casa |
| `away_away_last5_goals_against_avg` | Média de gols sofridos fora |
| `home_distinct_players_last5` | Variação de escalação — mandante |
| `diff_win_rate` | Diferença de taxa de vitória |
| `diff_goals_for_avg` | Diferença de média de gols marcados |
| `diff_goals_against_avg` | Diferença de média de gols sofridos |

### In-play

- ⏱ Tempo de jogo
- 🏃 Posse de bola
- 🎯 Finalizações
- 🟨 Cartões
- ⚡ Pressão ofensiva

---

## 🤖 Machine Learning

### Objetivo

- Prever o resultado da partida: **Home / Draw / Away**
- Estimar probabilidades para cada desfecho

### Pipeline

```
Feature Store → Dataset → Train/Validation Split → Model → Prediction
```

### Boas Práticas Implementadas

- ✅ Separação por liga para evitar viés
- ✅ Separação por temporada (walk-forward validation)
- ✅ Prevenção de **data leakage**

---

## 🎯 Recommendation Engine

O score de recomendação combina a probabilidade estimada pelo modelo com a odd de mercado:

```python
recommendation_score = model_probability * market_odd
```

Quanto maior o score, mais atrativa é a aposta em termos de valor esperado.

---

## 🎟 Coupon Service

### Fluxo

```
Input
  ├── user_id
  ├── matches
  ├── target_risk
  └── max_selections
        ↓
Busca recomendações
        ↓
Filtra por nível de risco
        ↓
Ordena por score (decrescente)
        ↓
Calcula odd total do cupom
        ↓
Output: Cupom personalizado
```

### Implementação

```python
# Ordenação por score
filtered_sorted = sorted(
    filtered,
    key=lambda x: x["recommendation_score"],
    reverse=True
)

# Cálculo da odd total
total_odd = 1.0
for item in selected:
    total_odd *= item["market_odd"]
```

---

## 🗃 Backend (FastAPI)

### Endpoints

| Método | Rota | Descrição |
|--------|------|-----------|
| `GET` | `/matches` | Lista partidas disponíveis |
| `GET` | `/odds/{match_id}` | Odds de uma partida específica |
| `GET` | `/recommendations/{match_id}` | Recomendações para uma partida |
| `POST` | `/coupon` | Gera um cupom personalizado |

---

## 🧵 Banco de Dados

### Stack

- **PostgreSQL** — banco relacional principal
- **Supabase** — hospedagem cloud com autenticação e API

### Pool de Conexão

```python
pool = SimpleConnectionPool(minconn=1, maxconn=10)
```

### Problemas Comuns

| Problema | Causa | Solução |
|----------|-------|---------|
| Max clients atingido | Pool saturado no Supabase | Fechar conexões explicitamente |
| Conexões abertas | Falta de `conn.close()` | Usar context managers |
| Porta duplicada | Processo já rodando | Verificar e matar processo anterior |
| Erro de autenticação | Credenciais incorretas | Verificar `.env` |

---

## 🤖 Agentes (LangChain / LangGraph)

### Casos de Uso

- 🔍 Consultar odds de partidas
- 📋 Gerar múltiplas/cupons automaticamente
- 💬 Explicar recomendações em linguagem natural

### Arquitetura de Agentes

```
Router Agent
  ├── Recommendation Agent  ← Geração de cupons
  └── Odds Agent            ← Consulta de odds
```

### Boas Práticas

- ✅ Lógica de negócio implementada em **tools**, não no prompt
- ✅ Fluxos determinísticos com LangGraph
- ✅ Separação clara de responsabilidades por agente

---

## 📊 Frontend

Desenvolvido com **Streamlit** para prototipação rápida.

**Funcionalidades:**
- Visualização de jogos e partidas disponíveis
- Exibição de odds e recomendações
- Geração interativa de apostas e cupons

---

## 🚀 Deploy

| Ambiente | Tecnologia | Uso |
|----------|-----------|-----|
| Cloud | Supabase | Banco de dados e autenticação |
| Local | Docker | Ambiente isolado de desenvolvimento |
| Dev/Testes | ngrok | Exposição de endpoint local |

---

## 📁 Estrutura do Projeto

```
.
├── backend/
│   └── app/
│       ├── models/         # Schemas e modelos de dados
│       ├── services/       # Lógica de negócio
│       └── routes/         # Endpoints da API
│
├── ml/
│   ├── training/           # Scripts de treinamento
│   ├── features/           # Feature engineering
│   └── models/             # Modelos salvos
│
├── ingestion/
│   ├── kaggle/             # Download de datasets
│   └── pipelines/          # Pipelines de ingestão
│
├── agents/
│   ├── tools/              # Tools dos agentes LLM
│   └── graphs/             # Fluxos LangGraph
│
└── frontend/
    └── streamlit/          # Interface do usuário
```

---

## 🧩 Padrões de Projeto

| Padrão | Onde é Aplicado |
|--------|-----------------|
| **Service Layer Pattern** | Separação entre rotas e lógica de negócio |
| **Feature Store** | Centralização e reuso de features de ML |
| **Separation of Concerns** | Camadas bem definidas (raw → silver → feature store → ML) |

---

## ⚠️ Problemas Conhecidos

### Banco de Dados
- Pool de conexões pode saturar em cargas altas
- Conexões não fechadas corretamente em alguns fluxos

### Dados
- Features ausentes em algumas partidas com dados incompletos
- Volume reduzido de partidas em ligas menos populares

### Machine Learning
- Risco de **overfitting** por volume limitado de dados
- Mistura de ligas pode introduzir viés nos modelos

---

## 📈 Roadmap

- [ ] 🔴 Odds em tempo real via API de mercado
- [ ] ⚡ Modelo in-play avançado com dados ao vivo
- [ ] 💰 Módulo de gestão de bankroll (Kelly Criterion)
- [ ] 🧠 Reinforcement Learning para otimização de apostas
- [ ] 🤖 Auto-betting com aprovação automática de cupons

---

## 🧱 Stack Tecnológica

| Categoria | Tecnologia |
|-----------|-----------|
| Backend | FastAPI, Python |
| Banco de Dados | PostgreSQL, Supabase |
| Machine Learning | Scikit-learn, Pandas |
| Agentes | LangChain, LangGraph |
| Frontend | Streamlit |
| Dados | StatsBomb, FBref, Kaggle |
| Infraestrutura | Docker, ngrok |

---

> **Objetivo final:** Evoluir para uma plataforma inteligente de betting automatizado, integrando dados em tempo real, aprendizado por reforço e execução autônoma de apostas.
