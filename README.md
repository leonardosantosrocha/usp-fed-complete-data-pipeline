## Contexto

Uma empresa de atuação nacional busca expandir sua presença no mercado e tornar suas estratégias comerciais mais eficientes. Para isso, precisa entender de forma estruturada como o poder de compra está distribuído entre diferentes regiões e perfis da população.

Atualmente, decisões de expansão, investimento e marketing são frequentemente baseadas em dados fragmentados e pouco estruturados, sem uma visão integrada de fatores essenciais como renda, volume populacional e diferenças regionais de consumo. Essa limitação dificulta a identificação de oportunidades reais de mercado e pode levar a alocações ineficientes de recursos, campanhas pouco assertivas e perda de potencial competitivo.

Diante desse cenário, surge a necessidade de consolidar e organizar os dados de forma analítica, permitindo uma visão clara e comparável entre regiões.

---

## Problema de Negócio

A ausência de uma análise integrada entre renda e densidade populacional impede responder, com precisão, perguntas fundamentais como:

- Onde estão concentrados os maiores volumes de consumidores?
- Quais regiões apresentam maior renda média?
- Existe relação entre volume populacional e poder de compra?
- Quais regiões combinam alto volume populacional com renda relevante?
- Onde estão as melhores oportunidades para expansão de mercado?

Sem essas respostas, decisões estratégicas são tomadas com baixa eficiência, aumentando o risco de investimentos mal direcionados.

---

## Objetivo

Este projeto tem como objetivo construir um pipeline de dados e uma camada analítica que permita identificar regiões com maior potencial econômico, combinando renda e volume populacional.

A proposta é transformar dados brutos em informações estruturadas, capazes de apoiar decisões estratégicas de negócio.

---

## Pergunta Principal

A principal pergunta que guia este projeto é:

**Onde estão os mercados com maior potencial de consumo?**

---

## Abordagem Analítica

Para responder a essa pergunta, o projeto utiliza uma modelagem dimensional que permite analisar os dados sob diferentes perspectivas:

- Dimensão geográfica (estado e região)
- Faixas de renda da população
- Volume de consumidores por faixa de renda
- Indicadores agregados por estado

A partir dessa estrutura, são construídos indicadores como:

- Renda média por indivíduo
- Volume total de renda por região
- Taxa efetiva de tributação
- Indicadores combinados de renda e população

Um dos principais outputs do projeto é um índice de potencial de mercado, que combina renda média com volume populacional ajustado (log da população), evitando distorções causadas por estados muito populosos. :contentReference[oaicite:0]{index=0}

---

## Resultado Esperado

Ao final, o projeto permite:

- Comparar estados de forma padronizada
- Identificar regiões com maior potencial econômico
- Apoiar decisões de expansão, investimento e marketing
- Melhorar a eficiência na alocação de recursos

Mais do que gerar dashboards, o objetivo é transformar dados em direcionamento estratégico.
