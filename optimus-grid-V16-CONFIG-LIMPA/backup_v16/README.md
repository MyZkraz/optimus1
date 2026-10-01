
# V15 - COMPORTAMENTO REAL - NÃO CRIA MILHARES DE PADRÕES

## Problema que você falou (certo!):
Antes cada suporte/resistência que funcionava virava "padrão" novo e criava milhares. Isso NÃO é padrão de comportamento gráfico!

## Solução V15:
- **Comportamento real** = sequência de comportamento do mercado que se repete, não só suporte/resistência que funcionou uma vez
- Ex de comportamento real: veio de queda forte + volatilidade caindo + volume caindo + POC no meio + sem busca de liquidez + lateral com muitos toques = acumulação real (gente grande comprando escondido)
- **Sempre compara** com comportamentos que já temos na biblioteca (biblioteca_viva.json)
- **Só cria padrão novo se surgir comportamento novo de verdade** (distância vetorial >2.5)
- Se for parecido com um que já existe, só conta como repetição daquele comportamento, não cria padrão novo à toa
- Total de comportamentos únicos fica em 20-50, não milhares!

## Outros ajustes que pediu:
- Nomes Delta_a1, Delta_a2, Delta_b1 etc - nunca acaba em Padrão 1, Padrão 2
- % entre ordens 0.45% a 0.85% travado - nada fora disso vai pro premium (menos que 0.45% fica preso sem abrir ordem, mais que 0.85% abre pouca ordem)
- Simulação 6h pra não ficar preso: testa se nas últimas 6h tocou pelo menos 30% das linhas do grid
- Probabilidade de fechamento baseado nas últimas 6h: de 15 a 22 ordens dentro de 1 a 3h
- Mostra sucesso e média mínima de fechamento nas repetições 24h em cada par
- Notícias a cada 30s em loop sem parar, destacando: Delta_a1 tá com tudo essa semana, apareceu em 30 pares diferentes com média mínima de fechamento de 15 a 30 ordens dentro de 1 a 3h em grid curto ou mais em grid longo
- Configuração detalhada pra leigo: explica o que é cada coisa (entrada, grids, %ord, alavancagem, faixa, etc)

## Como rodar:
npm install
node server.js
http://localhost:3000
