# OPTIMUS V17 — Premium Vivo + SQLite

Esta versão parte do V16 enviado e preserva os arquivos JSON existentes como espelho/backup. O SQLite local (`data/optimus.db`) é a fonte principal.

## O que mudou

### 1. Premium vivo
O Premium não é histórico. A cada ciclo concluído, a lista é reconstruída a partir dos candidatos que ainda satisfazem as condições atuais.

- Se um par deixa de ser observável, ele sai do Premium.
- O evento de saída é registrado em `premium_events`.
- O conhecimento histórico continua salvo para aprendizado.
- O frontend recebe `premiums_history` novamente ao final do ciclo, então sinais antigos também desaparecem da tela.

### 2. SQLite local
Não há servidor de banco separado.

Arquivo:
`data/optimus.db`

As estruturas principais ficam na tabela `kv`, com tabelas específicas para:

- `analysis_cursor`: último candle fechado processado por par.
- `premium_events`: entradas/saídas do Premium.

Os JSONs do V16 continuam no diretório `data/` e são atualizados como espelho para compatibilidade e backup.

### 3. Migração automática
Ao iniciar:

1. Se uma chave ainda não existe no SQLite, o sistema lê o JSON correspondente.
2. O conteúdo é gravado no SQLite.
3. A partir daí, o SQLite passa a ser a fonte principal.
4. Os JSONs continuam sendo atualizados pelo `save()`.

Portanto, não é necessário apagar nem converter manualmente os dados antigos.

### 4. Cursor incremental
O V17 não usa mais a regra "não analise esse par por 24h".

Agora cada par possui um cursor do último candle fechado realmente processado.

A análise pode consultar uma janela recente de contexto de até 3 dias, mas o aprendizado só registra uma nova ocorrência quando existe candle fechado posterior ao cursor.

Isso evita contar a mesma evidência repetidamente.

### 5. Zonas, não preços exatos
Suporte/resistência deixou de ser tratado como uma linha exata.

A largura da zona usa uma combinação de:

- faixa atual;
- ATR;
- margem adaptativa.

A proteção contra rompimento também usa uma margem variável, aproximadamente entre 15% e 28% da faixa conforme a volatilidade, em vez de um limite fixo de 12%.

Se o preço estiver perto do topo/fundo da faixa, o sinal entra em zona de perigo e não é colocado no Premium.

### 6. Comportamentos de mercado
Foi acrescentada uma camada para reconhecer evidências como:

- impulso anterior seguido de respiro/compressão;
- faixa com rejeições por sombras;
- falsas tentativas de rompimento;
- compressão de ATR;
- expansão de volatilidade;
- mudança de comportamento depois de uma tendência.

Esses sinais entram na assinatura comportamental junto dos outros fatores. O sistema continua evitando transformar cada suporte/resistência em um padrão.

## Requisitos

Node.js 22.5+.

O V17 usa `node:sqlite`, portanto não precisa instalar um pacote SQLite nativo separado.

## PowerShell

```powershell
cd .\optimus-grid-V16-CONFIG-LIMPA
npm install
npm run self-test
npm start
```

Depois abra:

`http://localhost:3000`

## Backup

Antes de trocar uma versão em produção, faça uma cópia da pasta `data/`.

O arquivo mais importante da memória atual passa a ser:

`data/optimus.db`

Os JSONs continuam presentes como cópia legível/compatível.

## Observação

O `self-test` valida SQLite, migração, cursor incremental e eventos do Premium. Ele não substitui um scan real contra a Binance.
