# TPF · Líder (LeaderBot)

## Configuração do Token

O token do bot Discord **nunca** deve ser incluído no repositório. O ficheiro `.gitignore` já está configurado para ignorar ficheiros `.env` e chaves sensíveis.

O token pode ser configurado de três formas (por ordem de preferência):

1. **Ficheiro `.env`** (na pasta do projeto ou na pasta `E2S1`):
   ```env
   BOT_TOKEN=o_teu_token_aqui
   ```

2. **Variável de Ambiente do Sistema:**
   ```bash
   export BOT_TOKEN="o_teu_token_aqui"        # Linux / macOS
   set BOT_TOKEN="o_teu_token_aqui"           # Windows CMD
   $env:BOT_TOKEN="o_teu_token_aqui"          # Windows PowerShell
   ```

3. **Propriedade de Sistema Java:**
   ```bash
   java -Dtoken="o_teu_token_aqui" ...
   ```

## Como Arrancar o Líder

1. No IntelliJ IDEA, abrir o projeto ou adicionar o `pom.xml` como módulo Maven.
2. Aguardar que o IntelliJ resolva a dependência do JDA.
3. Executar a classe principal `tpf.LeaderBot`.
