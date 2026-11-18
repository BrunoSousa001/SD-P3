# TPF · Líder (LeaderBot)

## Configuração do Token

O token do bot Discord **nunca** deve ser incluído no repositório. O ficheiro `.gitignore` já está configurado para ignorar ficheiros `.env` e chaves sensíveis.

O token pode ser configurado de três formas (por ordem de preferência):

1. **Ficheiro `.env`** (na pasta do projeto `tpf-lider` ou na pasta `E2-1`):
   ```env
   BOT_TOKEN=o_teu_token_aqui
   ```

2. **Variável de Ambiente do Sistema:**
   ```powershell
   $env:BOT_TOKEN="o_teu_token_aqui"          # Windows PowerShell
   ```

3. **Propriedade de Sistema Java:**
   ```bash
   java -Dtoken="o_teu_token_aqui" ...
   ```

## Como Arrancar o Líder

Podes arrancar o bot de duas formas simples:

1. **Via PowerShell (com o wrapper incluído):**
   ```powershell
   cd Sprints\E2-1\tpf-lider
   .\run.ps1
   ```

2. **Via Maven (se tiveres Maven instalado ou via IDE):**
   ```bash
   mvn compile exec:java
   ```
