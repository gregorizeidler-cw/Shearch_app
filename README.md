# Shearch - Monitoramento de Notícias e Extração de Entidades

Shearch é uma ferramenta de monitoramento de notícias que extrai entidades (pessoas e empresas) das notícias encontradas e as compara com registros no BigQuery para identificar potenciais correspondências de interesse.

## Características

- Busca de notícias recentes sobre tópicos específicos (ex: lavagem de dinheiro, fintechs)
- Extração de conteúdo de notícias da web
- Identificação de entidades (pessoas e empresas) usando GPT-4o
- Integração com BigQuery para comparar entidades com dados existentes
- Notificações via Slack para correspondências encontradas

## Requisitos

- Python 3.8+
- Conta na OpenAI com acesso à API (para usar o GPT-4o)
- Acesso à API do BigQuery
- Token de API do Slack
- Bibliotecas Python (listadas em `requirements.txt`)

## Instalação

1. Clone o repositório:
   ```
   git clone https://github.com/SEU_USUARIO/shearch.git
   cd shearch
   ```

2. Instale as dependências:
   ```
   pip install -r requirements.txt
   ```

3. Configure as variáveis de ambiente:
   - Copie o arquivo `.env.example` para `.env`
   - Preencha suas credenciais no arquivo `.env`

## Configuração

1. **API da OpenAI**: Obtenha uma chave da API em [OpenAI](https://platform.openai.com)
2. **Slack**: 
   - Crie um [app no Slack](https://api.slack.com/apps)
   - Gere um token de bot
   - Adicione o bot ao canal onde deseja receber notificações
3. **BigQuery**:
   - Configure o acesso ao BigQuery com as permissões adequadas
   - O script usa o método de autenticação interativa por padrão

## Uso

Execute o script principal:
```
python app.py
```

O script irá:
1. Autenticar com o BigQuery
2. Buscar notícias recentes sobre os tópicos configurados
3. Extrair entidades das notícias
4. Comparar com dados do BigQuery
5. Enviar alertas para o Slack quando correspondências relevantes forem encontradas

## Personalização

- Modifique a lista `palavras_chave` em `app.py` para ajustar os tópicos de busca
- Ajuste os limites de score no código para controlar a sensibilidade dos alertas
- Edite as funções de filtragem para personalizar a extração de entidades

## Contribuições

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou enviar pull requests com melhorias.

## Licença

[MIT](LICENSE)
