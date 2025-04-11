import datetime
import re
import time
import json
import os
from urllib.parse import urlparse, unquote
from dotenv import load_dotenv

# Carrega variáveis do arquivo .env
load_dotenv()

# Importar dependências externas
from slack import WebClient
from slack.errors import SlackApiError
from google.cloud import bigquery
import pydata_google_auth
import requests
from googlesearch import search
import openai

# Configurar a API do OpenAI
openai.api_key = os.getenv("OPENAI_API_KEY")

# Configuração do Slack
SLACK_TOKEN = os.getenv("SLACK_TOKEN")
CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")

# Configuração do BigQuery
BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID", "infinitepay-production")

# Configuração de autenticação para BigQuery
def autenticar_bigquery(projeto_id=BIGQUERY_PROJECT_ID):
    print(f"Iniciando autenticação com o Google Cloud para o projeto: {projeto_id}...")
    
    # Escopos necessários para BigQuery
    SCOPES = [
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/cloud-platform'
    ]
    
    try:
        # Solicita autenticação interativa
        credentials = pydata_google_auth.get_user_credentials(
            SCOPES,
            auth_local_webserver=True,
            client_id=None,  # Usa as credenciais padrão
            client_secret=None,  # Usa as credenciais padrão
        )
        
        # Cria o cliente BigQuery com as credenciais autenticadas
        client = bigquery.Client(credentials=credentials, project=projeto_id)
        print("Autenticação com BigQuery concluída com sucesso!")
        return client
    except Exception as e:
        print(f"Erro durante autenticação: {e}")
        raise

# Função para buscar notícias
def buscar_noticias(query, num_results=10, dias_anteriores=1):
    hoje = datetime.date.today()
    data_anterior = hoje - datetime.timedelta(days=dias_anteriores)
    
    print(f"Buscando notícias dos últimos {dias_anteriores} dias para '{query}'...")
    
    resultados = []
    try:
        # Adiciona "after:" para limitar pela data
        query_com_data = f"{query} after:{data_anterior.strftime('%Y-%m-%d')}"
        
        # Domínios a ignorar (redes sociais, etc)
        dominios_ignorar = [
            'facebook.com', 'twitter.com', 'instagram.com', 
            'youtube.com', 'linkedin.com', 'tiktok.com'
        ]
        
        # Usa a biblioteca googlesearch-python para fazer a busca
        for j in search(query_com_data, num_results=num_results, lang="pt"):
            # Filtrar resultados que não são sites de notícias
            if not any(dominio in j for dominio in dominios_ignorar):
                resultados.append(j)
                print(f"Encontrado: {j}")
            
        return resultados
    except Exception as e:
        print(f"Erro ao buscar '{query}': {e}")
        return []

# Função para extrair o conteúdo completo da notícia
def obter_conteudo_da_pagina(url):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        }
        
        # Aumentar o timeout para sites mais lentos
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        
        # Salvar o HTML original para depuração
        html = response.text
        
        # Extrair o título
        titulo_match = re.search(r'<title>(.*?)</title>', html, re.IGNORECASE)
        titulo = titulo_match.group(1).strip() if titulo_match else extrair_titulo_da_url(url)
        
        # Etapa 1: Limpar o HTML de elementos não relevantes
        # Remover scripts, estilos, comentários e tags de navegação
        html_limpo = re.sub(r'<script[^>]*>.*?</script>', ' ', html, flags=re.DOTALL)
        html_limpo = re.sub(r'<style[^>]*>.*?</style>', ' ', html_limpo, flags=re.DOTALL)
        html_limpo = re.sub(r'<!--.*?-->', ' ', html_limpo, flags=re.DOTALL)
        html_limpo = re.sub(r'<nav[^>]*>.*?</nav>', ' ', html_limpo, flags=re.DOTALL)
        html_limpo = re.sub(r'<header[^>]*>.*?</header>', ' ', html_limpo, flags=re.DOTALL)
        html_limpo = re.sub(r'<footer[^>]*>.*?</footer>', ' ', html_limpo, flags=re.DOTALL)
        html_limpo = re.sub(r'<aside[^>]*>.*?</aside>', ' ', html_limpo, flags=re.DOTALL)
        html_limpo = re.sub(r'<iframe[^>]*>.*?</iframe>', ' ', html_limpo, flags=re.DOTALL)
        
        # Etapa 2: Tentar identificar o conteúdo principal da notícia
        # Estratégia 1: Buscar elementos com classes/IDs comuns de conteúdo principal
        conteudo = ""
        
        # Lista de padrões para encontrar o conteúdo principal em sites de notícias
        padroes_conteudo = [
            # Elementos <article>
            r'<article[^>]*>(.*?)</article>',
            # Divs com classes comuns para conteúdo de notícias
            r'<div[^>]*class="[^"]*(?:content|article|post|news|materia|texto|entry|body|main)[^"]*"[^>]*>(.*?)</div>',
            # Divs com ID de conteúdo
            r'<div[^>]*id="[^"]*(?:content|article|post|news|materia|texto|entry|body|main)[^"]*"[^>]*>(.*?)</div>',
            # Elementos <main>
            r'<main[^>]*>(.*?)</main>',
            # Seções de conteúdo
            r'<section[^>]*class="[^"]*(?:content|article|post|news|materia)[^"]*"[^>]*>(.*?)</section>'
        ]
        
        # Tentar cada padrão até encontrar conteúdo significativo
        for padrao in padroes_conteudo:
            matches = re.findall(padrao, html_limpo, re.DOTALL)
            
            # Verificar se encontrou algo significativo
            if matches:
                for match in matches:
                    # Verificar se o conteúdo tem tamanho mínimo para ser relevante
                    if len(match) > 500:  # Pelo menos 500 caracteres
                        conteudo = match
                        break
                
                if conteudo:
                    break
        
        # Estratégia 2: Se não encontrou conteúdo pelo padrão,
        # extrair todos os parágrafos <p> com conteúdo significativo
        if not conteudo or len(conteudo) < 500:
            paragrafos = re.findall(r'<p[^>]*>(.*?)</p>', html_limpo, re.DOTALL)
            conteudo = ' '.join([p for p in paragrafos if len(p) > 30])  # Parágrafos com pelo menos 30 caracteres
        
        # Etapa 3: Limpar o conteúdo extraído
        # Remover todas as tags HTML restantes, preservando o texto
        conteudo_limpo = re.sub(r'<[^>]+>', ' ', conteudo)
        
        # Normalizar espaços e quebras de linha
        conteudo_limpo = re.sub(r'\s+', ' ', conteudo_limpo).strip()
        
        # Converter entidades HTML para caracteres
        conteudo_limpo = conteudo_limpo.replace('&nbsp;', ' ')
        conteudo_limpo = conteudo_limpo.replace('&amp;', '&')
        conteudo_limpo = conteudo_limpo.replace('&lt;', '<')
        conteudo_limpo = conteudo_limpo.replace('&gt;', '>')
        conteudo_limpo = conteudo_limpo.replace('&quot;', '"')
        conteudo_limpo = conteudo_limpo.replace('&#39;', "'")
        
        # Se o conteúdo for muito pequeno, usar fallback
        if len(conteudo_limpo) < 300:
            # Extrair todo o texto do HTML como último recurso
            todo_texto = re.sub(r'<[^>]+>', ' ', html_limpo)
            todo_texto = re.sub(r'\s+', ' ', todo_texto).strip()
            
            # Dividir em parágrafos por pontuação final
            paragrafos = re.split(r'(?<=[.!?])\s+', todo_texto)
            
            # Selecionar apenas parágrafos significativos (com mais de X caracteres)
            paragrafos_significativos = [p for p in paragrafos if len(p) > 50]
            
            conteudo_limpo = ' '.join(paragrafos_significativos)
        
        # Se ainda for muito grande, limitar o tamanho
        if len(conteudo_limpo) > 15000:
            conteudo_limpo = conteudo_limpo[:15000]
            
        # Imprime o tamanho do conteúdo extraído para depuração
        print(f"Tamanho do conteúdo extraído: {len(conteudo_limpo)} caracteres")
        
        return {
            "titulo": titulo, 
            "conteudo": conteudo_limpo,
            "url": url
        }
    except Exception as e:
        print(f"Erro ao obter conteúdo de {url}: {e}")
        return {"titulo": extrair_titulo_da_url(url), "conteudo": "", "url": url}

def extrair_titulo_da_url(url):
    # Extrai título a partir da URL como fallback
    parse_result = urlparse(url)
    path = parse_result.path
    
    # Obter o último segmento da URL e substituir hífens por espaços
    titulo = unquote(path.split('/')[-1].replace('-', ' '))
    return titulo

# Função para extrair entidades usando GPT
def extrair_entidades_gpt(texto):
    try:
        # Configura a chamada para a API do OpenAI usando a nova interface v1.x
        params = {
            "model": "gpt-4o-2024-11-20",  # Usar GPT-4o direto
            "messages": [
                {"role": "system", "content": "Você é um especialista em análise de textos. Sua tarefa é extrair somente nomes próprios completos de pessoas e empresas mencionadas em notícias."},
                {"role": "user", "content": f"""
                Analise o seguinte texto de notícia e extraia APENAS:
                
                1. Nomes COMPLETOS de PESSOAS
                2. Nomes COMPLETOS de EMPRESAS
                
                IMPORTANTE:
                - NÃO extraia nomes de cidades, estados ou países
                - NÃO extraia órgãos públicos como "Polícia Federal", "Ministério Público", etc
                - NÃO extraia termos genéricos como "empresa", "companhia", "organização"
                - Extraia apenas nomes próprios completos específicos
                
                Texto da notícia: {texto}
                
                Forneça APENAS o seguinte formato JSON sem explicações adicionais:
                [
                  {{"texto": "Nome da Pessoa", "tipo": "PER"}},
                  {{"texto": "Nome da Empresa", "tipo": "ORG"}}
                ]
                
                Se não houver nenhuma pessoa ou empresa específica mencionada, retorne uma lista vazia [].
                """}
            ],
            "temperature": 0.0  # Zero para respostas determinísticas
        }
        
        # Faz a chamada para a API do OpenAI
        response = openai.chat.completions.create(**params)
        
        # Extrai a resposta
        resultado = response.choices[0].message.content.strip()
        print(f"Resposta GPT: {resultado}")
        
        # Tenta converter a resposta para JSON
        try:
            # Remover qualquer texto que não seja JSON
            if resultado.startswith("```json"):
                resultado = resultado.replace("```json", "").replace("```", "")
            elif resultado.startswith("```"):
                resultado = resultado.replace("```", "")
                
            resultado = resultado.strip()
            
            # Tentar converter para JSON
            entidades = json.loads(resultado)
            
            # Verificar se é uma lista e tem elementos
            if isinstance(entidades, list) and len(entidades) > 0:
                # Filtrar entidades para remover entidades não desejadas
                entidades_filtradas = []
                termos_ignorar = [
                    "g1", "brasil", "cnn brasil", "cnn", "veja", "estadão", "youtube", 
                    "agência brasil", "rio de janeiro", "são paulo", "polícia civil", 
                    "polícia federal", "pf", "banco digital", "banco", "fintech", 
                    "empresa", "companhia", "organização", "instituição"
                ]
                
                # Lista de nomes de cidades brasileiras comuns que podem ser erroneamente extraídas como entidades
                cidades = ["rio", "são paulo", "brasília", "salvador", "fortaleza", "recife", 
                          "belo horizonte", "manaus", "curitiba", "porto alegre", "belém",
                          "goiânia", "guarulhos", "campinas", "são luís", "maceió"]
                
                for entidade in entidades:
                    nome = entidade.get("texto", "").lower()
                    
                    # Não incluir termos genéricos ou locais geográficos
                    if (nome and nome not in termos_ignorar and 
                        not any(cidade in nome for cidade in cidades) and
                        not any(orgao.lower() in nome for orgao in ["ministério", "polícia", "receita", "secretaria"])):
                        entidades_filtradas.append(entidade)
                
                print(f"Entidades extraídas e filtradas: {entidades_filtradas}")
                return entidades_filtradas
            return []
        except json.JSONDecodeError:
            print(f"Erro ao decodificar JSON da resposta GPT: {resultado}")
            return []
            
    except Exception as e:
        print(f"Erro ao chamar API do OpenAI: {e}")
        return []

# Função para extrair entidades (pessoas e organizações) do conteúdo da notícia
def extrair_entidades_do_conteudo(url):
    # Tenta obter o título e conteúdo real da página
    dados = obter_conteudo_da_pagina(url)
    print(f"Título extraído: {dados['titulo']}")
    
    # Combina título e conteúdo para a análise
    texto_completo = f"{dados['titulo']} \n\n {dados['conteudo']}"
    
    # Use a função de extração de entidades baseada em GPT
    entidades = extrair_entidades_gpt(texto_completo)
    
    return entidades

# Função para buscar entidades no BigQuery
def buscar_no_bigquery(entidades):
    # Usa o cliente global já autenticado
    client = bigquery_client
    
    resultados = []
    
    for entidade in entidades:
        # Constrói a consulta SQL para encontrar correspondências parciais no merchant_name
        query = f"""
        SELECT user_id, merchant_name
        FROM `{BIGQUERY_PROJECT_ID}.maindb.merchants`
        WHERE LOWER(merchant_name) LIKE '%{entidade["texto"].lower()}%'
        """
        
        try:
            # Executa a consulta
            query_job = client.query(query)
            
            # Processa os resultados
            for row in query_job:
                score = calcular_score_fuzzy(entidade, row.merchant_name)
                resultados.append({
                    "entidade": entidade["texto"],
                    "tipo": entidade["tipo"],
                    "user_id": row.user_id,
                    "merchant_name": row.merchant_name,
                    "score": score
                })
        except Exception as e:
            print(f"Erro ao consultar BigQuery para '{entidade['texto']}': {e}")
    
    return resultados

# Função para calcular score de relevância
def calcular_score_fuzzy(entidade, merchant_name):
    entidade_texto = entidade["texto"].lower()
    merchant_lower = merchant_name.lower()
    
    # Score base de acordo com o tipo de entidade
    base_score = {
        "PER": 0.5,  # Pessoas
        "ORG": 0.6,  # Organizações
        "ENT": 0.4   # Entidades genéricas
    }.get(entidade["tipo"], 0.3)
    
    # Calcular similaridade
    token_similarity = calcular_similaridade(entidade_texto, merchant_lower)
    substring_similarity = calcular_similaridade_substring(entidade_texto, merchant_lower)
    
    # Verificar correspondência exata
    if entidade_texto == merchant_lower:
        exact_match = 1.0
    # Verificar se entidade é uma palavra inteira dentro do merchant name
    elif re.search(r'\b' + re.escape(entidade_texto) + r'\b', merchant_lower):
        exact_match = 0.8
    else:
        exact_match = 0.0
    
    # Calcular score final combinando os resultados
    final_score = (token_similarity * 0.3) + (substring_similarity * 0.3) + (exact_match * 0.4) + (base_score * 0.2)
    
    # Limitar a 1.0
    return min(final_score, 1.0)

# Função para calcular similaridade entre conjuntos de tokens
def calcular_similaridade(str1, str2):
    # Converter para minúsculas e dividir em tokens (palavras)
    tokens1 = set(str1.lower().split())
    tokens2 = set(str2.lower().split())
    
    # Verificar se os conjuntos estão vazios
    if not tokens1 or not tokens2:
        return 0.0
    
    # Calcular interseção (palavras em comum)
    intersecao = tokens1.intersection(tokens2)
    
    # Calcular coeficiente de similaridade (Jaccard)
    return len(intersecao) / max(len(tokens1), len(tokens2))

# Função para verificar se uma string está contida em outra
def calcular_similaridade_substring(str1, str2):
    # Verificar se uma string é substring da outra
    if str1 in str2:
        return 0.7 + (len(str1) / len(str2)) * 0.3  # Valoriza strings maiores
    elif str2 in str1:
        return 0.7 + (len(str2) / len(str1)) * 0.3
    
    # Verificar substrings parciais
    # Comprimento mínimo para considerar uma substring
    min_length = 3
    max_substr = ""
    
    # Verificar substrings de str1 em str2
    for i in range(len(str1)):
        for j in range(i + min_length, len(str1) + 1):
            substr = str1[i:j]
            if substr in str2 and len(substr) > len(max_substr):
                max_substr = substr
    
    # Se encontrou uma substring significativa
    if len(max_substr) > min_length:
        return 0.4 + (len(max_substr) / len(str1)) * 0.3
    
    return 0.0

# Função para enviar mensagem para o Slack
def enviar_para_slack(mensagem, thread_ts=None):
    try:
        # Inicializa o cliente do Slack
        slack_client = WebClient(token=SLACK_TOKEN)
        
        response = slack_client.chat_postMessage(
            channel=CHANNEL_ID,
            text=mensagem,
            thread_ts=thread_ts
        )
        print(f"Mensagem enviada para o Slack: {mensagem[:50]}...")
        return response
    except Exception as e:
        print(f"Erro ao enviar mensagem para o Slack: {e}")
        return None

def main():
    # Palavras-chave para busca
    palavras_chave = [
        "lavagem de dinheiro",
        "fintech"
    ]
    
    # Inicializa o cliente BigQuery
    global bigquery_client
    bigquery_client = autenticar_bigquery()
    
    # Buscar notícias para cada palavra-chave
    todas_noticias = {}
    for palavra in palavras_chave:
        print(f"\nBuscando notícias para '{palavra}'...")
        todas_noticias[palavra] = buscar_noticias(palavra)
        time.sleep(2)  # Pausa para evitar bloqueio API
    
    # Analisar notícias, extrair entidades e buscar no BigQuery
    todos_resultados = []
    
    for palavra, links in todas_noticias.items():
        if links:
            # Enviar links de notícias
            mensagem = f"Top notícias do último dia para '{palavra}':"
            response = enviar_para_slack(mensagem)
            thread_ts = response.data.get('ts') if response else None
            
            for i, link in enumerate(links, 1):
                mensagem = f"{i}. {link}"
                enviar_para_slack(mensagem, thread_ts)
                
                # Extrair entidades do conteúdo completo da notícia
                entidades = extrair_entidades_do_conteudo(link)
                
                if entidades:
                    print(f"Entidades extraídas de '{link}': {entidades}")
                    
                    # Buscar entidades no BigQuery
                    resultados = buscar_no_bigquery(entidades)
                    todos_resultados.extend(resultados)
                else:
                    print(f"Nenhuma entidade encontrada em: {link}")
        else:
            print(f"Nenhuma notícia encontrada para '{palavra}'")
    
    # Agrupar resultados por user_id para evitar duplicações
    resultados_por_usuario = {}
    for resultado in todos_resultados:
        user_id = resultado["user_id"]
        
        if user_id in resultados_por_usuario:
            # Atualiza o score se o novo for maior
            if resultado["score"] > resultados_por_usuario[user_id]["score"]:
                resultados_por_usuario[user_id] = resultado
        else:
            resultados_por_usuario[user_id] = resultado
    
    # Ordenar resultados por score (decrescente)
    resultados_ordenados = sorted(
        resultados_por_usuario.values(), 
        key=lambda x: x["score"], 
        reverse=True
    )
    
    # Enviar resultados para o Slack
    if resultados_ordenados:
        mensagem = "*ALERTA: Possíveis correspondências de entidades em notícias recentes*"
        response = enviar_para_slack(mensagem)
        thread_ts = response.data.get('ts') if response else None
        
        for resultado in resultados_ordenados:
            # Mostra apenas resultados com score significativo
            if resultado['score'] > 0.4:
                score_percentual = int(resultado['score'] * 100)
                
                # Definir nível de alerta com base no score
                if resultado['score'] > 0.8:
                    nivel_alerta = "🔴 ALTO"
                elif resultado['score'] > 0.6:
                    nivel_alerta = "🟠 MÉDIO"
                else:
                    nivel_alerta = "🟡 BAIXO"
                    
                mensagem = (
                    f"*Entidade:* {resultado['entidade']} ({resultado['tipo']})\n"
                    f"*User ID:* {resultado['user_id']}\n"
                    f"*Merchant Name:* {resultado['merchant_name']}\n"
                    f"*Score:* {score_percentual}% ({nivel_alerta})"
                )
                enviar_para_slack(mensagem, thread_ts)
    else:
        mensagem = "Nenhuma correspondência encontrada entre entidades de notícias e dados de merchants."
        enviar_para_slack(mensagem)

    print("Processo concluído.")

if __name__ == "__main__":
    main()
