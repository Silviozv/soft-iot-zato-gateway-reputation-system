import subprocess
import json
import os
import csv
from datetime import datetime

# ==========================================
# CONFIGURAÇÕES
# ==========================================
# Nome exato da imagem Docker dos seus gateways
IMAGEM_GATEWAY = "silviozv/soft-iot-gateway-zato:1.0.0"

# A porta interna do Zato onde o serviço REST está rodando
PORTA_ZATO = "11223"

# A rota exata do seu serviço Zato que devolve o JSON local
ENDPOINT_EXTRACAO = "/soft-iot/reputation/task/get-data"

# Diretório base onde as coleções serão acumuladas
DIRETORIO_SAIDA = "data_distributed"

# Prefixo do arquivo JSON bruto
PREFIXO_ARQUIVO = "backup_gateways_data_"

# ==========================================
# ALVOS DA REDE
# ==========================================
# Defina o número total de máquinas que o script deve acessar
TOTAL_MAQUINAS = 15 

# Gera a lista dinamicamente: ['larsid01', 'larsid02', ..., 'larsid15']
#MAQUINAS_SSH = [f"larsid{i:02d}" for i in range(1, 2)]
MAQUINAS_SSH = ["larsid01", "larsid02", "larsid04", "larsid13", "larsid14"]

def obter_containers_remotos():
    """Busca containers com a imagem do gateway via SSH em cada máquina."""
    gateways_encontrados = []
    
    for maquina in MAQUINAS_SSH:
        print(f"\n[*] Conectando em {maquina} para buscar gateways...")
        try:
            # Comando SSH para listar os IDs dos containers na máquina remota
            cmd_ids = f"ssh {maquina} 'docker ps -q --filter ancestor={IMAGEM_GATEWAY}'"
            resultado_ids = subprocess.check_output(cmd_ids, shell=True, stderr=subprocess.DEVNULL).decode('utf-8').strip().split('\n')
            ids = [cid for cid in resultado_ids if cid]

            if not ids:
                print(f"[-] Nenhum gateway rodando na {maquina}.")
                continue

            print(f"[+] Encontrados {len(ids)} gateways em {maquina}. Coletando nomes e IPs...")

            for cid in ids:
                # Pega o IP interno do container na máquina remota
                cmd_ip = f"ssh {maquina} 'docker inspect -f \"{{{{range.NetworkSettings.Networks}}}}{{{{.IPAddress}}}}{{{{end}}}}\" {cid}'"
                ip = subprocess.check_output(cmd_ip, shell=True, stderr=subprocess.DEVNULL).decode('utf-8').strip()
                
                # Pega o nome do container
                cmd_nome = f"ssh {maquina} 'docker inspect -f \"{{{{.Name}}}}\" {cid}'"
                nome = subprocess.check_output(cmd_nome, shell=True, stderr=subprocess.DEVNULL).decode('utf-8').strip().replace('/', '')
                
                if ip:
                    gateways_encontrados.append({
                        "maquina_host": maquina,
                        "container_id": cid,
                        "nome": nome,
                        "ip": ip
                    })

        except subprocess.CalledProcessError:
            print(f"[!] Falha de comunicação SSH com {maquina}. Máquina desligada ou sem acesso.")
            
    return gateways_encontrados

def extrair_dados_ssh(lista_gateways):
    """Utiliza o curl via SSH dentro da máquina remota para contornar a restrição da rede Docker."""
    dados_consolidados = {
        "data_extracao": datetime.now().isoformat(),
        "total_gateways": len(lista_gateways),
        "gateways": {}
    }

    for gateway in lista_gateways:
        maquina = gateway['maquina_host']
        nome = gateway['nome']
        ip = gateway['ip']
        url_interna = f"http://{ip}:{PORTA_ZATO}{ENDPOINT_EXTRACAO}"
        
        print(f"[*] Extraindo dados de {nome} (via {maquina})...")
        
        try:
            # Pede via SSH que a máquina remota faça um curl no IP interno do Docker dela mesma
            cmd_curl = f"ssh {maquina} 'curl -s -m 10 {url_interna}'"
            resposta_curl = subprocess.check_output(cmd_curl, shell=True, stderr=subprocess.DEVNULL).decode('utf-8').strip()
            
            if resposta_curl:
                try:
                    conteudo = json.loads(resposta_curl)
                    dados_consolidados['gateways'][nome] = {
                        "host": maquina,
                        "ip_interno": ip,
                        "status": "sucesso",
                        "dados": conteudo
                    }
                    print(f"  [+] Dados de {nome} coletados com sucesso.")
                except json.JSONDecodeError:
                    dados_consolidados['gateways'][nome] = {
                        "host": maquina,
                        "ip_interno": ip,
                        "status": "erro_json",
                        "resposta_bruta": resposta_curl
                    }
                    print(f"  [-] Erro: {nome} não retornou um JSON válido.")
            else:
                dados_consolidados['gateways'][nome] = {
                    "host": maquina,
                    "ip_interno": ip,
                    "status": "vazio"
                }
                print(f"  [-] {nome} retornou resposta vazia.")
                
        except subprocess.CalledProcessError:
            dados_consolidados['gateways'][nome] = {
                "host": maquina,
                "ip_interno": ip,
                "status": "falha_ssh_curl"
            }
            print(f"  [!] Falha ao executar o curl na máquina {maquina} para o container {nome}.")

    return dados_consolidados

def obter_proximo_diretorio():
    os.makedirs(DIRETORIO_SAIDA, exist_ok=True)
    numeros_coleta = []
    
    for nome_pasta in os.listdir(DIRETORIO_SAIDA):
        caminho_pasta = os.path.join(DIRETORIO_SAIDA, nome_pasta)
        if os.path.isdir(caminho_pasta) and nome_pasta.startswith("collect_"):
            try:
                numero = int(nome_pasta.split("_")[1])
                numeros_coleta.append(numero)
            except ValueError:
                pass
                
    proximo_numero = max(numeros_coleta) + 1 if numeros_coleta else 1
    novo_diretorio = os.path.join(DIRETORIO_SAIDA, f"collect_{proximo_numero}")
    os.makedirs(novo_diretorio, exist_ok=True)
    return novo_diretorio

def salvar_dados(dados):
    diretorio_atual = obter_proximo_diretorio()
    print(f"\n[📁] Diretório da execução criado: {diretorio_atual}")
    
    # 1. Salva o backup bruto JSON na raiz da pasta collect_X
    timestamp_atual = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_arquivo_json = f"{PREFIXO_ARQUIVO}{timestamp_atual}.json"
    caminho_json = os.path.join(diretorio_atual, nome_arquivo_json)
    
    try:
        with open(caminho_json, 'w', encoding='utf-8') as f:
            json.dump(dados, f, ensure_ascii=False, indent=4)
        print(f"  [📄] JSON global salvo: {nome_arquivo_json}")
    except Exception as e:
        print(f"  [!] Erro ao salvar o JSON global: {e}")

    # 2. Varre os gateways para extrair os CSVs em suas respectivas subpastas
    for nome_gateway, info_gateway in dados.get("gateways", {}).items():
        if info_gateway.get("status") == "sucesso":
            try:
                historico_requisicoes = info_gateway["dados"]["data"]["requests_history"]
                
                if not historico_requisicoes:
                    print(f"  [!] {nome_gateway}: A lista 'requests_history' está vazia. CSV ignorado.")
                    continue
                
                # --- LÓGICA DE SEPARAÇÃO POR MÁQUINA ---
                # Extrai o nome da máquina (host) armazenado durante a coleta via SSH
                nome_maquina = info_gateway.get("host", "unknown_host")
                
                # Cria a subpasta específica da máquina (ex: data/collect_1/larsid01)
                diretorio_maquina = os.path.join(diretorio_atual, nome_maquina)
                os.makedirs(diretorio_maquina, exist_ok=True)
                
                # Prepara o caminho final do CSV dentro dessa subpasta
                nome_arquivo_csv = f"{nome_gateway}_requests.csv"
                caminho_csv = os.path.join(diretorio_maquina, nome_arquivo_csv)
                # ---------------------------------------
                
                colunas = historico_requisicoes[0].keys()
                
                with open(caminho_csv, 'w', newline='', encoding='utf-8') as f_csv:
                    escritor = csv.DictWriter(f_csv, fieldnames=colunas)
                    escritor.writeheader()
                    escritor.writerows(historico_requisicoes)
                    
                print(f"  [📊] CSV gerado: {nome_maquina}/{nome_arquivo_csv}")
                
            except KeyError:
                print(f"  [!] {nome_gateway}: Estrutura de dados não possui 'requests_history'.")
            except Exception as e:
                print(f"  [!] {nome_gateway}: Erro ao gerar o CSV - {e}")

    print(f"\n[🚀] Extração finalizada com sucesso no diretório: {os.path.abspath(diretorio_atual)}")

if __name__ == "__main__":
    gateways = obter_containers_remotos()
    if gateways:
        resultados = extrair_dados_ssh(gateways)
        salvar_dados(resultados)
    else:
        print("\n[-] Nenhum gateway foi encontrado na rede para extração.")